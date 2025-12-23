"""Module for streaming BigQuery (bq) queries into parquet (pq) files."""

import json
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol, runtime_checkable

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery import job, table
import concurrent.futures
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


@runtime_checkable
class PipelineBQPQPathsProvider(Protocol):
    """
    Provide paths where to:

    1. stream a `data.parquet` file containing the query result

    2. and write a `stats.json` file containing the query statistics

    Methods:
        data_parquet_file_path: return the `data.parquet` file path
        stats_json_file_path: return the `stats.json` file path
    """

    def data_parquet_file_path(self) -> Path: ...

    def stats_json_file_path(self) -> Path: ...


@dataclass(frozen=True)
class PipelineBQPQQueryResult:
    """
    Result of the query with reference to job and row iterator.

    Attributes:
        bq_read_client: the client to stream results.
        job: the corresponding BigQuery job.
        rows: the iterable rows.
        paths_provider: protocol providing the destination paths.
        template_hash: SHA256 hash of the original query template
    """

    bq_read_client: bigquery_storage_v1.BigQueryReadClient
    job: job.QueryJob
    rows: table.RowIterator
    paths_provider: PipelineBQPQPathsProvider
    template_hash: str

    def save_data_parquet(self) -> Path:
        """Streams the query results to self.paths_provider.data_parquet_file_path().

        If the query returns no rows, an empty parquet file is written.

        Returns:
            Path to the written file.
        """
        parquet_path = self.paths_provider.data_parquet_file_path()
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        # Access the first batch to obtain the schema
        batches = self.rows.to_arrow_iterable(bqstorage_client=self.bq_read_client)

        with (
            logging_redirect_tqdm(),
            tqdm(
                total=self.rows.total_rows,
                desc="BQ dload",
                unit="rows",
            ) as pbar,
        ):
            first_batch = next(batches, None)
            if first_batch is not None:
                pbar.update(first_batch.num_rows)

            schema = first_batch.schema if first_batch is not None else pa.schema([])

            # Write the possibly-empty parquet file
            # Use a temporary directory, which is always removed regardless
            # of whether there's still a temporary file inside it
            with TemporaryDirectory() as tmp_dir:
                tmp_file = Path(tmp_dir) / parquet_path.name
                with pq.ParquetWriter(tmp_file, schema) as writer:
                    if first_batch is not None:
                        writer.write_batch(first_batch)
                    for batch in batches:
                        writer.write_batch(batch)
                        pbar.update(batch.num_rows)
                shutil.move(tmp_file, parquet_path)

        return parquet_path

    def save_stats_json(self) -> Path:
        """Saves query statistics to self.paths_provider.stats_json_file_path().

        Returns:
            Path to the written file.
        """
        stats_path = self.paths_provider.stats_json_file_path()
        stats_path.parent.mkdir(parents=True, exist_ok=True)

        # Calculate query start time and duration from BigQuery job
        query_start_time = None
        if self.job.started:
            query_start_time = self.job.started.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        query_duration_seconds = None
        if self.job.ended and self.job.started:
            duration = self.job.ended - self.job.started
            query_duration_seconds = duration.total_seconds()

        # Extract bytes processed/billed from job statistics
        total_bytes_processed = self.job.total_bytes_processed
        total_bytes_billed = self.job.total_bytes_billed

        stats = {
            "query_start_time": query_start_time,
            "query_duration_seconds": query_duration_seconds,
            "template_hash": self.template_hash,
            "total_bytes_processed": total_bytes_processed,
            "total_bytes_billed": total_bytes_billed,
        }

        # Use a temporary directory, which is always removed regardless
        # of whether there's still a temporary file inside it
        with TemporaryDirectory() as tmp_dir:
            tmp_file = Path(tmp_dir) / stats_path.name
            with tmp_file.open("w") as filep:
                json.dump(stats, filep, indent=2)
                filep.write("\n")
            shutil.move(tmp_file, stats_path)

        return stats_path


class PipelineBQPQClient:
    """Client for streaming BigQuery query results to parquet."""

    def __init__(self, project: str):
        """
        Initialize client with BigQuery project ID.

        Parameters:
            project: billing BigQuery project.
        """
        self._project = project
        self._client = None
        self._bq_read_clnt = None

    @property
    def client(self) -> bigquery.Client:
        """Lazy initialization of the BigQuery Client"""
        if self._client is None:
            self._client = bigquery.Client(project=self._project)
        return self._client

    @property
    def bq_read_clnt(self) -> bigquery_storage_v1.BigQueryReadClient:
        """Lazy initialization of the BigQueryReadClient."""
        if self._bq_read_clnt is None:
            self._bq_read_clnt = bigquery_storage_v1.BigQueryReadClient()
        return self._bq_read_clnt

    def execute_query(
        self,
        *,
        template_hash: str,
        query: str,
        paths_provider: PipelineBQPQPathsProvider,
    ) -> PipelineBQPQQueryResult:
        """
        Execute the given BigQuery query.

        Args:
            template_hash: SHA256 of the query template hash.
            query: The specific query to execute.
            paths_provider: provides paths where to write the results.

        Returns:
            A PipelineBQPQQueryResult instance.
        """
        # 1. execute the query and get job
        job = self.client.query(query)

        # 2. wait for job to finish with a progress bar
        with (
            logging_redirect_tqdm(),
            tqdm(
                desc="BQ job ",  # space to align with `BQ dload`
                unit="it",
                bar_format="{l_bar}{bar}| {n_fmt}{unit} [{elapsed}] {postfix}",
            ) as pbar,
        ):
            while job.state != "DONE":
                time.sleep(1)
                job.reload()
                pbar.update(1)
                if job.total_bytes_processed is not None:
                    pbar.set_postfix_str(
                        f"{job.total_bytes_processed / 1e9:.3f} GB processed",
                        refresh=True,
                    )

        # 3. obtain rows and return
        rows = job.result()
        return PipelineBQPQQueryResult(
            bq_read_client=self.bq_read_clnt,
            job=job,
            rows=rows,
            paths_provider=paths_provider,
            template_hash=template_hash,
        )

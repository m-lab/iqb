"""Module for streaming BigQuery (bq) queries into parquet (pq) files."""

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol, runtime_checkable

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery import job, table
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

log = logging.getLogger("pipeline/bqpq")


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


@dataclass(frozen=True, kw_only=True)
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

        log.info("BigQuery download... start")
        with (
            logging_redirect_tqdm(),
            tqdm(
                total=self.rows.total_rows,
                desc="BigQuery download",
                unit="rows",
            ) as pbar,
        ):
            first_batch = next(batches, None)
            schema = first_batch.schema if first_batch is not None else pa.schema([])
            if first_batch is not None:
                pbar.update(first_batch.num_rows)

            # Write the possibly-empty parquet file
            # Use a temporary directory, which is always removed regardless
            # of whether there's still a temporary file inside it
            with TemporaryDirectory(dir=parquet_path.parent) as tmp_dir:
                tmp_file = Path(tmp_dir) / parquet_path.name
                with pq.ParquetWriter(tmp_file, schema) as writer:
                    if first_batch is not None:
                        writer.write_batch(first_batch)
                    for batch in batches:
                        writer.write_batch(batch)
                        pbar.update(batch.num_rows)
                # On Windows, readers may block replace due to missing
                # FILE_SHARE_DELETE. YAGNI: add retry/backoff if it arises.
                os.replace(tmp_file, parquet_path)

        log.info("BigQuery download... ok")
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
        with TemporaryDirectory(dir=stats_path.parent) as tmp_dir:
            tmp_file = Path(tmp_dir) / stats_path.name
            with tmp_file.open("w") as filep:
                json.dump(stats, filep, indent=2)
                filep.write("\n")
            # On Windows, readers may block replace due to missing
            # FILE_SHARE_DELETE. YAGNI: add retry/backoff if it arises.
            os.replace(tmp_file, stats_path)

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
        _sleep_secs: int | float = 1,  # used for shorter testing
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
        log.info("BigQuery query... start")
        with (
            logging_redirect_tqdm(),
            tqdm(
                desc="BigQuery query",
                total=10,
                bar_format="{l_bar}{bar}| [{elapsed}] {postfix}",
            ) as pbar,
        ):
            while job.state != "DONE":
                time.sleep(_sleep_secs)
                factor = 2 if (pbar.n / pbar.total) >= 0.8 else 1
                pbar.total *= factor
                job.reload()
                pbar.update(1)

            pbar.n = pbar.total

        # 3. log about the total number of bytes processed
        bytes_processed_str = (
            f" ({job.total_bytes_processed / 1e9:.3f} GB processed)"
            if job.total_bytes_processed is not None
            else ""
        )
        log.info("BigQuery query... ok%s", bytes_processed_str)

        # 4. obtain rows and return
        rows = job.result()
        return PipelineBQPQQueryResult(
            bq_read_client=self.bq_read_clnt,
            job=job,
            rows=rows,
            paths_provider=paths_provider,
            template_hash=template_hash,
        )

"""Module for streaming BigQuery (bq) queries into parquet (pq) files."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery import job, table


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

        # Note: using .as_posix to avoid paths with backslashes
        # that can cause issues with PyArrow on Windows
        posix_path = parquet_path.as_posix()

        # Access the first batch to obtain the schema
        batches = self.rows.to_arrow_iterable(bqstorage_client=self.bq_read_client)
        first_batch = next(batches, None)
        schema = first_batch.schema if first_batch is not None else pa.schema([])

        # Write the possibly-empty parquet file
        with pq.ParquetWriter(posix_path, schema) as writer:
            if first_batch is not None:
                writer.write_batch(first_batch)
            for batch in batches:
                writer.write_batch(batch)

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

        with stats_path.open("w") as filep:
            json.dump(stats, filep, indent=2)
            filep.write("\n")

        return stats_path


class PipelineBQPQClient:
    """Client for streaming BigQuery query results to parquet."""

    def __init__(self, project: str):
        """
        Initialize client with BigQuery project ID.

        Parameters:
            project: billing BigQuery project.
        """
        self.client = bigquery.Client(project=project)
        self.bq_read_clnt = bigquery_storage_v1.BigQueryReadClient()

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
        # 1. execute the query and get job and iterable rows
        job = self.client.query(query)
        rows = job.result()

        # 2. return the result object
        return PipelineBQPQQueryResult(
            bq_read_client=self.bq_read_clnt,
            job=job,
            rows=rows,
            paths_provider=paths_provider,
            template_hash=template_hash,
        )

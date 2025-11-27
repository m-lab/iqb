"""Module for populating the IQB-measurement-data cache.

The IQBPipeline component runs BigQuery queries and populates the cache.

It uses the same directory convention used by IQBCache.

Design
------

We store files named after the following pattern:

    $datadir/cache/v1/$since/$until/$query_type.parquet

The `$since` and `$until` variables are timestamps using the ISO8601 format
with UTC timezone, formatted to be file-system friendly (i.e., without
including the ":" character). For example: `20251126T123600Z`.

The `$since` timestamp is included and the `$until` one is excluded. This
simplifies specifying time ranges significantly.

The fact that we use explicit timestamps allows the cache to contain any
kind of time range, including partially overlapping ones.

The $query_type is one of the following query granularity types:

1. country_$kind
2. country_asn_$kind
3. country_province_$kind
4. country_province_asn_$kind
5. country_city_$kind
6. country_city_asn_$kind

Where $kind is either "download" or "upload". The final implementation
of this design will have all the required queries implemented.

For each query type, we have a corresponding parquet file.

We use parquet as the file format because:

1. we can stream to it when writing with BigQuery

2. regardless of the file size, we can always process and filter it in
chunks since parquet divides rows into independent groups

Additionally, we can process parquet with Pandas data frames, which
are a standard tool into a data scientist's toolkit.

We store the raw results of queries for further processing, since the
query itself is the expensive operation while further elaborations
are comparatively cheaper.

With dataframes and parquet files, ideally we can:

1. select a country (and other properties when needed, e.g., the ASN or
the city) and skip the row groups that do not match easily

2. immediately project the columns we don't care about out of the equation

If processing the parquet-dumped raw query outputs is fast enough, we
can directly access the data without intermediate formats.
"""

from dataclasses import dataclass
from datetime import datetime
from importlib.resources import files
from pathlib import Path
from typing import Final

import pyarrow.parquet as pq
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery import job, table

from . import cache, queries

VALID_TEMPLATE_NAMES: Final[set[str]] = {
    "downloads_by_country",
    "uploads_by_country",
}


@dataclass(frozen=True)
class ParquetFileInfo:
    """
    Result of serializing a query result into the cache using parquet.

    Attributes:
        no_content: true if the query returned no content, in which case
            no parquet file is actually being written.
        file_path: full path to the written file.
    """

    no_content: bool
    file_path: Path


@dataclass(frozen=True)
class QueryResult:
    """
    Result of the query with reference to job and row iterator.

    Attributes:
        bq_read_client: the client to stream results.
        job: the corresponding BigQuery job.
        rows: the iterable rows.
        parquet_path: path where to save the parquet file
    """

    bq_read_client: bigquery_storage_v1.BigQueryReadClient
    job: job.QueryJob
    rows: table.RowIterator
    parquet_path: Path

    def save_parquet(self) -> ParquetFileInfo:
        """Streams and saves the query results to `self.parquet_path`."""
        self.parquet_path.parent.mkdir(parents=True, exist_ok=True)

        # Access the first batch to obtain the schema
        batches = self.rows.to_arrow_iterable(bqstorage_client=self.bq_read_client)
        first_batch = next(batches, None)
        if first_batch is None:
            return ParquetFileInfo(no_content=True, file_path=self.parquet_path)

        # Note: using .as_posix to avoid paths with backslashes
        # that can cause issues with PyArrow on Windows
        posix_path = self.parquet_path.as_posix()
        with pq.ParquetWriter(posix_path, first_batch.schema) as writer:
            writer.write_batch(first_batch)
            for batch in batches:
                writer.write_batch(batch)

        return ParquetFileInfo(no_content=False, file_path=self.parquet_path)


class IQBPipeline:
    """Component for populating the IQB-measurement-data cache."""

    def __init__(self, project_id: str, data_dir: str | Path | None = None):
        """
        Initialize cache with data directory path.

        Parameters:
            project_id: BigQuery project ID.
            data_dir: Path to directory containing cached data files.
                If None, defaults to .iqb/ in current working directory.
        """
        self.client = bigquery.Client(project=project_id)
        self.bq_read_clnt = bigquery_storage_v1.BigQueryReadClient()
        self.data_dir = cache.data_dir_or_default(data_dir)

    def execute_query_template(
        self,
        template: str,
        start_date: str,
        end_date: str,
    ) -> QueryResult:
        """
        Execute the given BigQuery query template.

        Args:
            template: name for the query template (e.g., "downloads_by_country")
            start_date: Date when to start the query (included) -- format YYYY-MM-DD
            end_date: Date when to end the query (excluded) -- format YYYY-MM-DD

        Returns:
            A QueryResult instance.
        """
        # 1. validate the start and the end dates
        start_time = _parse_date(start_date)
        end_time = _parse_date(end_date)
        if start_time > end_time:
            raise ValueError(f"start_date must be <= end_date, got: {start_date} > {end_date}")

        # 2. load the query template
        if template not in VALID_TEMPLATE_NAMES:
            valid = ", ".join(sorted(VALID_TEMPLATE_NAMES))
            raise ValueError(f"Unknown template {template!r}; valid templates: {valid}")
        query = _load_query_template(template, start_date, end_date)

        # 3. execute the query and get job and iterable rows
        job = self.client.query(query)
        rows = job.result()

        # 4. compute the path where we would save the results
        fs_date_format = "%Y%m%dT000000Z"
        start_dir = start_time.strftime(fs_date_format)
        end_dir = end_time.strftime(fs_date_format)
        filename = f"{template}.parquet"
        parquet_path = self.data_dir / "cache" / "v1" / start_dir / end_dir / filename

        # 5. return the result object
        return QueryResult(
            bq_read_client=self.bq_read_clnt,
            job=job,
            rows=rows,
            parquet_path=parquet_path,
        )


def _parse_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {value} (expected YYYY-MM-DD)") from e


def _load_query_template(name: str, start_date: str, end_date: str) -> str:
    query_file = files(queries).joinpath(f"{name}.sql")
    query = query_file.read_text()
    query = query.replace("{START_DATE}", start_date)
    query = query.replace("{END_DATE}", end_date)
    return query

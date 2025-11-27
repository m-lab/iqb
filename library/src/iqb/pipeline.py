"""Module for populating the IQB-measurement-data cache.

The IQBPipeline component runs BigQuery queries and populates the cache.

It uses the same directory convention used by IQBCache.

Design
------

We store files named after the following pattern:

    $datadir/cache/v1/$since/$until/$query_type/data.parquet
    $datadir/cache/v1/$since/$until/$query_type/stats.json

Each query result is stored in a directory containing the data file (data.parquet)
and query metadata (stats.json). The stats file records query execution details
such as start time (RFC3339 format with Z suffix), duration, and bytes processed
for transparency and debugging

The `$since` and `$until` variables are timestamps using the ISO8601 format
with UTC timezone, formatted to be file-system friendly (i.e., without
including the ":" character). For example: `20251126T123600Z`.

The `$since` timestamp is included and the `$until` one is excluded. This
simplifies specifying time ranges significantly (e.g., October 2025 is
represented using `since=20251001T000000Z` and `until=20251101T000000Z`).

The fact that we use explicit timestamps allows the cache to contain any
kind of time range, including partially overlapping ones. This content-
addressable approach means the time range IS the path identifier, eliminating
the need for metadata files, cache invalidation logic, or naming conventions
to prevent conflicts. Overlapping queries coexist naturally without coordination.

The $query_type is one of the following query granularity types:

1. country_$kind
2. country_asn_$kind
3. country_province_$kind
4. country_province_asn_$kind
5. country_city_$kind
6. country_city_asn_$kind

Where $kind is either "download" or "upload". The final implementation
of this design will have all the required queries implemented.

For each query type, we have a corresponding directory containing the data
and metadata files.

We use parquet as the file format because:

1. we can stream to it when writing BigQuery queries results

2. regardless of the file size, we can always process and filter it in
chunks since parquet divides rows into independent groups

We store the raw results of queries for further processing, since the
query itself is the expensive operation while further elaborations
are comparatively cheaper, and can be done locally with PyArrow/Pandas.

Specifically, reading from the cache (when implemented in cache.py) will
use PyArrow's predicate pushdown for efficient filtering:

1. Use PyArrow's filters parameter to skip row groups at I/O level:
   pq.read_table(path, filters=[('country', '=', 'US')], columns=['...'])

2. Convert to Pandas DataFrame only after filtering for analysis

This approach ensures we can select a country (and other properties when
needed, e.g., the ASN or city) and skip row groups that do not match, while
immediately projecting out columns we don't need. If processing the parquet-
dumped raw query outputs is fast enough, we can directly access the data
without producing intermediate formats.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
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
    "downloads_by_country_city_asn",
    "uploads_by_country_city_asn",
}

# Cache file names
CACHE_DATA_FILENAME: Final[str] = "data.parquet"
CACHE_STATS_FILENAME: Final[str] = "stats.json"


@dataclass(frozen=True)
class CacheEntry:
    """
    Reference to a cache entry containing query results and metadata.

    Attributes:
        data_path: Path to data.parquet file
        stats_path: Path to stats.json file
    """

    data_path: Path
    stats_path: Path


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
        cache_dir: directory where to save data.parquet and stats.json
        query_start_time: RFC3339 UTC timestamp when query was started (with Z suffix)
        template_hash: SHA256 hash of the original query template
    """

    bq_read_client: bigquery_storage_v1.BigQueryReadClient
    job: job.QueryJob
    rows: table.RowIterator
    cache_dir: Path
    query_start_time: str
    template_hash: str

    def save_parquet(self) -> ParquetFileInfo:
        """Streams and saves the query results to data.parquet in cache_dir."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = self.cache_dir / CACHE_DATA_FILENAME

        # Access the first batch to obtain the schema
        batches = self.rows.to_arrow_iterable(bqstorage_client=self.bq_read_client)
        first_batch = next(batches, None)
        if first_batch is None:
            return ParquetFileInfo(no_content=True, file_path=parquet_path)

        # Note: using .as_posix to avoid paths with backslashes
        # that can cause issues with PyArrow on Windows
        posix_path = parquet_path.as_posix()
        with pq.ParquetWriter(posix_path, first_batch.schema) as writer:
            writer.write_batch(first_batch)
            for batch in batches:
                writer.write_batch(batch)

        return ParquetFileInfo(no_content=False, file_path=parquet_path)

    def save_stats(self) -> Path:
        """Writes query statistics to stats.json in cache_dir.

        Returns:
            Path to the written stats.json file.
        """
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        stats_path = self.cache_dir / CACHE_STATS_FILENAME

        # Calculate query duration from BigQuery job
        query_duration_seconds = None
        if self.job.ended and self.job.started:
            duration = self.job.ended - self.job.started
            query_duration_seconds = duration.total_seconds()

        # Extract bytes processed/billed from job statistics
        total_bytes_processed = self.job.total_bytes_processed
        total_bytes_billed = self.job.total_bytes_billed

        stats = {
            "query_start_time": self.query_start_time,
            "query_duration_seconds": query_duration_seconds,
            "template_hash": self.template_hash,
            "total_bytes_processed": total_bytes_processed,
            "total_bytes_billed": total_bytes_billed,
        }

        with stats_path.open("w") as f:
            json.dump(stats, f, indent=2)
            f.write("\n")  # Add newline at EOF for git-friendly diffs

        return stats_path


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

    def _cache_dir_path(
        self,
        template: str,
        start_date: str,
        end_date: str,
    ) -> Path:
        """
        Compute the cache directory path for a query.

        Args:
            template: name for the query template (e.g., "downloads_by_country")
            start_date: Date when to start the query (included) -- format YYYY-MM-DD
            end_date: Date when to end the query (excluded) -- format YYYY-MM-DD

        Returns:
            Path to the cache directory.
        """
        start_time = _parse_date(start_date)
        end_time = _parse_date(end_date)

        fs_date_format = "%Y%m%dT000000Z"
        start_dir = start_time.strftime(fs_date_format)
        end_dir = end_time.strftime(fs_date_format)

        return self.data_dir / "cache" / "v1" / start_dir / end_dir / template

    def get_cache_entry(
        self,
        template: str,
        start_date: str,
        end_date: str,
        *,
        fetch_if_missing: bool = False,
    ) -> CacheEntry:
        """
        Get or create a cache entry for the given query.

        Args:
            template: name for the query template (e.g., "downloads_by_country")
            start_date: Date when to start the query (included) -- format YYYY-MM-DD
            end_date: Date when to end the query (excluded) -- format YYYY-MM-DD
            fetch_if_missing: if True, execute query and save if cache doesn't exist.
                Default is False (do not fetch automatically).

        Returns:
            CacheEntry with paths to data.parquet and stats.json.

        Raises:
            FileNotFoundError: if cache doesn't exist and fetch_if_missing is False.
        """
        cache_dir = self._cache_dir_path(template, start_date, end_date)
        data_path = cache_dir / CACHE_DATA_FILENAME
        stats_path = cache_dir / CACHE_STATS_FILENAME

        # Check if cache exists
        if data_path.exists() and stats_path.exists():
            return CacheEntry(data_path=data_path, stats_path=stats_path)

        # Cache doesn't exist
        if not fetch_if_missing:
            raise FileNotFoundError(
                f"Cache entry not found for {template} "
                f"({start_date} to {end_date}). "
                f"Set fetch_if_missing=True to execute query."
            )

        # Execute query and save to cache
        result = self.execute_query_template(template, start_date, end_date)
        result.save_parquet()
        result.save_stats()

        return CacheEntry(data_path=data_path, stats_path=stats_path)

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
        # 1. parse the start and the end dates
        start_time = _parse_date(start_date)
        end_time = _parse_date(end_date)
        if start_time > end_time:
            raise ValueError(f"start_date must be <= end_date, got: {start_date} > {end_date}")

        # 2. load the query template and get its hash
        if template not in VALID_TEMPLATE_NAMES:
            valid = ", ".join(sorted(VALID_TEMPLATE_NAMES))
            raise ValueError(f"Unknown template {template!r}; valid templates: {valid}")
        query, template_hash = _load_query_template(template, start_date, end_date)

        # 3. record query start time (RFC3339 format with Z suffix)
        query_start_time = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # 4. execute the query and get job and iterable rows
        job = self.client.query(query)
        rows = job.result()

        # 5. compute the directory where we would save the results
        cache_dir = self._cache_dir_path(template, start_date, end_date)

        # 6. return the result object
        return QueryResult(
            bq_read_client=self.bq_read_clnt,
            job=job,
            rows=rows,
            cache_dir=cache_dir,
            query_start_time=query_start_time,
            template_hash=template_hash,
        )


def _parse_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {value} (expected YYYY-MM-DD)") from e


def _load_query_template(name: str, start_date: str, end_date: str) -> tuple[str, str]:
    """Load and instantiate a query template.

    Returns:
        Tuple of (instantiated_query, template_hash) where template_hash is
        the SHA256 hash of the original template file.
    """
    query_file = files(queries).joinpath(f"{name}.sql")
    template_text = query_file.read_text()

    # Compute hash of the original template (before substitution)
    template_hash = hashlib.sha256(template_text.encode("utf-8")).hexdigest()

    # Instantiate the template
    query = template_text.replace("{START_DATE}", start_date)
    query = query.replace("{END_DATE}", end_date)

    return query, template_hash

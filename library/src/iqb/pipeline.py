"""Module for populating the IQB-measurement-data cache.

The IQBPipeline component runs BigQuery queries and populates the cache.

It uses the same directory convention used by IQBCache.

Design
------

We store files named after the following pattern:

    $datadir/cache/v1/$since/$until/$query_type/data.parquet
    $datadir/cache/v1/$since/$until/$query_type/stats.json

Each query result is stored in a directory containing the data file (data.parquet)
and the query metadata (stats.json). The stats file records query execution details
such as start time (RFC3339 format with Z suffix), duration, and bytes processed
for transparency and debugging

The `$since` and `$until` variables are timestamps using the RFC3339 format
with UTC timezone, formatted to be file-system friendly (i.e., without
including the ":" character). For example: `20251126T100000Z`.

The `$since` timestamp is included and the `$until` one is excluded. This
simplifies specifying time ranges significantly (e.g., October 2025 is
represented using `since=20251001T000000Z` and `until=20251101T000000Z`).

Note that when using BigQuery the second component of the path will always
be `T000000Z` because we do not support hourly range queries for now.

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

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery import job, table

from . import queries

VALID_TEMPLATE_NAMES: Final[set[str]] = {
    "downloads_by_country",
    "uploads_by_country",
    "downloads_by_country_city_asn",
    "uploads_by_country_city_asn",
}

# Cache file names
PIPELINE_CACHE_DATA_FILENAME: Final[str] = "data.parquet"
PIPELINE_CACHE_STATS_FILENAME: Final[str] = "stats.json"


@dataclass(frozen=True)
class ParsedTemplateName:
    """Container for a parsed template name."""

    value: str


@dataclass(frozen=True)
class PipelineCacheEntry:
    """
    Reference to a cache entry containing query results and metadata.

    Attributes:
        data_dir: the Path that points to the data dir
        tname: the ParsedTemplateName to use
        start_time: the datetime containing the start time
        end_time: the datetime containing the end time
    """

    data_dir: Path
    tname: ParsedTemplateName
    start_time: datetime
    end_time: datetime

    def dir_path(self) -> Path:
        """Returns the directory path where to write files."""
        fs_date_format = "%Y%m%dT000000Z"
        start_dir = self.start_time.strftime(fs_date_format)
        end_dir = self.end_time.strftime(fs_date_format)
        return self.data_dir / "cache" / "v1" / start_dir / end_dir / self.tname.value

    def data_path(self) -> Path | None:
        """Returns the path to the parquet data file, if it exists, or None."""
        value = self.dir_path() / PIPELINE_CACHE_DATA_FILENAME
        if not value.exists():
            return None
        return value

    def stats_path(self) -> Path | None:
        """Returns the path to the JSON stats file, if it exists, or None."""
        value = self.dir_path() / PIPELINE_CACHE_STATS_FILENAME
        if not value.exists():
            return None
        return value


@dataclass(frozen=True)
class ParquetFileInfo:
    """
    Result of serializing a query result into the cache using parquet.

    An empty parquet file is written if the query returns no rows.

    Attributes:
        file_path: full path to the written file.
    """

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
        """Streams and saves the query results to data.parquet in cache_dir.

        If the query returns no rows, an empty parquet file is written.
        """
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = self.cache_dir / PIPELINE_CACHE_DATA_FILENAME

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

        return ParquetFileInfo(file_path=parquet_path)

    def save_stats(self) -> Path:
        """Writes query statistics to stats.json in cache_dir.

        Returns:
            Path to the written stats.json file.
        """
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        stats_path = self.cache_dir / PIPELINE_CACHE_STATS_FILENAME

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


class PipelineCacheManager:
    """Manages the cache populated by the IQBPipeline."""

    def __init__(self, data_dir: str | Path | None = None):
        """
        Initialize cache with data directory path.

        Parameters:
            data_dir: Path to directory containing cached data files.
                If None, defaults to .iqb/ in current working directory.
        """
        self.data_dir = data_dir_or_default(data_dir)

    def get_cache_entry(
        self,
        template: str,
        start_date: str,
        end_date: str,
    ) -> PipelineCacheEntry:
        """
        Get cache entry for the given query.

        Args:
            template: name for the query template (e.g., "downloads_by_country")
            start_date: Date when to start the query (included) -- format YYYY-MM-DD
            end_date: Date when to end the query (excluded) -- format YYYY-MM-DD

        Returns:
            PipelineCacheEntry with correctly initialized fields.
        """
        # 1. parse the start and the end dates
        start_time, end_time = _parse_both_dates(start_date, end_date)

        # 2. ensure the template name is correct
        tname = _parse_template_name(template)

        # 3. return the corresponding entry
        return PipelineCacheEntry(
            data_dir=self.data_dir,
            tname=tname,
            start_time=start_time,
            end_time=end_time,
        )


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
        self.manager = PipelineCacheManager(data_dir)

    def get_cache_entry(
        self,
        template: str,
        start_date: str,
        end_date: str,
        *,
        fetch_if_missing: bool = False,
    ) -> PipelineCacheEntry:
        """
        Get or create a cache entry for the given query.

        Args:
            template: name for the query template (e.g., "downloads_by_country")
            start_date: Date when to start the query (included) -- format YYYY-MM-DD
            end_date: Date when to end the query (excluded) -- format YYYY-MM-DD
            fetch_if_missing: if True, execute query and save if cache doesn't exist.
                Default is False (do not fetch automatically).

        Returns:
            PipelineCacheEntry with paths to data.parquet and stats.json.

        Raises:
            FileNotFoundError: if cache doesn't exist and fetch_if_missing is False.
        """
        # 1. get the cache entry
        entry = self.manager.get_cache_entry(template, start_date, end_date)

        # 2. make sure the entry exists
        if entry.data_path() is not None and entry.stats_path() is not None:
            return entry

        # 3. handle missing cache without auto-fetching
        if not fetch_if_missing:
            raise FileNotFoundError(
                f"Cache entry not found for {template} "
                f"({start_date} to {end_date}). "
                f"Set fetch_if_missing=True to execute query."
            )

        # 4. execute query and update the cache
        result = self._execute_query_template(entry)
        result.save_parquet()
        result.save_stats()

        # 5. return information about the cache entry
        assert entry.data_path() is not None
        assert entry.stats_path() is not None
        return entry

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
        return self._execute_query_template(
            self.manager.get_cache_entry(template, start_date, end_date)
        )

    def _execute_query_template(self, entry: PipelineCacheEntry) -> QueryResult:
        # 1. load the actual query
        query, template_hash = _load_query_template(entry.tname, entry.start_time, entry.end_time)

        # 2. record query start time (RFC3339 format with Z suffix)
        query_start_time = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # 3. execute the query and get job and iterable rows
        job = self.client.query(query)
        rows = job.result()

        # 4. compute the directory where we would save the results
        cache_dir = entry.dir_path()

        # 5. return the result object
        return QueryResult(
            bq_read_client=self.bq_read_clnt,
            job=job,
            rows=rows,
            cache_dir=cache_dir,
            query_start_time=query_start_time,
            template_hash=template_hash,
        )


def data_dir_or_default(data_dir: str | Path | None) -> Path:
    """
    Return data_dir as a Path if not empty. Otherwise return the
    default value for the data_dir (i.e., `./.iqb` like git).
    """
    return Path.cwd() / ".iqb" if data_dir is None else Path(data_dir)


def _parse_both_dates(start_date: str, end_date: str) -> tuple[datetime, datetime]:
    """Parses both dates and ensures start_date <= end_date."""
    start_time = _parse_date(start_date)
    end_time = _parse_date(end_date)
    if start_time > end_time:
        raise ValueError(f"start_date must be <= end_date, got: {start_date} > {end_date}")
    return start_time, end_time


def _parse_template_name(value: str) -> ParsedTemplateName:
    """Ensure that the template name is a valid template name."""
    if value not in VALID_TEMPLATE_NAMES:
        valid = ", ".join(sorted(VALID_TEMPLATE_NAMES))
        raise ValueError(f"Unknown template {value!r}; valid templates: {valid}")
    return ParsedTemplateName(value=value)


def _parse_date(value: str) -> datetime:
    """Ensure that a single date is consistent with the format and return it parsed."""
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {value} (expected YYYY-MM-DD)") from e


def _load_query_template(
    tname: ParsedTemplateName,
    start_date: datetime,
    end_date: datetime,
) -> tuple[str, str]:
    """Load and instantiate a query template.

    Returns:
        Tuple of (instantiated_query, template_hash) where template_hash is
        the SHA256 hash of the original template file.
    """
    query_file = files(queries).joinpath(f"{tname.value}.sql")
    template_text = query_file.read_text()

    # Compute hash of the original template (before substitution)
    template_hash = hashlib.sha256(template_text.encode("utf-8")).hexdigest()

    # Instantiate the template
    query = template_text.replace("{START_DATE}", start_date.strftime("%Y-%m-%d"))
    query = query.replace("{END_DATE}", end_date.strftime("%Y-%m-%d"))

    return query, template_hash

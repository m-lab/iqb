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
from datetime import datetime
from importlib.resources import files
from pathlib import Path

from .. import queries
from .bqpq import (
    PipelineBQPQClient,
    PipelineBQPQQueryResult,
)
from .cache import (
    PipelineCacheEntry,
    PipelineCacheManager,
    PipelineCacheTemplateName,
)


class IQBPipeline:
    """Component for populating the IQB-measurement-data cache."""

    def __init__(self, project: str, data_dir: str | Path | None = None):
        """
        Initialize cache with data directory path.

        Parameters:
            project_id: BigQuery project ID.
            data_dir: Path to directory containing cached data files.
                If None, defaults to .iqb/ in current working directory.
        """
        self.client = PipelineBQPQClient(project=project)
        self.manager = PipelineCacheManager(data_dir=data_dir)

    def get_cache_entry(
        self,
        *,
        template: str,
        start_date: str,
        end_date: str,
        fetch_if_missing: bool = False,
    ) -> PipelineCacheEntry:
        """
        Get or create a cache entry for the given query template.

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

        # 2. if the entry exists, there's nothing to do
        if entry.data_parquet_file_path().exists() and entry.stats_json_file_path().exists():
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
        result.save_data_parquet()
        result.save_stats_json()

        # 5. return information about the cache entry
        return entry

    def execute_query_template(
        self,
        *,
        template: str,
        start_date: str,
        end_date: str,
    ) -> PipelineBQPQQueryResult:
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

    def _execute_query_template(
        self,
        entry: PipelineCacheEntry,
    ) -> PipelineBQPQQueryResult:
        # 1. load the query
        query, template_hash = _load_query_template(entry.tname, entry.start_time, entry.end_time)

        # 2. execute the query
        return self.client.execute_query(
            template_hash=template_hash,
            query=query,
            paths_provider=entry,
        )


def _load_query_template(
    tname: PipelineCacheTemplateName,
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

    # Compute hash of the query template
    template_hash = hashlib.sha256(template_text.encode("utf-8")).hexdigest()

    # Instantiate the template
    query = template_text.replace("{START_DATE}", start_date.strftime("%Y-%m-%d"))
    query = query.replace("{END_DATE}", end_date.strftime("%Y-%m-%d"))

    return query, template_hash

"""Module implementing the IQBPipeline type."""

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
    PipelineRemoteCache,
)


class IQBPipeline:
    """Component for populating the IQB-measurement-data cache."""

    def __init__(
        self,
        project: str,
        data_dir: str | Path | None = None,
        remote_cache: PipelineRemoteCache | None = None,
    ):
        """
        Initialize cache with data directory path.

        Parameters:
            project: BigQuery billing project name. That is the project to use
                for *executing* the queries. The project on which the data
                lives is instead determinaed by the table name *in* the query.
            data_dir: Path to directory containing cached data files.
                If None, defaults to .iqb/ in current working directory.
            remote_cache: Optional remote cache for fetching cached query results.
        """
        # TODO(bassosimone): Consider adding query sampling support for development/testing
        #
        # Use case: When developing new queries, it's often useful to test on a sample
        # (e.g., 10-25%) of the data to avoid:
        #   - High BigQuery costs during iteration
        #   - Rate limiting / quota exhaustion
        #   - Long query execution times
        #
        # BigQuery supports TABLESAMPLE SYSTEM for efficient block-level sampling:
        #   FROM table TABLESAMPLE SYSTEM (25 PERCENT)
        #
        # Benefits:
        #   - Only scans ~25% of data blocks (cost savings!)
        #   - Fast query execution
        #   - Perfect for development/testing
        #
        # CRITICAL GOTCHA: TABLESAMPLE (100 PERCENT) != omitting the clause!
        #   - Regular queries (no TABLESAMPLE): Results are CACHED by BigQuery
        #   - TABLESAMPLE (100 PERCENT): NO CACHING, full scan every time
        #   - See: https://cloud.google.com/bigquery/docs/table-sampling
        #
        # Therefore, implementation MUST use conditional logic:
        #   - If sample_percent == 100: omit TABLESAMPLE entirely (use cache)
        #   - If sample_percent < 100: add TABLESAMPLE SYSTEM (X PERCENT)
        #
        # Potential implementation:
        #   1. Add sample_percent parameter to __init__ (default: 100)
        #   2. Query templates get {TABLESAMPLE} placeholder
        #   3. Replace with "" (100%) or "TABLESAMPLE SYSTEM (X PERCENT)" (< 100%)
        #   4. Cache path includes sample rate when < 100 to prevent mixing data:
        #      cache/v1/{since}/{until}/{dataset}/data.parquet          # 100%
        #      cache/v1/{since}/{until}/{dataset}_sample25/data.parquet # 25%
        #
        # Alternative quick implementation:
        #   - Use environment variable: IQB_SAMPLE_PERCENT (default: 100)
        #   - No code changes to queries
        #   - Usage: IQB_SAMPLE_PERCENT=25 python run_query.py ...
        #
        # Decision: Not implementing now (YAGNI - add when actually needed)

        self.client = PipelineBQPQClient(project=project)
        self.manager = PipelineCacheManager(data_dir=data_dir, remote_cache=remote_cache)

    def get_cache_entry(
        self,
        *,
        dataset_name: str,
        start_date: str,
        end_date: str,
        fetch_if_missing: bool = False,
    ) -> PipelineCacheEntry:
        """
        Get or create a cache entry for the given query template.

        If fetch_if_missing is False and the entry does not exist on
        disk, this method raises a FileNotFoundError exception.

        Otherwise, attempts to fetch from configured remote_cache (if any),
        then falls back to BigQuery execution if still missing.

        Args:
            dataset_name: Name for the dataset (e.g., "downloads_by_country")
            start_date: Date when to start the query (included) -- format YYYY-MM-DD
            end_date: Date when to end the query (excluded) -- format YYYY-MM-DD
            fetch_if_missing: Whether to try to fetch or query for the entry

        Returns:
            PipelineCacheEntry with paths to data.parquet and stats.json.

        Raises:
            FileNotFoundError: if cache doesn't exist and fetch_if_missing is False.
        """
        # 1. get the cache entry (manager handles local cache check and remote sync)
        entry = self.manager.get_cache_entry(
            dataset_name=dataset_name,
            start_date=start_date,
            end_date=end_date,
            fetch_if_missing=fetch_if_missing,
        )

        # 2. if the entry exists (either was local or synced from remote), we're done
        if entry.data_parquet_file_path().exists() and entry.stats_json_file_path().exists():
            return entry

        # 3. handle missing cache without auto-fetching
        if not fetch_if_missing:
            raise FileNotFoundError(
                f"Cache entry not found for {dataset_name} "
                f"({start_date} to {end_date}). "
                f"Set fetch_if_missing=True to execute query."
            )

        # 4. fall back to BigQuery execution if remote cache didn't have it
        result = self._execute_query_template(entry)
        result.save_data_parquet()
        result.save_stats_json()
        return entry

    def execute_query_template(
        self,
        *,
        dataset_name: str,
        start_date: str,
        end_date: str,
    ) -> PipelineBQPQQueryResult:
        """
        Execute the given BigQuery query template.

        Args:
            dataset_name: Name of the dataset (e.g., "downloads_by_country")
            start_date: Date when to start the query (included) -- format YYYY-MM-DD
            end_date: Date when to end the query (excluded) -- format YYYY-MM-DD

        Returns:
            A QueryResult instance.
        """
        return self._execute_query_template(
            self.manager.get_cache_entry(
                start_date=start_date,
                end_date=end_date,
                dataset_name=dataset_name,
            )
        )

    def _execute_query_template(
        self,
        entry: PipelineCacheEntry,
    ) -> PipelineBQPQQueryResult:
        # 1. load the query
        query, template_hash = _load_query_template(
            entry.dataset_name, entry.start_time, entry.end_time
        )

        # 2. execute the query
        return self.client.execute_query(
            template_hash=template_hash,
            query=query,
            paths_provider=entry,
        )


def _load_query_template(
    dataset_name: str,
    start_date: datetime,
    end_date: datetime,
) -> tuple[str, str]:
    """Load and instantiate a query template.

    Returns:
        Tuple of (instantiated_query, template_hash) where template_hash is
        the SHA256 hash of the original template file.
    """
    query_file = files(queries).joinpath(f"{dataset_name}.sql")
    template_text = query_file.read_text()

    # Compute hash of the query template
    template_hash = hashlib.sha256(template_text.encode("utf-8")).hexdigest()

    # Instantiate the template
    query = template_text.replace("{START_DATE}", start_date.strftime("%Y-%m-%d"))
    query = query.replace("{END_DATE}", end_date.strftime("%Y-%m-%d"))

    return query, template_hash

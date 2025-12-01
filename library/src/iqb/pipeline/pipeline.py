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
)


class IQBPipeline:
    """Component for populating the IQB-measurement-data cache."""

    def __init__(self, project: str, data_dir: str | Path | None = None):
        """
        Initialize cache with data directory path.

        Parameters:
            project: BigQuery billing project name. That is the project to use
                for *executing* the queries. The project on which the data
                lives is instead determinaed by the table name *in* the query.
            data_dir: Path to directory containing cached data files.
                If None, defaults to .iqb/ in current working directory.
        """
        self.client = PipelineBQPQClient(project=project)
        self.manager = PipelineCacheManager(data_dir=data_dir)

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

        Args:
            dataset_name: Name for the dataset (e.g., "downloads_by_country")
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
        entry = self.manager.get_cache_entry(
            dataset_name=dataset_name,
            start_date=start_date,
            end_date=end_date,
        )

        # 2. if the entry exists, there's nothing to do
        if entry.data_parquet_file_path().exists() and entry.stats_json_file_path().exists():
            return entry

        # 3. handle missing cache without auto-fetching
        if not fetch_if_missing:
            raise FileNotFoundError(
                f"Cache entry not found for {dataset_name} "
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

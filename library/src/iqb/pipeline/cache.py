"""Module to manage the on-disk IQB measurements cache."""

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final, Protocol

# Cache file names
PIPELINE_CACHE_DATA_FILENAME: Final[str] = "data.parquet"
PIPELINE_CACHE_STATS_FILENAME: Final[str] = "stats.json"


@dataclass(frozen=True)
class PipelineCacheEntry:
    """
    Reference to a cache entry containing query results and metadata.

    Attributes:
        data_dir: the Path that points to the data dir
        dataset_name: the name of the dataset
        start_time: the datetime containing the start time
        end_time: the datetime containing the end time
    """

    data_dir: Path
    dataset_name: str
    start_time: datetime
    end_time: datetime

    def dir_path(self) -> Path:
        """Returns the directory path where to write files."""
        fs_date_format = "%Y%m%dT000000Z"
        start_dir = self.start_time.strftime(fs_date_format)
        end_dir = self.end_time.strftime(fs_date_format)
        return self.data_dir / "cache" / "v1" / start_dir / end_dir / self.dataset_name

    def data_parquet_file_path(self) -> Path:
        """Returns the path to the `data.parquet` file."""
        return self.dir_path() / PIPELINE_CACHE_DATA_FILENAME

    def stats_json_file_path(self) -> Path:
        """Returns the path to the `stats.json` file."""
        return self.dir_path() / PIPELINE_CACHE_STATS_FILENAME


class PipelineRemoteCache(Protocol):
    """
    Represent the possibility of fetching a cache entry from a
    remote location or service (e.g. a GCS bucket).

    Methods:
        sync: sync remote cache entry to disk and return whether
            we successfully synced it or not.
    """

    def sync(self, entry: PipelineCacheEntry) -> bool: ...


class PipelineCacheManager:
    """Manages the cache populated by the IQBPipeline."""

    def __init__(
        self,
        data_dir: str | Path | None = None,
        remote_cache: PipelineRemoteCache | None = None,
    ):
        """
        Initialize cache with data directory path.

        Parameters:
            data_dir: Path to directory containing cached data files.
                If None, defaults to .iqb/ in current working directory.
            remote_cache: Optional remote cache for fetching cached query results.
        """
        self.data_dir = data_dir_or_default(data_dir)
        self.remote_cache = remote_cache

    def get_cache_entry(
        self,
        *,
        dataset_name: str,
        start_date: str,
        end_date: str,
        fetch_if_missing: bool = False,
    ) -> PipelineCacheEntry:
        """
        Get cache entry for the given query template.

        If the entry exists on disk, returns it immediately.

        If the entry does not exist and fetch_if_missing is True and
        a remote_cache was configured, attempts to sync from remote cache.

        Args:
            dataset_name: Name of the dataset (e.g., "downloads_by_country")
            start_date: Date when to start the query (included) -- format YYYY-MM-DD
            end_date: Date when to end the query (excluded) -- format YYYY-MM-DD
            fetch_if_missing: Whether to try fetching from remote cache if missing

        Returns:
            PipelineCacheEntry with correctly initialized fields.
        """
        # 1. parse the start and the end dates
        start_time, end_time = _parse_both_dates(start_date, end_date)

        # 2. ensure the dataset name is correct
        if not re.match(r"^[a-z0-9_]+$", dataset_name):
            raise ValueError(f"Invalid dataset name: {dataset_name}")

        # 3. create the cache entry
        entry = PipelineCacheEntry(
            data_dir=self.data_dir,
            dataset_name=dataset_name,
            start_time=start_time,
            end_time=end_time,
        )

        # 4. if the entry exists locally, we're done
        if entry.data_parquet_file_path().exists() and entry.stats_json_file_path().exists():
            return entry

        # 5. try fetching from remote cache if requested and available
        if fetch_if_missing and self.remote_cache is not None:
            self.remote_cache.sync(entry)

        # 6. return the entry (may or may not exist on disk now)
        return entry


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


def _parse_date(value: str) -> datetime:
    """Ensure that a single date is consistent with the format and return it parsed."""
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {value} (expected YYYY-MM-DD)") from e

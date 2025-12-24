"""Module to manage the on-disk IQB measurements cache."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final, Protocol

from filelock import BaseFileLock, FileLock

# Cache file names
PIPELINE_CACHE_DATA_FILENAME: Final[str] = "data.parquet"
PIPELINE_CACHE_DOTLOCK_FILENAME: Final[str] = ".lock"
PIPELINE_CACHE_STATS_FILENAME: Final[str] = "stats.json"


class PipelineEntrySyncError(RuntimeError):
    """Error emitted when we cannot sync the entry."""


@dataclass(kw_only=True)
class PipelineCacheEntry:
    """
    Reference to a cache entry containing query results and metadata.

    Attributes:
        data_dir: the Path that points to the data dir
        dataset_name: the name of the dataset
        start_time: the datetime containing the start time
        end_time: the datetime containing the end time
        syncers: functions to sync the entry
    """

    data_dir: Path
    dataset_name: str
    start_time: datetime
    end_time: datetime
    syncers: list[Callable[[PipelineCacheEntry], bool]]

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

    def lock(self) -> BaseFileLock:
        """Return a FileLock locking the entry."""
        lock_file_path = self.dir_path() / PIPELINE_CACHE_DOTLOCK_FILENAME
        lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        return FileLock(lock_file_path)

    def exists(self) -> bool:
        """Return True if the entry files exist, False otherwise."""
        return self.stats_json_file_path().exists() and self.data_parquet_file_path().exists()

    def sync(self) -> None:
        """
        Sync the entry using all the configured syncers.

        If one syncer succeeds, the entry is considered synced and this
        method returns. If syncers are present and all fail, raise a
        PipelineEntrySyncError. If there are no syncers configured, this
        method only succeeds when the entry files already exist on disk;
        otherwise it raises FileNotFoundError.
        """
        if self.syncers:
            if not any(sync(self) for sync in self.syncers):
                raise PipelineEntrySyncError(f"Cannot sync {self}: see above logs")
            return

        if not self.exists():
            start_date = self.start_time.date().isoformat()
            end_date = self.end_time.date().isoformat()
            raise FileNotFoundError(
                f"Cache entry not found for {self.dataset_name} ({start_date} to {end_date})"
            )


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
    ) -> PipelineCacheEntry:
        """
        Get cache entry for the given query template.

        Use `PipelineCacheEntry.sync` to fetch the files. The entry is lazy
        and may not exist on disk until you sync it.

        Args:
            dataset_name: Name of the dataset (e.g., "downloads_by_country")
            start_date: Date when to start the query (included) -- format YYYY-MM-DD
            end_date: Date when to end the query (excluded) -- format YYYY-MM-DD

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
            syncers=[],
        )

        # 4. if a remote_cache exists, configure it as a syncer
        if self.remote_cache is not None:
            entry.syncers.append(self.remote_cache.sync)

        # 5. return the entry (may not exist on disk until we .sync it)
        return entry


def data_dir_or_default(data_dir: str | Path | None) -> Path:
    """
    Return data_dir as a Path if not empty. Otherwise return the
    default value for the data_dir (i.e., `./.iqb` like git).
    """
    return Path.cwd() / ".iqb" if data_dir is None else Path(data_dir)


def _parse_both_dates(start_date: str, end_date: str) -> tuple[datetime, datetime]:
    """Parses both dates and ensures start_date < end_date."""
    start_time = _parse_date(start_date, descr="start date")
    end_time = _parse_date(end_date, descr="end date")
    if start_time >= end_time:
        raise ValueError(f"start_date must be < end_date, got: {start_date} >= {end_date}")
    return start_time, end_time


def _parse_date(value: str, *, descr: str = "date") -> datetime:
    """Ensure that a single date is consistent with the format and return it parsed."""
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid {descr} format: {value} (expected YYYY-MM-DD)") from exc

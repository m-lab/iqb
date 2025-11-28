"""Module to read and write the on-disk IQB measurements cache."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final

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

    # TODO(bassosimone): returning None here is wrong and a better
    # approach would be to return the path and let the caller
    # decide whether the path is actually valid!

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
        Get cache entry for the given query template.

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

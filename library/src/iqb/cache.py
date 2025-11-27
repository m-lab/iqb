"""Module for fetching IQB measurement data from cache.

The IQBCache component manages local caching of IQB measurement data, following
a Git-like convention for storing local state.

Cache Directory Convention
---------------------------
By default, IQBCache looks for a `.iqb/` directory in the current working
directory, similar to how Git uses `.git/` for repository state. This provides:

- Clear ownership (`.iqb/` contains IQB-specific data)
- Per-project isolation (each project has its own cache directory)
- Conventional pattern (like `.cache/`, `.config/`, etc.)

Example usage:

    # Uses ./.iqb/ in current directory
    cache = IQBCache()

    # Or specify custom location
    cache = IQBCache(data_dir="/shared/iqb-cache")
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from .pipeline import PipelineCacheManager, data_dir_or_default


@dataclass(frozen=True)
class CacheEntry:
    """
    Entry inside the data cache.

    Attributes:
        download_data: full path to data.parquet for download
        upload_data: full path to data.parquet for upload
        download_stats: full path to stats.json for download
        upload_stats: full path to stats.json for upload
    """

    download_data: Path
    upload_data: Path
    download_stats: Path
    upload_stats: Path

    @staticmethod
    def _read_data_frame(
        filepath: Path,
        *,
        country_code: str | None,
        asn: int | None,
        city: str | None,
        columns: list[str] | None,
    ) -> pd.DataFrame:
        # 1. setup the reading filters to efficiently skip groups of rows
        # PyArrow filters: list of tuples (column, operator, value)
        filters = []
        if asn is not None:
            filters.append(("asn", "=", asn))
        if city is not None:
            filters.append(("city", "=", city))
        if country_code is not None:
            filters.append(("country_code", "=", country_code))

        # 2. load in memory using the filters and potentially cutting the columns
        # Note: PyArrow requires filters=None (not []) when there are no filters
        table = pq.read_table(filepath, filters=filters if filters else None, columns=columns)

        # 3. finally convert to data frame
        return table.to_pandas()

    def read_download_data_frame(
        self,
        *,
        country_code: str | None = None,
        asn: int | None = None,
        city: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Load the download dataset as a dataframe.

        The arguments allow to select a subset of the entire dataset.

        Arguments:
          country_code: either None or the desired country code (e.g., "IT")
          asn: either None or the desired ASN (e.g., 137)
          city: either None or the desired city (e.g., "Boston")
          columns: either None (all columns) or list of column names to read

        Return:
          A pandas DataFrame.
        """
        return self._read_data_frame(
            self.download_data,
            country_code=country_code,
            asn=asn,
            city=city,
            columns=columns,
        )

    def read_upload_data_frame(
        self,
        *,
        country_code: str | None = None,
        asn: int | None = None,
        city: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Load the upload dataset as a dataframe.

        The arguments allow to select a subset of the entire dataset.

        Arguments:
          country_code: either None or the desired country code (e.g., "IT")
          asn: either None or the desired ASN (e.g., 137)
          city: either None or the desired city (e.g., "Boston")
          columns: either None (all columns) or list of column names to read

        Return:
          A pandas DataFrame.
        """
        return self._read_data_frame(
            self.upload_data,
            country_code=country_code,
            asn=asn,
            city=city,
            columns=columns,
        )


class IQBCache:
    """Component for fetching IQB measurement data from cache."""

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
        *,
        start_date: str,
        end_date: str,
        granularity: str,
    ) -> CacheEntry:
        """
        Get cache entry associated with given dates and granularity.

        The available granularities are:

        1. "country"
        2. "country_asn"
        3. "country_city"
        4. "country_city_asn"

        Note that this function is a low level building block allowing you to
        access and filter data very efficiently. Consider using higher-level
        user-friendlty APIs when they are actually available.

        The returned CacheEntry allows you to read raw data as DataFrame.

        Arguments:
            start_date: start measurement date expressed as YYYY-MM-DD (included)
            end_date: end measurement date expressed as YYYY-MM-DD (excluded)
            granularity: the granularity to use

        Return:
            A CacheEntry instance.

        Example:
            >>> # Returns data for October 2025
            >>> result = cache.get_cache_entry("2025-10-01", "2025-11-01", "country")
        """
        # 1. create a temporary cache manager instance
        manager = PipelineCacheManager(self.data_dir)

        # 2. check whether the download entry exists
        download_entry = manager.get_cache_entry(
            f"downloads_by_{granularity}",
            start_date,
            end_date,
        )
        download_data = download_entry.data_path()
        download_stats = download_entry.stats_path()
        if download_data is None or download_stats is None:
            raise FileNotFoundError(
                f"Cache entry not found for downloads_by_{granularity} ({start_date} to {end_date})"
            )

        # 3. check whether the upload entry exists
        upload_entry = manager.get_cache_entry(
            f"uploads_by_{granularity}",
            start_date,
            end_date,
        )
        upload_data = upload_entry.data_path()
        upload_stats = upload_entry.stats_path()
        if upload_data is None or upload_stats is None:
            raise FileNotFoundError(
                f"Cache entry not found for uploads_by_{granularity} ({start_date} to {end_date})"
            )

        # 4. return the actual cache entry
        return CacheEntry(
            download_data=download_data,
            upload_data=upload_data,
            download_stats=download_stats,
            upload_stats=upload_stats,
        )

    def get_data(
        self,
        country: str,
        start_date: datetime,
        end_date: datetime | None = None,
        percentile: int = 95,
    ) -> dict:
        """
        Fetch measurement data for IQB calculation.

        Args:
            country: ISO 2-letter country code (e.g., "US", "DE", "BR", "FR", etc.).
            start_date: Start of date range (inclusive).
            end_date: End of date range (exclusive). If None, defaults to start_date + 1 month.
            percentile: Which percentile to extract (1-99).

        Returns:
            dict with keys for IQBCalculator:
            {
                "download_throughput_mbps": float,
                "upload_throughput_mbps": float,
                "latency_ms": float,
                "packet_loss": float,
            }

        Raises:
            FileNotFoundError: If requested data is not available in cache.
            ValueError: If requested percentile is not available in cached data.

        ⚠️  PERCENTILE INTERPRETATION (CRITICAL!)
        =========================================

        For "higher is better" metrics (throughput):
          - Raw p95 = "95% of users have ≤ 625 Mbit/s speed"
          - Directly usable: download_p95 ≥ threshold?
          - No inversion needed (standard statistical definition)

        For "lower is better" metrics (latency, packet loss):
          - Raw p95 = "95% of users have ≤ 80ms latency" (worst-case typical)
          - We want p95 to represent best-case typical (to match throughput)
          - Solution: Use p5 raw labeled as p95
          - Mathematical inversion: p(X)_labeled = p(100-X)_raw
          - Example: OFFSET(5) raw → labeled as "latency_p95" in JSON

        This inversion happens in BigQuery (see data/query_*.sql),
        so this cache code treats all percentiles uniformly.

        When you request percentile=95, you get the 95th percentile value
        that can be compared uniformly against thresholds.

        NOTE: This creates semantics where p95 represents "typical best
        performance" - empirical validation will determine if appropriate.
        """
        # TODO(bassosimone): we should convert this method to become
        # a wrapper of `get_cache_entry` in the future.

        # Design Note
        # -----------
        #
        # For now, we are going with separate pipelines producing data for
        # separate data sources, since this scales well incrementally.
        #
        # Additionally, note how the data is relatively small regardless
        # of the time window that we're choosing (it's always four metrics
        # each of which contains between 25 and 100 percentiles). So,
        # computationally, gluing together N datasets in here will never
        # become an expensive operation.
        #
        # We may revisit this choice when we approach production readiness.

        # 1. Normalize country to be lowercase
        country_lower = country.lower()

        # 2. Create the dictionary with the results
        results = {}

        # TODO(bassosimone): a design choice here is whether we want to
        # allow for partial data (e.g., we have m-lab data but we do not
        # have cloudflare data), which happens right now with a static
        # cache and may also happen when fetching from remote. It is not
        # an issue right now, since there's just the m-lab data source.

        # 3. Attempt to fill the results with m-lab data
        results["m-lab"] = self._get_mlab_data(
            country_lower=country_lower,
            start_date=start_date,
            end_date=end_date,
            percentile=percentile,
        )

        # 4. Attempt to fill the results with cloudflare data
        # TODO(bassosimone): implement

        # 5. Attempt to fill the results with ookla data
        # TODO(bassosimone): implement

        # 6. Return assembled result
        return results

    def _get_mlab_data(
        self,
        country_lower: str,
        start_date: datetime,
        end_date: datetime | None = None,
        percentile: int = 95,
    ) -> dict:
        """Return m-lab data with the given country code, dates, etc."""

        # Map datetime to period string
        # We only support single-month periods with end_date=None
        if end_date is not None:
            raise FileNotFoundError(
                f"No cached data for country={country_lower}, "
                f"start_date={start_date}, end_date={end_date}. "
                f"Multi-month periods not yet supported."
            )

        # Map known periods to their string representation
        known_periods = {
            datetime(2024, 10, 1): "2024_10",
            datetime(2025, 10, 1): "2025_10",
        }

        if start_date not in known_periods:
            raise FileNotFoundError(
                f"No cached data for country={country_lower}, "
                f"start_date={start_date}, end_date={end_date}. "
                f"Available periods: {list(known_periods.keys())}"
            )

        period_str = known_periods[start_date]
        filename = f"{country_lower}_{period_str}.json"
        filepath = self.data_dir / "cache" / "v0" / filename

        # Check if file exists
        if not filepath.exists():
            raise FileNotFoundError(
                f"No cached data file found: {filepath}. "
                f"File does not exist for country={country_lower}, period={period_str}"
            )

        # Load from file
        with open(filepath) as filep:
            data = json.load(filep)

        # Extract the requested percentile
        return self._extract_percentile(data, percentile)

    def _extract_percentile(self, data: dict, percentile: int) -> dict:
        """
        Extract specific percentile from JSON data.

        Converts from JSON format to IQBCalculator format:
        - Input: {"metrics": {"download_throughput_mbps": {"p95": 123, ...}, ...}}
        - Output: {"download_throughput_mbps": 123, ...}

        Args:
            data: Full JSON data structure from cache file.
            percentile: Which percentile to extract.

        Returns:
            dict with metric values for the specified percentile.

        Raises:
            ValueError: If requested percentile is not available in the cached data.
        """
        metrics = data["metrics"]
        p_key = f"p{percentile}"

        try:
            return {
                "download_throughput_mbps": metrics["download_throughput_mbps"][p_key],
                "upload_throughput_mbps": metrics["upload_throughput_mbps"][p_key],
                "latency_ms": metrics["latency_ms"][p_key],
                "packet_loss": metrics["packet_loss"][p_key],
            }
        except KeyError as err:
            # Determine which percentiles ARE available
            available = sorted(
                [int(k[1:]) for k in metrics["download_throughput_mbps"] if k.startswith("p")]
            )
            raise ValueError(
                f"Percentile {percentile} not available in cached data. "
                f"Available percentiles: {available}"
            ) from err

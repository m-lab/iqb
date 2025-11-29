"""Package for fetching IQB measurement data from cache.

The IQBCache component manages local caching of IQB measurement data, following
a Git-like convention for storing local state.

Cache Directory Convention
---------------------------
By default, IQBCache looks for a `.iqb/` directory in the current working
directory, similar to how Git uses `.git/` for repository state. This provides:

- Clear ownership (`.iqb/` contains IQB-specific data)
- Per-project isolation (each project has its own cache directory)
- Conventional pattern (like `.cache/`, `.config/`, etc.)

⚠️  PERCENTILE INTERPRETATION (CRITICAL!)
----------------------------------------

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

Example usage
-------------

    # Uses .iqb/ in current directory
    cache = IQBCache()

    # Or specify custom location
    cache = IQBCache(data_dir="/shared/iqb-cache")
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta

from ..pipeline.dataset import IQBDatasetGranularity
from ..pipeline.pipeline import PipelineCacheManager
from .mlab import MLabCacheEntry, MLabCacheReader


@dataclass(frozen=True)
class CacheEntry:
    """
    Cached data for computing IQB scores.

    Use the entries methods for obtaining pandas DataFrames.

    Attributes:
      mlab: Cached M-Lab data.
    """

    mlab: MLabCacheEntry


class IQBCache:
    """Component for fetching IQB measurement data from cache."""

    def __init__(self, data_dir: str | Path | None = None):
        """
        Initialize cache with data directory path.

        Parameters:
            data_dir: Path to directory containing cached data files.
                If None, defaults to .iqb/ in current working directory.
        """
        self.manager = PipelineCacheManager(data_dir)
        self.mlab = MLabCacheReader(self.manager)

    @property
    def data_dir(self) -> Path:
        """Return the data directory used by the cache."""
        return self.manager.data_dir

    def get_cache_entry(
        self,
        *,
        start_date: str,
        end_date: str,
        granularity: IQBDatasetGranularity,
    ) -> CacheEntry:
        """
        Get cache entry associated with given dates and granularity.

        The returned CacheEntry allows you to read raw data as DataFrame.

        Arguments:
            start_date: start measurement date expressed as YYYY-MM-DD (included)
            end_date: end measurement date expressed as YYYY-MM-DD (excluded)
            granularity: the granularity to use

        Return:
            A CacheEntry instance.

        Example:
            >>> # Returns the cached data for October 2025
            >>> entry = cache.get_cache_entry(
            ...     start_date="2025-10-01",
            ...     end_date="2025-11-01",
            ...     granularity=IQBDatasetGranularity.COUNTRY,
            ... )
        """

        # 1. Obtain cached M-Lab data
        mlab_entry = self.mlab.get_cache_entry(
            start_date=start_date,
            end_date=end_date,
            granularity=granularity,
        )

        # 2. Try obtaining cached cloudflare data
        # TODO(bassosimone): implement

        # 3. Try obtaining cached ookla data
        # TODO(bassosimone): implement

        # 4. Fill the result
        return CacheEntry(mlab=mlab_entry)

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
            country: ISO 2-letter country code (e.g., "US").
            start_date: Start of date range (inclusive).
            end_date: End of date range (exclusive). If None, defaults to start_date + 1 month.
            percentile: Which percentile to extract (1-99).

        Returns:
            dict with keys for IQBCalculator:

            {
                "m-lab": {
                    "download_throughput_mbps": float,
                    "upload_throughput_mbps": float,
                    "latency_ms": float,
                    "packet_loss": float,
                }
            }

        Raises:
            FileNotFoundError: If requested data is not available in cache.
            ValueError: If requested percentile is not available in cached data.
        """

        # Design Note
        # -----------
        #
        # We are going with separate pipelines producing data for separate
        # data sources, since this scales well incrementally.
        #
        # Additionally, note how the data is relatively small regardless
        # of the time window that we're choosing (it's always four metrics
        # each of which contains between 25 and 100 percentiles). So,
        # computationally, gluing together N datasets in here will never
        # become an expensive operation.

        # 1. Normalize country to be uppercase
        country_upper = country.upper()

        # 2. Assign a value to end_date if not set
        if end_date is None:
            end_date = start_date + relativedelta(months=1)

        # 3. Create the dictionary with the results
        results = {}

        # 4. Attempt to fill the results with m-lab data
        results["m-lab"] = self.mlab.get_data(
            country_code=country_upper,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            percentile=percentile,
            granularity=IQBDatasetGranularity.COUNTRY,
        )

        # 4. Attempt to fill the results with cloudflare data
        # TODO(bassosimone): implement

        # 5. Attempt to fill the results with ookla data
        # TODO(bassosimone): implement

        # 6. Return assembled result
        return results

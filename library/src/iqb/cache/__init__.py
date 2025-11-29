"""Package for reading IQB measurement data from cache.

The IQBCache component provides read access to cached IQB measurement data.
The cache is written by the `iqb.pipeline` package, which owns the on-disk
data format specification.

Cache Data Format Ownership
----------------------------
The `iqb.pipeline` package owns and specifies the on-disk cache structure.
This package (iqb.cache) reads that structure using primitives provided by
`iqb.pipeline` to ensure consistency.

See `iqb.pipeline` documentation for the complete cache format specification.

Cache Directory Convention
---------------------------
By default, IQBCache looks for a `.iqb/` directory in the current working
directory, similar to how Git uses `.git/` for repository state. This provides:

- Clear ownership (`.iqb/` contains IQB-specific data)
- Per-project isolation (each project has its own cache directory)
- Conventional pattern (like `.cache/`, `.config/`, etc.)

Example usage:

    # Uses .iqb/ in current directory
    cache = IQBCache()

    # Or specify custom location
    cache = IQBCache(data_dir="/shared/iqb-cache")

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
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from ..pipeline import (
    IQBDatasetGranularity,
)
from ..pipeline.cache import (
    PipelineCacheManager,
)
from .mlab import (
    MLabCacheEntry,
    MLabCacheManager,
    iqb_mlab_dataframes_to_summary,
)


@dataclass(frozen=True)
class CacheEntry:
    """
    Entry inside the data cache allowing to read the underlying
    data using pandas DataFrames.
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
        self.mlab = MLabCacheManager(self.manager)

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

        Arguments:
            start_date: start measurement date expressed as YYYY-MM-DD (included)
            end_date: end measurement date expressed as YYYY-MM-DD (excluded)
            granularity: the granularity to use (from IQBDatasetGranularity enum)

        Return:
            A CacheEntry instance.

        Raises:
            FileNotFoundError if there's no cache entry for m-lab. Other
                dataset are optional and don't trigger errors.

        Example:
            >>> from iqb.pipeline import IQBDatasetGranularity
            >>> # Returns data for October 2025
            >>> entry = cache.get_cache_entry(
            ...     start_date="2025-10-01",
            ...     end_date="2025-11-01",
            ...     granularity=IQBDatasetGranularity.BY_COUNTRY,
            ... )
        """
        # 1. read the mlab cache entry
        mlab = self.mlab.get_cache_entry(
            start_date=start_date,
            granularity=granularity,
            end_date=end_date,
        )
        if mlab is None:
            raise FileNotFoundError(
                f"no cache entry for m-lab data "
                f"(granularity={granularity!r}, start_date={start_date}, end_date={end_date})"
            )

        # 2. read the cloudflare cache entry
        # TODO(bassosimone): implement

        # 3. read the ookla cache entry
        # TODO(bassosimone): implement

        # 4. assemble and return the result
        return CacheEntry(mlab=mlab)

    def get_data(
        self,
        *,
        country_code: str,
        asn: int | None = None,
        city: str | None = None,
        start_date: datetime | str,
        end_date: datetime | str,
        granularity: IQBDatasetGranularity,
        percentile: int = 95,
    ) -> dict[str, dict[str, float]]:
        """
        Fetch measurement data for IQB calculation.

        Args:
            country_code: ISO 2-letter country code (e.g., "US", "DE", "BR", "FR", etc.).
            asn: optional ASN (e.g., 137).
            city: optional city name (e.g., "Boston").
            start_date: Start of date range (inclusive) in YYYY-MM-DD format or
                datetime which we will format using %Y-%m-%d.
            end_date: End of date range (exclusive) in YYYY-MM-DD format or
                datetime which we will format using %Y-%m-%d.
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
            ValueError: If requested percentile is not available in cached data
                or the granularity is not compatible with the filters causing
                the code to select zero or multiple rows.

        """
        # Design Note
        # -----------
        #
        # We use separate pipelines producing data for separate data
        # sources, since this scales well incrementally.
        #
        # The data is relatively small regardless of the time window that
        # we're choosing (it's always four metrics each of which
        # contains between 25 and 100 percentiles). So, computationally,
        # gluing together N datasets in here will never become
        # an expensive operation.

        # 0. ensure that the granularity is compatible with the given filters
        if city is not None and "city" not in granularity.value:
            raise ValueError(f"granularity {granularity!r} does not include city")

        if asn is not None and "asn" not in granularity.value:
            raise ValueError(f"granularity {granularity!r} does not include ASN")

        # 1. ensure that the country_code is uppercase
        country_code = country_code.upper()

        # 2. convert start_date to string since cache code wants a string
        if isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y-%m-%d")

        # 3. same as above but for end_date
        if isinstance(end_date, datetime):
            end_date = end_date.strftime("%Y-%m-%d")

        # 4. get the overall cache entry
        entry = self.get_cache_entry(
            start_date=start_date,
            granularity=granularity,
            end_date=end_date,
        )

        # 5. Create the dictionary with the results
        results = {}

        # 6. Fill the results with m-lab data
        mlab_download_df = entry.mlab.read_download_data_frame(
            country_code=country_code,
            asn=asn,
            city=city,
        )
        mlab_upload_df = entry.mlab.read_upload_data_frame(
            country_code=country_code,
            asn=asn,
            city=city,
        )
        mlab_summary = iqb_mlab_dataframes_to_summary(
            percentile=percentile,
            download=mlab_download_df,
            upload=mlab_upload_df,
        )
        results["m-lab"] = mlab_summary.to_dict()

        # 7. Attempt to fill the results with cloudflare data
        # TODO(bassosimone): implement

        # 8. Attempt to fill the results with ookla data
        # TODO(bassosimone): implement

        # 9. Return assembled result
        return results

"""Module implementing IQBCache."""

import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta

from ..pipeline.dataset import IQBDatasetGranularity
from ..pipeline.pipeline import PipelineCacheManager, PipelineRemoteCache
from .mlab import IQBDataMLab, MLabCacheEntry, MLabCacheManager


@dataclass(frozen=True, kw_only=True)
class CacheEntry:
    """
    Cached data for computing IQB scores.

    Use the entries methods for obtaining pandas DataFrames.

    Attributes:
      mlab: Cached M-Lab data.
    """

    mlab: MLabCacheEntry


@dataclass(frozen=True, kw_only=True)
class IQBData:
    """
    Data for computing the IQB score.

    Attributes:
      mlab: M-Lab data.
    """

    mlab: IQBDataMLab


class IQBCache:
    """Component for fetching IQB measurement data from cache."""

    def __init__(
        self,
        *,
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
        self.manager = PipelineCacheManager(data_dir, remote_cache=remote_cache)
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

        The returned CacheEntry is lazy: reading data frames may trigger
        a sync/fetch from cache providers under an entry-level lock.

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

    def get_iqb_data(
        self,
        *,
        granularity: IQBDatasetGranularity,
        country_code: str,
        start_date: str,
        end_date: str,
        asn: int | None = None,
        city: str | None = None,
        percentile: int = 95,
    ) -> IQBData:
        """
        Fetch measurement data for IQB calculation.

        This method may trigger cache sync under an entry-level lock.

        Args:
            granularity: The granularity to use.
            country_code: ISO 2-letter country code (e.g., "US").
            start_date: Start of date range (inclusive) using YYYY-MM-DD format.
            end_date: End of date range (exclusive) using YYYY-MM-DD format.
            asn: Optional ASN to filter for (e.g., 137).
            city: Optional city to filter for (e.g., "Boston").
            percentile: Which percentile to extract (1-99).

        Returns:
            IQBData instance.

        Raises:
            FileNotFoundError: If requested data is not available in cache.
            ValueError: If requested percentile is not available in cached data.
        """

        # 1. Obtain M-Lab IQB data
        mlab_data = self.mlab.get_iqb_data(
            start_date=start_date,
            end_date=end_date,
            granularity=granularity,
            country_code=country_code,
            asn=asn,
            city=city,
            percentile=percentile,
        )

        # 2. Try obtaining cloudflare IQB data
        # TODO(bassosimone): implement

        # 3. Try obtaining ookla IQB data
        # TODO(bassosimone): implement

        # 4. Fill the result
        return IQBData(mlab=mlab_data)

    @warnings.deprecated("use get_iqb_data instead")
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

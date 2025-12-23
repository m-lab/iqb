"""Module for reading the m-lab cache."""

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ..pipeline import (
    IQBDatasetGranularity,
    iqb_dataset_name_for_mlab,
    iqb_parquet_read,
)
from ..pipeline.cache import PipelineCacheEntry
from ..pipeline.dataset import IQBDatasetMLabTable
from ..pipeline.pipeline import PipelineCacheManager


@dataclass(frozen=True)
class IQBDataMLab:
    """
    Contains M-Lab data for computing the IQB score.

    Attributes:
        download: Download speed in Mbit/s.
        upload: Upload speed in Mbit/s.
        latency: Minimum RTT in ms.
        loss: Packet loss rate.
    """

    download: float
    upload: float
    latency: float
    loss: float

    def to_dict(self) -> dict[str, float]:
        """Convert to standard dict used to compute IQB score."""
        return {
            "download_throughput_mbps": float(self.download),
            "upload_throughput_mbps": float(self.upload),
            "latency_ms": float(self.latency),
            "packet_loss": float(self.loss),
        }


@dataclass(frozen=True)
class MLabDataFramePair:
    """
    Pair of DataFrames containing M-Lab measurement data.

    This class represents pre-filtered measurement data ready for conversion to
    various output formats. The DataFrames contain all percentile columns, allowing
    flexible extraction of any percentile.

    Attributes:
        download_df: pandas DataFrame with download/latency/loss data
        upload_df: pandas DataFrame with upload data
    """

    download: pd.DataFrame
    upload: pd.DataFrame

    def to_iqb_data(self, *, percentile: int = 95) -> IQBDataMLab:
        """
        Converts the DataFramePair to a dictionary suitable for IQBCalculator.

        Args:
            percentile: The percentile to extract (1-99), default 95

        Returns:
            An IQBDataMLab instance.

        Raises:
            ValueError: If the DataFrames don't have exactly one row, or if
                the required percentile columns don't exist.
        """
        # 1. Ensure we have exactly one row in each DataFrame
        if len(self.download) != 1:
            raise ValueError(
                f"Expected exactly 1 row in download DataFrame, but got {len(self.download)} rows"
            )

        if len(self.upload) != 1:
            raise ValueError(
                f"Expected exactly 1 row in upload DataFrame, but got {len(self.upload)} rows"
            )

        # 2. Construct the percentile column names
        download_col = f"download_p{percentile}"
        upload_col = f"upload_p{percentile}"
        latency_col = f"latency_p{percentile}"
        loss_col = f"loss_p{percentile}"

        # 3. Check that the percentile columns actually exist
        for col in [download_col, latency_col, loss_col]:
            if col not in self.download.columns:
                raise ValueError(
                    f"Percentile column '{col}' not found in download data. "
                    f"Available columns: {list(self.download.columns)}"
                )

        if upload_col not in self.upload.columns:
            raise ValueError(
                f"Percentile column '{upload_col}' not found in upload data. "
                f"Available columns: {list(self.upload.columns)}"
            )

        # 4. Extract the single row we need
        download_row = self.download.iloc[0]
        upload_row = self.upload.iloc[0]

        # 5. Return the dict with explicit float conversion
        return IQBDataMLab(
            download=float(download_row[download_col]),
            upload=float(upload_row[upload_col]),
            latency=float(download_row[latency_col]),
            loss=float(download_row[loss_col]),
        )


@dataclass(frozen=True)
class MLabCacheEntry:
    """
    M-Lab entry inside the data cache.

    Attributes:
        granularity: granularity used by this dataset
        start_date: the start date used by this dataset (YYYY-MM-DD; included)
        end_date: the end date used by this dataset (YYYY-MM-DD; excluded)
        upload: reference to the upload entry
        download: reference to the download entry
    """

    start_date: str
    end_date: str
    granularity: IQBDatasetGranularity
    download: PipelineCacheEntry
    upload: PipelineCacheEntry

    @property
    def download_data(self) -> Path:
        return self.download.data_parquet_file_path()

    @property
    def download_stats(self) -> Path:
        return self.download.stats_json_file_path()

    @property
    def upload_data(self) -> Path:
        return self.upload.data_parquet_file_path()

    @property
    def upload_stats(self) -> Path:
        return self.upload.stats_json_file_path()

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
        return iqb_parquet_read(
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
        return iqb_parquet_read(
            self.upload_data,
            country_code=country_code,
            asn=asn,
            city=city,
            columns=columns,
        )

    def read_data_frame_pair(
        self,
        *,
        country_code: str,
        city: str | None = None,
        asn: int | None = None,
    ) -> MLabDataFramePair:
        """
        High-level API: Get filtered download/upload data for specific parameters.

        This method provides a convenient way to get measurement data ready for
        conversion to IQBCalculator format or other analysis. All the columns are
        loaded, allowing for flexible extraction later.

        Arguments:
            country_code: ISO 2-letter country code (e.g., "US", "DE")
            city: Optional city name (required for "country_city" and "country_city_asn" granularity)
            asn: Optional ASN number (required for "country_city_asn" granularity)

        Returns:
            DataFramePair containing filtered download and upload DataFrames
            with all the original percentile columns

        Raises:
            ValueError: If the requested granularity is incompatible with the
                cache granularity (e.g., requesting city with country-level data)

        Example:
            >>> entry = cache.get_cache_entry("2024-10-01", "2024-11-01", "country")
            >>> pair = entry.get_data_frame_pair(country_code="US")
            >>> data_p95 = pair.to_dict(percentile=95)
            >>> data_p50 = pair.to_dict(percentile=50)
        """
        # 1. Validate granularity compatibility
        if city is not None and "city" not in self.granularity:
            raise ValueError(
                f"Cannot filter by city with granularity '{self.granularity}'. "
                f"Use granularity containing 'city' (e.g., 'country_city')."
            )

        if asn is not None and "asn" not in self.granularity:
            raise ValueError(
                f"Cannot filter by ASN with granularity '{self.granularity}'. "
                f"Use granularity containing 'asn' (e.g., 'country_asn')."
            )

        # 2. Read download data with filtering (all columns for flexibility)
        download_df = self.read_download_data_frame(
            country_code=country_code,
            city=city,
            asn=asn,
        )

        # 3. Read upload data with filtering (all columns for flexibility)
        upload_df = self.read_upload_data_frame(
            country_code=country_code,
            city=city,
            asn=asn,
        )

        # 4. Make and return the pair
        return MLabDataFramePair(
            download=download_df,
            upload=upload_df,
        )


class MLabCacheManager:
    """Component for managing the M-Lab cache."""

    def __init__(self, manager: PipelineCacheManager):
        """
        Initialize cache with data directory path.

        Parameters:
            manager: the PipelineCacheManager to use.
        """
        self.manager = manager

    def get_cache_entry(
        self,
        *,
        start_date: str,
        end_date: str,
        granularity: IQBDatasetGranularity,
    ) -> MLabCacheEntry:
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
            >>> # Returns data for October 2025
            >>> entry = cache.get_mlab_cache_entry(
            ...     start_date="2025-10-01",
            ...     end_date="2025-11-01",
            ...     granularity=IQBDatasetGranularity.COUNTRY,
            ... )
        """
        # 1. get the download entry
        download_dataset_name = iqb_dataset_name_for_mlab(
            granularity=granularity,
            table=IQBDatasetMLabTable.DOWNLOAD,
        )
        download_entry = self.manager.get_cache_entry(
            dataset_name=download_dataset_name,
            start_date=start_date,
            end_date=end_date,
        )

        # 2. get the upload entry
        upload_dataset_name = iqb_dataset_name_for_mlab(
            granularity=granularity,
            table=IQBDatasetMLabTable.UPLOAD,
        )
        upload_entry = self.manager.get_cache_entry(
            dataset_name=upload_dataset_name,
            start_date=start_date,
            end_date=end_date,
        )

        # 3. bail if entries are missing
        if not download_entry.exists() or not upload_entry.exists():
            raise FileNotFoundError(
                f"Cache entry not found for {download_dataset_name} {upload_dataset_name}"
                f" ({start_date} to {end_date})"
            )

        # 4. return the actual cache entry
        return MLabCacheEntry(
            granularity=granularity,
            start_date=start_date,
            end_date=end_date,
            download=download_entry,
            upload=upload_entry,
        )

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
    ) -> IQBDataMLab:
        """
        Fetch M-Lab measurement data for IQB calculation.

        Args:
            granularity: The granularity to use.
            country_code: ISO 2-letter country code (e.g., "US").
            start_date: Start of date range (inclusive) using YYYY-MM-DD format.
            end_date: End of date range (exclusive) using YYYY-MM-DD format.
            asn: Optional ASN to filter for (e.g., 137).
            city: Optional city to filter for (e.g., "Boston").
            percentile: Which percentile to extract (1-99).

        Returns:
            IQBDataMLab instance.

        Raises:
            FileNotFoundError: If requested data is not available in cache.
            ValueError: If requested percentile is not available in cached data.
        """

        entry = self.get_cache_entry(
            start_date=start_date,
            end_date=end_date,
            granularity=granularity,
        )

        pair = entry.read_data_frame_pair(
            country_code=country_code,
            asn=asn,
            city=city,
        )

        return pair.to_iqb_data(percentile=percentile)

    def get_data(
        self,
        *,
        granularity: IQBDatasetGranularity,
        country_code: str,
        start_date: str,
        end_date: str,
        asn: int | None = None,
        city: str | None = None,
        percentile: int = 95,
    ) -> dict[str, float]:
        """
        Fetch M-Lab measurement data for IQB calculation.

        Args:
            granularity: The granularity to use.
            country_code: ISO 2-letter country code (e.g., "US").
            start_date: Start of date range (inclusive) using YYYY-MM-DD format.
            end_date: End of date range (exclusive) using YYYY-MM-DD format.
            asn: Optional ASN to filter for (e.g., 137).
            city: Optional city to filter for (e.g., "Boston").
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
        """

        iqb_data = self.get_iqb_data(
            granularity=granularity,
            country_code=country_code,
            start_date=start_date,
            end_date=end_date,
            asn=asn,
            city=city,
            percentile=percentile,
        )

        return iqb_data.to_dict()

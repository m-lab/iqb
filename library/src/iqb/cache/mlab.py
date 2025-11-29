"""Module for accessing cached M-Lab IQB data."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..pipeline import (
    IQBDatasetGranularity,
    iqb_dataset_name_for_mlab,
    iqb_parquet_read,
)
from ..pipeline.cache import (
    PipelineCacheEntry,
    PipelineCacheManager,
)
from ..pipeline.dataset import (
    PipelineDatasetMLabExperiment,
)
from .summary import IQBSummary


@dataclass(frozen=True)
class MLabCacheEntry:
    """Cache entry containing M-Lab data."""

    download: PipelineCacheEntry
    granularity: IQBDatasetGranularity
    upload: PipelineCacheEntry

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

        Raises:
          FileNotFoundError if files do not exist in cache.
        """
        return iqb_parquet_read(
            self.download.data_parquet_file_path(),
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

        Raises:
          FileNotFoundError if files do not exist in cache.
        """
        return iqb_parquet_read(
            self.upload.data_parquet_file_path(),
            country_code=country_code,
            asn=asn,
            city=city,
            columns=columns,
        )


MLAB_GRANULARITY_TO_MERGE_COLUMNS = {
    IQBDatasetGranularity.BY_COUNTRY: ["country_code"],
    IQBDatasetGranularity.BY_COUNTRY_CITY_ASN: ["country_code", "city", "asn"],
}


def iqb_mlab_merge_download_upload(
    *,
    download: pd.DataFrame,
    granularity: IQBDatasetGranularity,
    upload: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merges the download and upload data frames into a single dataframe.

    We determine the merging columns using the granularity argument.

    Arguments:
      download: data frame containing download data.
      granularity: granularity of the loaded data.
      upload: data frame containing the upload data.

    Return:
      A pandas DataFrame.

    Raises:
      KeyError if we did not correctly update the mapping between
      granularity and columns (should really not happen).
    """

    # 1. determine the columns on which to merge
    merge_cols = MLAB_GRANULARITY_TO_MERGE_COLUMNS[granularity]

    # 2. return the merged data frame
    return pd.merge(
        download,
        upload,
        on=merge_cols,
        how="inner",
        suffixes=("_download", "_upload"),
    )


class MLabCacheManager:
    """Cache manager managing M-Lab data."""

    def __init__(self, manager: PipelineCacheManager) -> None:
        """
        Initialize cache with data directory path.

        Parameters:
            manager: the PipelineCacheManager to use.
        """
        self.manager = manager

    def get_cache_entry(
        self,
        *,
        end_date: str,
        granularity: IQBDatasetGranularity,
        start_date: str,
    ) -> MLabCacheEntry | None:
        """
        Return cache entry associated with given dates and granularity
        or None if the given cache entry does not exist.

        Arguments:
            start_date: start measurement date expressed as YYYY-MM-DD (included)
            end_date: end measurement date expressed as YYYY-MM-DD (excluded)
            granularity: the granularity to use (from IQBDatasetGranularity enum)

        Return:
            A MLabCacheEntry instance.

        Example:
            >>> # Returns data for October 2025
            >>> from iqb.pipeline import IQBDatasetGranularity
            >>> entry = cache.get_cache_entry(
            ...     start_date="2025-10-01",
            ...     end_date="2025-11-01",
            ...     granularity=IQBDatasetGranularity.BY_COUNTRY,
            ... )
            >>> assert entry is not None
        """
        # 1. check whether the download entry actually exists
        download_dataset = iqb_dataset_name_for_mlab(
            experiment=PipelineDatasetMLabExperiment.DOWNLOAD,
            granularity=granularity,
        )
        download_entry = self.manager.get_cache_entry(
            download_dataset,
            start_date,
            end_date,
        )
        assert download_entry is not None
        download_data = download_entry.data_parquet_file_path()
        download_stats = download_entry.stats_json_file_path()
        if not download_data.exists() or not download_stats.exists():
            return None

        # 2. check whether the upload entry actually exists
        upload_dataset = iqb_dataset_name_for_mlab(
            experiment=PipelineDatasetMLabExperiment.UPLOAD,
            granularity=granularity,
        )
        upload_entry = self.manager.get_cache_entry(
            upload_dataset,
            start_date,
            end_date,
        )
        assert upload_entry is not None
        upload_data = upload_entry.data_parquet_file_path()
        upload_stats = upload_entry.stats_json_file_path()
        if not upload_data.exists() or not upload_stats.exists():
            return None

        # 3. return the actual cache entry
        return MLabCacheEntry(
            download=download_entry,
            granularity=granularity,
            upload=upload_entry,
        )


def iqb_mlab_dataframes_to_summary(
    *,
    download: pd.DataFrame,
    percentile: int = 95,
    upload: pd.DataFrame,
) -> IQBSummary:
    """
    Converts the download and upload data frames to an IQBSummary.

    Args:
        download: The data frame containing download data.
        percentile: The percentile to extract (1-99), default 95
        upload: The data frame containing upload data.

    Returns:
        IQBSummary instance.

    Raises:
        ValueError: If either data frame doesn't have exactly one row, the
            required percentile columns don't exist, or the percentile value
            is not between 1 and 99 (inclusive).
    """
    # 0. ensure that the percentile is correct
    if not (1 <= percentile <= 99):
        raise ValueError(f"percentile must be between 1 and 99, got {percentile}")

    # 1. Ensure we have exactly one row
    if len(download) != 1:
        raise ValueError(f"Expected download to have 1 row, got {len(download)} rows")
    if len(upload) != 1:
        raise ValueError(f"Expected upload to have 1 row, got {len(upload)} rows")

    # 2. Construct the percentile column names
    download_col = f"download_p{percentile}"
    upload_col = f"upload_p{percentile}"
    latency_col = f"latency_p{percentile}"
    loss_col = f"loss_p{percentile}"

    # 3. Check that the percentile columns actually exist
    for col in [download_col, latency_col, loss_col]:
        if col not in download.columns:
            raise ValueError(
                f"Percentile column '{col}' not found in download data frame. "
                f"Available columns: {list(download.columns)}"
            )
    if upload_col not in upload.columns:
        raise ValueError(
            f"Percentile column '{upload_col}' not found in upload data frame. "
            f"Available columns: {list(upload.columns)}"
        )

    # 4. Extract the single row we need
    download_row = download.iloc[0]
    upload_row = upload.iloc[0]

    # 5. Return the summary with explicit float conversion
    return IQBSummary(
        download_throughput_mbps=float(download_row[download_col]),
        latency_ms=float(download_row[latency_col]),
        packet_loss=float(download_row[loss_col]),
        upload_throughput_mbps=float(upload_row[upload_col]),
    )

"""Module to construct dataset names."""

from enum import Enum


class PipelineDatasetMLabTable(str, Enum):
    """Enumerate available parquet tables in the m-lab dataset."""

    DOWNLOAD = "downloads"
    UPLOAD = "uploads"


class IQBDatasetGranularity(str, Enum):
    """Enumerate available dataset granularity."""

    COUNTRY = "by_country"
    COUNTRY_CITY_ASN = "by_country_city_asn"


def iqb_dataset_name_for_mlab(
    *,
    granularity: IQBDatasetGranularity,
    table: PipelineDatasetMLabTable,
) -> str:
    """
    Construct the name of an m-lab dataset.

    Arguments:
        granularity: the desired dataset granularity
        table: the specific parquet table to read
    """
    return f"{table.value}_{granularity.value}"

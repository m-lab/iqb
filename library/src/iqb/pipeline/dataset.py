"""Module to construct dataset names."""

from enum import Enum


class IQBDatasetMLabTable(str, Enum):
    """Enumerate available parquet tables in the m-lab dataset."""

    DOWNLOAD = "downloads"
    UPLOAD = "uploads"


class IQBDatasetGranularity(str, Enum):
    """Enumerate available dataset granularity."""

    COUNTRY = "by_country"
    COUNTRY_ASN = "by_country_asn"
    COUNTRY_SUBDIVISION1 = "by_country_subdivision1"
    COUNTRY_SUBDIVISION1_ASN = "by_country_subdivision1_asn"
    COUNTRY_CITY = "by_country_city"
    COUNTRY_CITY_ASN = "by_country_city_asn"


def iqb_dataset_name_for_mlab(
    *,
    granularity: IQBDatasetGranularity,
    table: IQBDatasetMLabTable,
) -> str:
    """
    Construct the name of an m-lab dataset.

    Arguments:
        granularity: the desired dataset granularity
        table: the specific parquet table to read
    """
    return f"{table.value}_{granularity.value}"

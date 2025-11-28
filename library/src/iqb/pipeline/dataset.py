"""Module to construct dataset names."""

from dataclasses import dataclass
from enum import Enum


class PipelineDatasetMLabExperiment(str, Enum):
    """Name of experiment inside the M-Lab dataset."""

    DOWNLOAD = "downloads"
    UPLOAD = "uploads"


class IQBDatasetGranularity(str, Enum):
    """Available granularity for aggregates."""

    BY_COUNTRY = "by_country"
    BY_COUNTRY_CITY_ASN = "by_country_city_asn"


@dataclass
class PipelineDatasetName:
    """Name of a dataset on disk.

    Created using functions like iqb_dataset_name_for_mlab
    and other similarly named functions.
    """

    value: str


def iqb_dataset_name_for_mlab(
    *,
    experiment: PipelineDatasetMLabExperiment,
    granularity: IQBDatasetGranularity,
) -> PipelineDatasetName:
    """Construct the name of an m-lab dataset."""
    return PipelineDatasetName(value=f"{experiment.value}_{granularity.value}")

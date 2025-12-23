"""Internet Quality Barometer (IQB) library.

This library provides methods for calculating the IQB score based on
network measurement data, weight matrices, and quality thresholds.
"""

from .cache import IQBCache
from .calculator import IQBCalculator
from .config import IQB_CONFIG
from .pipeline import (
    IQBDatasetGranularity,
    IQBDatasetMLabTable,
    IQBPipeline,
    iqb_dataset_name_for_mlab,
)

# Backward compatibility alias
IQB = IQBCalculator

__all__ = [
    "IQB",
    "IQBCalculator",
    "IQBCache",
    "IQB_CONFIG",
    "IQBPipeline",
    "IQBDatasetGranularity",
    "IQBDatasetMLabTable",
    "iqb_dataset_name_for_mlab",
]
__version__ = "0.4.0"

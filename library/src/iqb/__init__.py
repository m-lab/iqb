"""Internet Quality Barometer (IQB) library.

This library provides methods for calculating the IQB score based on
network measurement data, weight matrices, and quality thresholds.
"""

from importlib.metadata import PackageNotFoundError, version

from .cache import IQBCache
from .calculator import IQBCalculator
from .config import IQB_CONFIG
from .ghremote import IQBGitHubRemoteCache, IQBRemoteCache
from .pipeline import (
    IQBDatasetGranularity,
    IQBDatasetMLabTable,
    IQBPipeline,
    iqb_dataset_name_for_mlab,
)

try:
    __version__ = version("mlab-iqb")
except PackageNotFoundError:  # pragma: no cover - not installed
    __version__ = "0.0.0"

# Backward compatibility alias
IQB = IQBCalculator

__all__ = [
    "IQB",
    "IQBCalculator",
    "IQBCache",
    "IQB_CONFIG",
    "IQBGitHubRemoteCache",
    "IQBRemoteCache",
    "IQBPipeline",
    "IQBDatasetGranularity",
    "IQBDatasetMLabTable",
    "iqb_dataset_name_for_mlab",
    "__version__",
]

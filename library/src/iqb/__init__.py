"""Internet Quality Barometer (IQB) library.

This library provides methods for calculating the IQB score based on
network measurement data, weight matrices, and quality thresholds.
"""

from importlib.metadata import PackageNotFoundError, version

from .cache import IQBCache
from .cache.cache import IQBData
from .cache.mlab import IQBDataMLab
from .calculator import (
    IQB_CONFIG,
    IQB_DEFAULT_CONFIG,
    IQBCalculator,
    IQBConfig,
    IQBConfigDataset,
    IQBConfigNetworkRequirement,
    IQBConfigUseCase,
    iqb_config_from_legacy,
)
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
    "IQBConfig",
    "IQBConfigDataset",
    "IQBConfigNetworkRequirement",
    "IQBConfigUseCase",
    "IQBData",
    "IQBDataMLab",
    "IQB_CONFIG",
    "IQB_DEFAULT_CONFIG",
    "IQBGitHubRemoteCache",
    "IQBRemoteCache",
    "IQBPipeline",
    "IQBDatasetGranularity",
    "IQBDatasetMLabTable",
    "iqb_config_from_legacy",
    "iqb_dataset_name_for_mlab",
    "__version__",
]

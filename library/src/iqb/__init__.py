"""Internet Quality Barometer (IQB) library.

This library provides methods for calculating the IQB score based on
network measurement data, weight matrices, and quality thresholds.
"""

from .cache import IQBCache
from .calculator import IQBCalculator
from .config import IQB_CONFIG

# Backward compatibility alias
IQB = IQBCalculator

__all__ = ["IQB", "IQBCalculator", "IQBCache", "IQB_CONFIG"]
__version__ = "0.1.0"

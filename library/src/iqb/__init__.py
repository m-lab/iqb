"""Internet Quality Barometer (IQB) library.

This library provides methods for calculating the IQB score based on
network measurement data, weight matrices, and quality thresholds.
"""

from .iqb_formula_config import IQB_CONFIG
from .iqb_score import IQB

__all__ = ["IQB", "IQB_CONFIG"]
__version__ = "0.1.0"

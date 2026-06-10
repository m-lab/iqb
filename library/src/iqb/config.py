"""Deprecated: import from iqb instead."""

import warnings

from .calculator.config import IQB_CONFIG

warnings.warn(
    "Importing from iqb.config is deprecated. Use 'from iqb import IQB_CONFIG' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["IQB_CONFIG"]

"""Application state management using dataclasses.

This module provides a cleaner way to manage Streamlit session state
by encapsulating all state in a single dataclass.
"""

from dataclasses import dataclass, field
from typing import Dict

from iqb import IQB


@dataclass
class IQBAppState:
    """Container for all IQB application state.

    This dataclass replaces multiple scattered session state variables
    with a single, well-organized state object.

    Attributes:
        manual_entry: User-entered network measurements
            Structure: {dataset: {requirement: value}}
        thresholds: Minimum threshold values for each requirement
            Structure: {use_case: {requirement: threshold_value}}
        requirement_weights: Importance weights for requirements
            Structure: {use_case: {requirement: weight}}
        use_case_weights: Importance weights for use cases
            Structure: {use_case: weight}
        dataset_weights: Importance weights for datasets per requirement
            Structure: {requirement: {dataset: weight}}
        dataset_exists_in_config: Tracks which datasets are configured
            Structure: {requirement: {dataset: bool}}
        reset_counter: Counter to force UI resets
        iqb: IQB calculator instance
    """

    manual_entry: Dict[str, Dict[str, float]] = field(default_factory=dict)
    thresholds: Dict[str, Dict[str, float]] = field(default_factory=dict)
    requirement_weights: Dict[str, Dict[str, float]] = field(default_factory=dict)
    use_case_weights: Dict[str, float] = field(default_factory=dict)
    dataset_weights: Dict[str, Dict[str, float]] = field(default_factory=dict)
    dataset_exists_in_config: Dict[str, Dict[str, bool]] = field(default_factory=dict)
    reset_counter: int = 0
    iqb: IQB = field(default_factory=IQB)

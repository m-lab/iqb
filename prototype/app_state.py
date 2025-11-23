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

    def reset_manual_entry(self, datasets, requirements, get_default_fn):
        """Reset manual entry to default values.

        Args:
            datasets: List of dataset names
            requirements: List of requirement names
            get_default_fn: Function to get default value for a requirement
        """
        # Clear existing data first
        self.manual_entry = {}

        # Add new data
        for dataset in datasets:
            self.manual_entry[dataset] = {}
            for req in requirements:
                self.manual_entry[dataset][req] = get_default_fn(req)
        self.reset_counter += 1

    def reset_thresholds(self, config):
        """Reset thresholds to config defaults.

        Args:
            config: IQB configuration dictionary
        """
        self.thresholds = {}
        for use_case_name, use_case_config in config["use cases"].items():
            self.thresholds[use_case_name] = {}
            for req_name, req_config in use_case_config["network requirements"].items():
                if "threshold min" in req_config:
                    self.thresholds[use_case_name][req_name] = req_config[
                        "threshold min"
                    ]
        self.reset_counter += 1

    def reset_requirement_weights(self, config):
        """Reset requirement weights to config defaults.

        Args:
            config: IQB configuration dictionary
        """
        self.requirement_weights = {}
        for use_case_name, use_case_config in config["use cases"].items():
            self.requirement_weights[use_case_name] = {}
            for req_name, req_config in use_case_config["network requirements"].items():
                self.requirement_weights[use_case_name][req_name] = req_config.get(
                    "w", 1.0
                )
        self.reset_counter += 1

    def reset_use_case_weights(self, config):
        """Reset use case weights to config defaults.

        Args:
            config: IQB configuration dictionary
        """
        self.use_case_weights = {}
        for use_case_name, use_case_config in config["use cases"].items():
            self.use_case_weights[use_case_name] = use_case_config.get("w", 1.0)
        self.reset_counter += 1

    def reset_dataset_weights(self, config, datasets):
        """Reset dataset weights to config defaults.

        Args:
            config: IQB configuration dictionary
            datasets: List of dataset names
        """
        self.dataset_weights = {}
        self.dataset_exists_in_config = {}

        for req in self._get_requirements_from_config(config):
            self.dataset_weights[req] = {}
            self.dataset_exists_in_config[req] = {}

            found_weights = False
            for use_case_name, use_case_config in config["use cases"].items():
                if req in use_case_config["network requirements"]:
                    req_config = use_case_config["network requirements"][req]

                    if "datasets" in req_config:
                        for ds_name, ds_config in req_config["datasets"].items():
                            self.dataset_weights[req][ds_name] = ds_config.get("w", 1.0)
                            self.dataset_exists_in_config[req][ds_name] = True
                        found_weights = True

                        for ds in datasets:
                            if ds not in req_config["datasets"]:
                                self.dataset_weights[req][ds] = 0.0
                                self.dataset_exists_in_config[req][ds] = False
                        break

            if not found_weights:
                for ds in datasets:
                    self.dataset_weights[req][ds] = 0.0
                    self.dataset_exists_in_config[req][ds] = False

        self.reset_counter += 1

    @staticmethod
    def _get_requirements_from_config(config):
        """Extract all requirements from config."""
        requirements = set()
        for use_case_config in config["use cases"].values():
            requirements.update(use_case_config["network requirements"].keys())
        return sorted(list(requirements))

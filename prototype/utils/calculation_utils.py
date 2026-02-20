"""Utility functions for IQB calculations."""

import copy
from typing import Dict, Tuple

from app_state import IQBAppState
from iqb import IQB, IQB_CONFIG
from utils.data_utils import get_available_datasets, get_available_requirements


def build_data_for_calculate(state: IQBAppState) -> Dict[str, Dict[str, float]]:
    """Build the data structure expected by calculate_iqb_score from manual entry."""
    datasets = get_available_datasets()
    requirements = get_available_requirements()

    data = {}
    for dataset in datasets:
        data[dataset] = {}
        for req in requirements:
            data[dataset][req] = state.manual_entry[dataset][req]

    return data


def get_config_with_custom_settings(state: IQBAppState) -> Dict:
    """Create a modified config with user-defined thresholds and weights."""
    modified_config = copy.deepcopy(IQB_CONFIG)

    # Update thresholds
    for use_case_name in state.thresholds:
        if use_case_name not in modified_config["use cases"]:
            continue
        for req_name in state.thresholds[use_case_name]:
            if (
                req_name
                not in modified_config["use cases"][use_case_name][
                    "network requirements"
                ]
            ):
                continue
            modified_config["use cases"][use_case_name]["network requirements"][
                req_name
            ]["threshold min"] = state.thresholds[use_case_name][req_name]

    # Update requirement weights
    for use_case_name in state.requirement_weights:
        if use_case_name not in modified_config["use cases"]:
            continue
        for req_name in state.requirement_weights[use_case_name]:
            if (
                req_name
                not in modified_config["use cases"][use_case_name][
                    "network requirements"
                ]
            ):
                continue
            modified_config["use cases"][use_case_name]["network requirements"][
                req_name
            ]["w"] = state.requirement_weights[use_case_name][req_name]

    # Update use case weights
    for use_case_name in state.use_case_weights:
        if use_case_name in modified_config["use cases"]:
            modified_config["use cases"][use_case_name]["w"] = state.use_case_weights[
                use_case_name
            ]

    return modified_config


def calculate_iqb_score_with_custom_settings(
    state: IQBAppState, data: Dict[str, Dict[str, float]], print_details: bool = False
) -> float:
    """Calculate IQB score using current UI-customized thresholds and weights."""
    custom_config = get_config_with_custom_settings(state)
    calculator = IQB(config=custom_config)
    return calculator.calculate_iqb_score(data=data, print_details=print_details)


def calculate_component_importance() -> Dict[str, float]:
    """Calculate the importance of each network component for visualization."""
    importance = {}
    requirements = get_available_requirements()

    for req in requirements:
        importance[req] = 0.0

    use_cases = IQB_CONFIG["use cases"]
    num_use_cases = len(use_cases)

    for use_case_name, use_case_config in use_cases.items():
        req_weights = {}
        for req_name, req_config in use_case_config["network requirements"].items():
            req_weights[req_name] = req_config.get("w", 1.0)

        total_req_weight = sum(req_weights.values())

        if total_req_weight > 0:
            for req_name, req_weight in req_weights.items():
                normalized_weight = (req_weight / total_req_weight) / num_use_cases
                importance[req_name] += normalized_weight

    return importance


def calculate_dataset_importance_per_requirement(
    state: IQBAppState,
) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, bool]]]:
    """Calculate dataset importance within each requirement using session state weights."""
    dataset_importance = {}
    dataset_exists = {}

    datasets = get_available_datasets()
    requirements = get_available_requirements()
    use_cases = IQB_CONFIG["use cases"]
    num_use_cases = len(use_cases)

    for req in requirements:
        dataset_importance[req] = {ds: 0.0 for ds in datasets}
        dataset_exists[req] = {ds: False for ds in datasets}

    for use_case_name, use_case_config in use_cases.items():
        for req_name, req_config in use_case_config["network requirements"].items():
            req_weight = state.requirement_weights.get(use_case_name, {}).get(
                req_name, req_config.get("w", 1.0)
            )

            req_weights = {}
            for r, rc in use_case_config["network requirements"].items():
                req_weights[r] = state.requirement_weights.get(use_case_name, {}).get(
                    r, rc.get("w", 1.0)
                )

            total_req_weight = sum(req_weights.values())

            if total_req_weight > 0:
                req_contribution = (req_weight / total_req_weight) / num_use_cases

                if req_name in state.dataset_weights:
                    dataset_weights = state.dataset_weights[req_name].copy()
                    for ds_name, ds_weight in dataset_weights.items():
                        if req_name in state.dataset_exists_in_config:
                            dataset_exists[req_name][ds_name] = (
                                state.dataset_exists_in_config[req_name].get(
                                    ds_name, False
                                )
                            )
                else:
                    if "datasets" in req_config:
                        dataset_weights = {
                            ds: dc.get("w", 1.0)
                            for ds, dc in req_config["datasets"].items()
                        }
                        for ds_name in req_config["datasets"].keys():
                            dataset_exists[req_name][ds_name] = True
                    else:
                        dataset_weights = {}

                total_ds_weight = sum(dataset_weights.values())

                if total_ds_weight > 0:
                    for ds_name, ds_weight in dataset_weights.items():
                        dataset_importance[req_name][ds_name] += req_contribution * (
                            ds_weight / total_ds_weight
                        )

    return dataset_importance, dataset_exists

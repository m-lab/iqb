"""Session state initialization and reset functions for IQB Streamlit app."""

from app_state import IQBAppState
from iqb import IQB_CONFIG
from utils.data_utils import (
    get_available_datasets,
    get_available_requirements,
    get_default_value_for_requirement,
)


def initialize_manual_entry(state: IQBAppState) -> None:
    """Initialize manual entry state with default test values.

    Args:
        state: Application state object to initialize
    """
    requirements = get_available_requirements()
    datasets = get_available_datasets()

    for dataset in datasets:
        state.manual_entry[dataset] = {}
        for req in requirements:
            state.manual_entry[dataset][req] = get_default_value_for_requirement(req)


def initialize_thresholds(state: IQBAppState) -> None:
    """Initialize thresholds from config.

    Args:
        state: Application state object to initialize
    """
    state.thresholds = {}
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        state.thresholds[use_case_name] = {}
        for req_name, req_config in use_case_config["network requirements"].items():
            if "threshold min" in req_config:
                state.thresholds[use_case_name][req_name] = req_config["threshold min"]


def initialize_requirement_weights(state: IQBAppState) -> None:
    """Initialize requirement weights from config.

    Args:
        state: Application state object to initialize
    """
    state.requirement_weights = {}
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        state.requirement_weights[use_case_name] = {}
        for req_name, req_config in use_case_config["network requirements"].items():
            state.requirement_weights[use_case_name][req_name] = req_config.get(
                "w", 1.0
            )


def initialize_use_case_weights(state: IQBAppState) -> None:
    """Initialize use case weights from config.

    Args:
        state: Application state object to initialize
    """
    state.use_case_weights = {}
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        state.use_case_weights[use_case_name] = use_case_config.get("w", 1.0)


def initialize_dataset_weights(state: IQBAppState) -> None:
    """Initialize dataset weights and track which datasets exist in config.

    Args:
        state: Application state object to initialize
    """
    state.dataset_weights = {}
    state.dataset_exists_in_config = {}

    datasets = get_available_datasets()
    requirements = get_available_requirements()

    for req in requirements:
        state.dataset_weights[req] = {}
        state.dataset_exists_in_config[req] = {}

        found_weights = False
        for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
            if req in use_case_config["network requirements"]:
                req_config = use_case_config["network requirements"][req]
                if "datasets" in req_config:
                    for ds_name, ds_config in req_config["datasets"].items():
                        state.dataset_weights[req][ds_name] = ds_config.get("w", 1.0)
                        state.dataset_exists_in_config[req][ds_name] = True
                    found_weights = True

                    for ds in datasets:
                        if ds not in req_config["datasets"]:
                            state.dataset_weights[req][ds] = 0.0
                            state.dataset_exists_in_config[req][ds] = False
                    break

        if not found_weights:
            for ds in datasets:
                state.dataset_weights[req][ds] = 0.0
                state.dataset_exists_in_config[req][ds] = False


def initialize_app_state() -> IQBAppState:
    """Initialize and return a new IQBAppState with all defaults.

    Returns:
        Fully initialized IQBAppState object
    """
    state = IQBAppState()
    initialize_manual_entry(state)
    initialize_thresholds(state)
    initialize_requirement_weights(state)
    initialize_use_case_weights(state)
    initialize_dataset_weights(state)
    return state


# ============================================================================
# RESET FUNCTIONS (moved from IQBAppState)
# ============================================================================


def reset_manual_entry(state: IQBAppState) -> None:
    """Reset manual entry to default values.

    Args:
        state: Application state object to reset
    """
    datasets = get_available_datasets()
    requirements = get_available_requirements()

    state.manual_entry = {}
    for dataset in datasets:
        state.manual_entry[dataset] = {}
        for req in requirements:
            state.manual_entry[dataset][req] = get_default_value_for_requirement(req)

    state.reset_counter += 1


def reset_thresholds(state: IQBAppState) -> None:
    """Reset thresholds to config defaults.

    Args:
        state: Application state object to reset
    """
    state.thresholds = {}
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        state.thresholds[use_case_name] = {}
        for req_name, req_config in use_case_config["network requirements"].items():
            if "threshold min" in req_config:
                state.thresholds[use_case_name][req_name] = req_config["threshold min"]

    state.reset_counter += 1


def reset_requirement_weights(state: IQBAppState) -> None:
    """Reset requirement weights to config defaults.

    Args:
        state: Application state object to reset
    """
    state.requirement_weights = {}
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        state.requirement_weights[use_case_name] = {}
        for req_name, req_config in use_case_config["network requirements"].items():
            state.requirement_weights[use_case_name][req_name] = req_config.get(
                "w", 1.0
            )

    state.reset_counter += 1


def reset_use_case_weights(state: IQBAppState) -> None:
    """Reset use case weights to config defaults.

    Args:
        state: Application state object to reset
    """
    state.use_case_weights = {}
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        state.use_case_weights[use_case_name] = use_case_config.get("w", 1.0)

    state.reset_counter += 1


def reset_dataset_weights(state: IQBAppState) -> None:
    """Reset dataset weights to config defaults.

    Args:
        state: Application state object to reset
    """
    datasets = get_available_datasets()
    requirements = get_available_requirements()

    state.dataset_weights = {}
    state.dataset_exists_in_config = {}

    for req in requirements:
        state.dataset_weights[req] = {}
        state.dataset_exists_in_config[req] = {}

        found_weights = False
        for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
            if req in use_case_config["network requirements"]:
                req_config = use_case_config["network requirements"][req]

                if "datasets" in req_config:
                    for ds_name, ds_config in req_config["datasets"].items():
                        state.dataset_weights[req][ds_name] = ds_config.get("w", 1.0)
                        state.dataset_exists_in_config[req][ds_name] = True
                    found_weights = True

                    for ds in datasets:
                        if ds not in req_config["datasets"]:
                            state.dataset_weights[req][ds] = 0.0
                            state.dataset_exists_in_config[req][ds] = False
                    break

        if not found_weights:
            for ds in datasets:
                state.dataset_weights[req][ds] = 0.0
                state.dataset_exists_in_config[req][ds] = False

    state.reset_counter += 1

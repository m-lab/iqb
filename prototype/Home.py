"""IQB Streamlit Prototype - Main Entry Point"""

import copy
from enum import Enum
from functools import lru_cache
from typing import Dict, List, Set, Tuple

import plotly.graph_objects as go
import streamlit as st
from app_state import IQBAppState
from iqb import IQB_CONFIG

# ============================================================================
# CONSTANTS
# ============================================================================

# Default measurement values
DEFAULT_DOWNLOAD_SPEED = 15.0
DEFAULT_UPLOAD_SPEED = 20.0
DEFAULT_LATENCY = 75.0
DEFAULT_PACKET_LOSS = 0.007

# Component colors from IQB Paper
COMPONENT_COLORS = {
    "Download": "#64C6CD",
    "Upload": "#78acdb",
    "Latency": "#9b92c6",
    "Packet Loss": "#da93bf",
}

# Dataset colors
DATASET_COLORS = {
    "cloudflare": "#B8E6E8",
    "m-lab": "#A8D5BA",
    "ookla": "#F9D5A7",
}

# Visualization settings
SUNBURST_HEIGHT = 700
COMPONENT_FONT_SIZE = 20
DATASET_FONT_SIZE = 16
CENTER_FONT_SIZE = 22
ROOT_FONT_SIZE = 1
ZERO_WEIGHT_DISPLAY_RATIO = 0.05  # Show zero-weight nodes at 5% of parent size, allows zero-weight nodes to be seen

# Weight ranges
MIN_REQUIREMENT_WEIGHT = 0
MAX_REQUIREMENT_WEIGHT = 5
MIN_USE_CASE_WEIGHT = 0.0
MAX_USE_CASE_WEIGHT = 1.0
MIN_DATASET_WEIGHT = 0.0
MAX_DATASET_WEIGHT = 1.0

# Input ranges
MIN_SPEED = 0.0
MAX_SPEED = 10000.0
SPEED_STEP = 1.0

MIN_LATENCY_MS = 0.0
MAX_LATENCY_MS = 1000.0
LATENCY_STEP = 0.1

MIN_PACKET_LOSS_PCT = 0.0
MAX_PACKET_LOSS_PCT = 100.0
PACKET_LOSS_STEP = 0.001


# ============================================================================
# ENUMS
# ============================================================================


class RequirementType(Enum):
    """Network requirement types"""

    DOWNLOAD = "download"
    UPLOAD = "upload"
    LATENCY = "latency"
    PACKET_LOSS = "packet_loss"


# ============================================================================
# UTILITY FUNCTIONS - Data Extraction
# ============================================================================
@lru_cache(maxsize=1)
def get_available_datasets() -> List[str]:
    """Extract list of datasets from config.

    Returns:
        Sorted list of dataset names
    """
    datasets = set()
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        for req_name, req_config in use_case_config["network requirements"].items():
            if "datasets" in req_config:
                datasets.update(req_config["datasets"].keys())
    return sorted(list(datasets))


@lru_cache(maxsize=1)
def get_available_requirements() -> List[str]:
    """Extract list of network requirements from config.

    Returns:
        Sorted list of requirement names
    """
    requirements = set()
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        requirements.update(use_case_config["network requirements"].keys())
    return sorted(list(requirements))


def identify_requirement_type(req_name: str) -> RequirementType:
    """Identify the type of a network requirement.

    Args:
        req_name: Name of the requirement

    Returns:
        RequirementType enum value
    """
    req_lower = req_name.lower()
    if RequirementType.DOWNLOAD.value in req_lower:
        return RequirementType.DOWNLOAD
    elif RequirementType.UPLOAD.value in req_lower:
        return RequirementType.UPLOAD
    elif RequirementType.LATENCY.value in req_lower:
        return RequirementType.LATENCY
    elif "packet" in req_lower or "loss" in req_lower:
        return RequirementType.PACKET_LOSS
    else:
        return None


def get_ordered_requirements(requirements: List[str]) -> List[str]:
    """Return requirements in display order: Download, Upload, Latency, Packet Loss.

    Args:
        requirements: List of requirement names

    Returns:
        Ordered list of requirements
    """
    # Define explicit order
    order_map = {
        RequirementType.DOWNLOAD: 0,
        RequirementType.UPLOAD: 1,
        RequirementType.LATENCY: 2,
        RequirementType.PACKET_LOSS: 3,
    }

    # Sort by type order
    def get_sort_key(req):
        req_type = identify_requirement_type(req)
        return order_map.get(req_type, 999)

    return sorted(requirements, key=get_sort_key)


def get_requirement_display_name(req_name: str) -> str:
    """Get display name for a requirement.

    Args:
        req_name: Internal requirement name

    Returns:
        Human-readable display name
    """
    req_type = identify_requirement_type(req_name)
    display_map = {
        RequirementType.DOWNLOAD: "Download",
        RequirementType.UPLOAD: "Upload",
        RequirementType.LATENCY: "Latency",
        RequirementType.PACKET_LOSS: "Packet Loss",
    }
    return display_map.get(req_type, req_name.replace("_", " ").title())


def get_requirement_input_config(req_name: str) -> Dict:
    """Get input configuration for a requirement type.

    Args:
        req_name: Name of the requirement

    Returns:
        Dictionary with label, min_value, max_value, step, and format
    """
    req_type = identify_requirement_type(req_name)

    configs = {
        RequirementType.DOWNLOAD: {
            "label": "Download Speed (Mbps)",
            "min_value": MIN_SPEED,
            "max_value": MAX_SPEED,
            "step": SPEED_STEP,
            "format": None,
        },
        RequirementType.UPLOAD: {
            "label": "Upload Speed (Mbps)",
            "min_value": MIN_SPEED,
            "max_value": MAX_SPEED,
            "step": SPEED_STEP,
            "format": None,
        },
        RequirementType.LATENCY: {
            "label": "Latency (ms)",
            "min_value": MIN_LATENCY_MS,
            "max_value": MAX_LATENCY_MS,
            "step": LATENCY_STEP,
            "format": None,
        },
        RequirementType.PACKET_LOSS: {
            "label": "Packet Loss (%)",
            "min_value": MIN_PACKET_LOSS_PCT,
            "max_value": MAX_PACKET_LOSS_PCT,
            "step": PACKET_LOSS_STEP,
            "format": "%.3f",
        },
    }

    return configs.get(
        req_type,
        {
            "label": req_name.replace("_", " ").title(),
            "min_value": MIN_SPEED,
            "max_value": MAX_SPEED,
            "step": SPEED_STEP,
            "format": None,
        },
    )


# ============================================================================
# UTILITY FUNCTIONS - Unit Conversion
# ============================================================================


def convert_packet_loss_for_display(decimal_value: float) -> float:
    """Convert packet loss from decimal (0-1) to percentage (0-100).

    Args:
        decimal_value: Packet loss as decimal

    Returns:
        Packet loss as percentage
    """
    return decimal_value * 100


def convert_packet_loss_from_display(percentage_value: float) -> float:
    """Convert packet loss from percentage (0-100) to decimal (0-1).

    Args:
        percentage_value: Packet loss as percentage

    Returns:
        Packet loss as decimal
    """
    return percentage_value / 100


def get_default_value_for_requirement(req_name: str) -> float:
    """Get default value for a requirement type.

    Args:
        req_name: Name of the requirement

    Returns:
        Default value
    """
    req_type = identify_requirement_type(req_name)
    defaults = {
        RequirementType.DOWNLOAD: DEFAULT_DOWNLOAD_SPEED,
        RequirementType.UPLOAD: DEFAULT_UPLOAD_SPEED,
        RequirementType.LATENCY: DEFAULT_LATENCY,
        RequirementType.PACKET_LOSS: DEFAULT_PACKET_LOSS,
    }
    return defaults.get(req_type, 0.0)


# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================


def initialize_manual_entry(state: IQBAppState) -> None:
    """Initialize manual entry state with default test values.

    Args:
        state: Application state object
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
        state: Application state object
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
        state: Application state object
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
        state: Application state object
    """
    state.use_case_weights = {}
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        state.use_case_weights[use_case_name] = use_case_config.get("w", 1.0)


def initialize_dataset_weights(state: IQBAppState) -> None:
    """Initialize dataset weights and track which datasets exist in config.

    Args:
        state: Application state object
    """
    state.dataset_weights = {}
    state.dataset_exists_in_config = {}

    datasets = get_available_datasets()
    requirements = get_available_requirements()

    for req in requirements:
        state.dataset_weights[req] = {}
        state.dataset_exists_in_config[req] = {}

        # Look through all use cases to find this requirement's dataset weights
        found_weights = False
        for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
            if req in use_case_config["network requirements"]:
                req_config = use_case_config["network requirements"][req]

                if "datasets" in req_config:
                    for ds_name, ds_config in req_config["datasets"].items():
                        state.dataset_weights[req][ds_name] = ds_config.get("w", 1.0)
                        state.dataset_exists_in_config[req][ds_name] = True
                    found_weights = True

                    # Mark any missing datasets as not in config
                    for ds in datasets:
                        if ds not in req_config["datasets"]:
                            state.dataset_weights[req][ds] = 0.0
                            state.dataset_exists_in_config[req][ds] = False
                    break

        # If no weights found in config, mark as not configured
        if not found_weights:
            for ds in datasets:
                state.dataset_weights[req][ds] = 0.0
                state.dataset_exists_in_config[req][ds] = False


def initialize_session_state() -> None:
    """Initialize all session state variables using the IQBAppState dataclass."""
    if "app_state" not in st.session_state:
        # Create the state object
        st.session_state.app_state = IQBAppState()

        # Initialize each component
        initialize_manual_entry(st.session_state.app_state)
        initialize_thresholds(st.session_state.app_state)
        initialize_requirement_weights(st.session_state.app_state)
        initialize_use_case_weights(st.session_state.app_state)
        initialize_dataset_weights(st.session_state.app_state)


# ============================================================================
# CALCULATION FUNCTIONS
# ============================================================================
def build_data_for_calculate(state: IQBAppState) -> Dict[str, Dict[str, float]]:
    """Build the data structure expected by calculate_iqb_score from manual entry.

    Args:
        state: Application state object

    Returns:
        Nested dictionary with structure: {dataset: {requirement: value}}
    """
    datasets = get_available_datasets()
    requirements = get_available_requirements()

    data = {}
    for dataset in datasets:
        data[dataset] = {}
        for req in requirements:
            data[dataset][req] = state.manual_entry[dataset][req]

    return data


def get_config_with_custom_settings(state: IQBAppState) -> Dict:
    """Create a modified config with user-defined thresholds and weights.

    Args:
        state: Application state object

    Returns:
        Modified configuration dictionary
    """
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


def calculate_component_importance() -> Dict[str, float]:
    """Calculate the importance of each network component for visualization.

    Returns:
        Dictionary mapping requirement names to importance values (0-1)
    """
    importance = {}
    requirements = get_available_requirements()

    for req in requirements:
        importance[req] = 0.0

    use_cases = IQB_CONFIG["use cases"]
    num_use_cases = len(use_cases)

    for use_case_name, use_case_config in use_cases.items():
        # Get all requirement weights for this use case
        req_weights = {}
        for req_name, req_config in use_case_config["network requirements"].items():
            req_weights[req_name] = req_config.get("w", 1.0)

        total_req_weight = sum(req_weights.values())

        if total_req_weight > 0:
            for req_name, req_weight in req_weights.items():
                # Equal use case weighting, weighted requirements within each use case
                normalized_weight = (req_weight / total_req_weight) / num_use_cases
                importance[req_name] += normalized_weight

    return importance


def calculate_dataset_importance_per_requirement(
    state: IQBAppState,
) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, bool]]]:
    """Calculate dataset importance within each requirement using session state weights.

    This function calculates how much space each dataset (Cloudflare, M-Lab, Ookla)
    occupies within its parent component ring (Download, Upload, Latency, Packet Loss).

    The function accounts for:
    1. Use case weights (how important is each use case)
    2. Requirement weights (how important is download vs latency within a use case)
    3. Dataset weights (how important is Cloudflare vs M-Lab for a specific requirement)

    Args:
        state: Application state object with custom weights

    Returns:
        Tuple of (dataset_importance, dataset_exists) dictionaries
        - dataset_importance: {requirement: {dataset: importance_value}}
        - dataset_exists: {requirement: {dataset: exists_in_config_bool}}
    """
    dataset_importance = {}
    dataset_exists = {}

    datasets = get_available_datasets()
    requirements = get_available_requirements()
    use_cases = IQB_CONFIG["use cases"]
    num_use_cases = len(use_cases)

    # Initialize
    for req in requirements:
        dataset_importance[req] = {ds: 0.0 for ds in datasets}
        dataset_exists[req] = {ds: False for ds in datasets}

    # Calculate using custom weights from session state
    for use_case_name, use_case_config in use_cases.items():
        for req_name, req_config in use_case_config["network requirements"].items():
            # Get requirement weights (use state if available, otherwise config)
            req_weight = state.requirement_weights.get(use_case_name, {}).get(
                req_name, req_config.get("w", 1.0)
            )

            # Calculate all requirement weights for normalization
            req_weights = {}
            for r, rc in use_case_config["network requirements"].items():
                req_weights[r] = state.requirement_weights.get(use_case_name, {}).get(
                    r, rc.get("w", 1.0)
                )

            total_req_weight = sum(req_weights.values())

            if total_req_weight > 0:
                req_contribution = (req_weight / total_req_weight) / num_use_cases

                # Use dataset weights from session state
                if req_name in state.dataset_weights:
                    dataset_weights = state.dataset_weights[req_name].copy()

                    # Mark datasets as existing based on session state
                    for ds_name, ds_weight in dataset_weights.items():
                        if req_name in state.dataset_exists_in_config:
                            dataset_exists[req_name][ds_name] = (
                                state.dataset_exists_in_config[req_name].get(
                                    ds_name, False
                                )
                            )
                else:
                    # Fallback to config
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


# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================


def prepare_sunburst_data(
    state: IQBAppState,
) -> Tuple[List, List, List, List, List, List, Set]:
    """Prepare data for sunburst visualization.

    Args:
        state: Application state object

    Returns:
        Tuple of (labels, parents, values, colors, ids, hover_text, zero_weight_nodes)
    """
    labels = [""]
    parents = [" "]  # Required to create the whitespace in the middle
    values = [0]
    colors = ["white"]
    ids = ["root"]
    hover_text = [""]
    zero_weight_nodes = set()

    # Get importance metrics
    component_importance = calculate_component_importance()
    dataset_importance, dataset_exists = calculate_dataset_importance_per_requirement(
        state
    )

    # Build requirement to display name mapping
    req_to_display = {}
    for req in get_available_requirements():
        req_to_display[req] = get_requirement_display_name(req)

    datasets = get_available_datasets()

    # Add components and their datasets
    for req, importance in component_importance.items():
        display_name = req_to_display.get(req, req)
        base_color = COMPONENT_COLORS.get(display_name, "#CCCCCC")

        # Add component
        labels.append(display_name)
        parents.append(" ")
        values.append(importance)
        colors.append(base_color)
        ids.append(display_name)
        hover_text.append("")

        # Add datasets for this component
        for dataset in datasets:
            ds_importance = dataset_importance[req].get(dataset, 0.0)

            # If dataset has zero weight, show with small fixed size
            if ds_importance == 0.0:
                display_value = importance * ZERO_WEIGHT_DISPLAY_RATIO
                unique_id = f"{dataset.upper()}-{display_name}"
                zero_weight_nodes.add(unique_id)
            else:
                display_value = ds_importance

            unique_id = f"{dataset}-{display_name}"

            labels.append(dataset.upper())
            parents.append(display_name)
            values.append(display_value)
            colors.append(DATASET_COLORS.get(dataset, "#CCCCCC"))
            ids.append(unique_id)

            # Create hover text
            exists_in_config = dataset_exists.get(req, {}).get(dataset, False)
            if not exists_in_config:
                hover_text.append(f"<b>{dataset.upper()}</b><br>No Data Available")
            else:
                current_weight = state.dataset_weights.get(req, {}).get(dataset, 0.0)
                if current_weight == 0.0:
                    hover_text.append(f"<b>{dataset.upper()}</b><br>Weight: 0.00")
                else:
                    hover_text.append(
                        f"<b>{dataset.upper()}</b><br>Weight: {current_weight:.2f}"
                    )

    return labels, parents, values, colors, ids, hover_text, zero_weight_nodes


def create_sunburst_figure(
    labels: List, parents: List, values: List, colors: List, ids: List, hover_text: List
) -> go.Figure:
    """Create the sunburst plotly figure.

    Args:
        labels: Node labels
        parents: Parent nodes
        values: Node values
        colors: Node colors
        ids: Node IDs
        hover_text: Hover text for nodes

    Returns:
        Plotly Figure object
    """
    # Calculate font sizes
    font_sizes = []
    for label, parent in zip(labels, parents):
        if parent == "":
            font_sizes.append(ROOT_FONT_SIZE)
        elif parent in [" "]:  # Components
            font_sizes.append(COMPONENT_FONT_SIZE)
        else:  # Datasets
            font_sizes.append(DATASET_FONT_SIZE)

    fig = go.Figure(
        go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            marker=dict(colors=colors, line=dict(color="white", width=3)),
            hovertext=hover_text,
            hoverinfo="text",
            textfont=dict(size=font_sizes, color="#3F3E3E", family="Arial"),
        )
    )

    fig.update_layout(
        height=SUNBURST_HEIGHT, margin=dict(t=10, b=10, l=10, r=10), showlegend=False
    )

    return fig


def add_iqb_score_annotation(fig: go.Figure, iqb_score: float) -> None:
    """Add IQB score annotation to the center of the sunburst.

    Args:
        fig: Plotly figure to annotate
        iqb_score: IQB score value
    """
    fig.add_annotation(
        text=f"<b>IQB Score</b><br>{iqb_score:.3f}",
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        font=dict(size=CENTER_FONT_SIZE, color="#333333", family="Arial"),
        showarrow=False,
        align="center",
    )


# ============================================================================
# UI RENDERING FUNCTIONS
# ============================================================================


def render_simple_mode_inputs(
    state: IQBAppState, requirements: List[str], datasets: List[str]
) -> None:
    """Render simple mode input controls.

    Args:
        state: Application state object
        requirements: List of requirement names
        datasets: List of dataset names
    """
    st.caption("Enter your network measurements (applied to all datasets)")

    ordered_requirements = get_ordered_requirements(requirements)
    first_dataset = datasets[0]

    for req in ordered_requirements:
        config = get_requirement_input_config(req)
        req_type = identify_requirement_type(req)

        # Handle packet loss percentage conversion
        if req_type == RequirementType.PACKET_LOSS:
            display_value = convert_packet_loss_for_display(
                state.manual_entry[first_dataset][req]
            )
            new_value = convert_packet_loss_from_display(
                st.number_input(
                    config["label"],
                    min_value=config["min_value"],
                    max_value=config["max_value"],
                    value=display_value,
                    step=config["step"],
                    format=config["format"],
                    key=f"manual_simple_{req}_{state.reset_counter}",
                )
            )
        else:
            new_value = st.number_input(
                config["label"],
                min_value=config["min_value"],
                max_value=config["max_value"],
                value=state.manual_entry[first_dataset][req],
                step=config["step"],
                key=f"manual_simple_{req}_{state.reset_counter}",
            )

        # Apply to all datasets
        for dataset in datasets:
            state.manual_entry[dataset][req] = new_value


def render_advanced_mode_inputs(
    state: IQBAppState, requirements: List[str], datasets: List[str]
) -> None:
    """Render advanced mode input controls with dataset-specific tabs.

    Args:
        state: Application state object
        requirements: List of requirement names
        datasets: List of dataset names
    """
    st.caption("Enter your network measurements per dataset")

    ordered_requirements = get_ordered_requirements(requirements)
    dataset_tabs = st.tabs([ds.upper() for ds in datasets])

    for dataset, tab in zip(datasets, dataset_tabs):
        with tab:
            for req in ordered_requirements:
                config = get_requirement_input_config(req)
                req_type = identify_requirement_type(req)

                # Handle packet loss percentage conversion
                if req_type == RequirementType.PACKET_LOSS:
                    display_value = convert_packet_loss_for_display(
                        state.manual_entry[dataset][req]
                    )
                    new_value = convert_packet_loss_from_display(
                        st.number_input(
                            config["label"],
                            min_value=config["min_value"],
                            max_value=config["max_value"],
                            value=display_value,
                            step=config["step"],
                            format=config["format"],
                            key=f"manual_{dataset}_{req}_{state.reset_counter}",
                        )
                    )
                else:
                    new_value = st.number_input(
                        config["label"],
                        min_value=config["min_value"],
                        max_value=config["max_value"],
                        value=state.manual_entry[dataset][req],
                        step=config["step"],
                        key=f"manual_{dataset}_{req}_{state.reset_counter}",
                    )

                state.manual_entry[dataset][req] = new_value


def render_measurement_inputs(state: IQBAppState) -> None:
    """Render the network measurement input section.

    Args:
        state: Application state object
    """
    st.header("🌐 Network Measurements")

    measurement_mode = st.radio(
        "Input Mode",
        ["Simple", "Advanced"],
        horizontal=True,
        help="Simple: Enter one set of measurements for all datasets. Advanced: Enter different measurements per dataset.",
    )

    requirements = get_available_requirements()
    datasets = get_available_datasets()

    if measurement_mode == "Simple":
        render_simple_mode_inputs(state, requirements, datasets)
    else:
        render_advanced_mode_inputs(state, requirements, datasets)

    # Reset button
    if st.button("🔄 Reset to Default", use_container_width=True):
        state.reset_manual_entry(
            datasets, requirements, get_default_value_for_requirement
        )
        st.rerun()


def render_threshold_editor(state: IQBAppState) -> None:
    """Render the threshold configuration editor.

    Args:
        state: Application state object
    """
    st.subheader("Thresholds by Use Case")
    st.caption("Edit the threshold values for each requirement per use case")

    use_case_tabs = st.tabs(
        [
            use_case.replace("_", " ").title()
            for use_case in IQB_CONFIG["use cases"].keys()
        ]
    )

    needs_rerun = False

    for use_case_name, tab in zip(IQB_CONFIG["use cases"].keys(), use_case_tabs):
        with tab:
            for req_name in state.thresholds.get(use_case_name, {}).keys():
                display_name = get_requirement_display_name(req_name)
                req_type = identify_requirement_type(req_name)

                # Set parameters based on requirement type
                if req_type in [RequirementType.DOWNLOAD, RequirementType.UPLOAD]:
                    label = f"{display_name} (Mbps)"
                    min_val, max_val, step = MIN_SPEED, MAX_SPEED, SPEED_STEP
                    format_str = "%.1f"
                    current_value = state.thresholds[use_case_name][req_name]
                elif req_type == RequirementType.LATENCY:
                    label = f"{display_name} (ms)"
                    min_val, max_val, step = MIN_LATENCY_MS, MAX_LATENCY_MS, 1.0
                    format_str = "%.1f"
                    current_value = state.thresholds[use_case_name][req_name]
                elif req_type == RequirementType.PACKET_LOSS:
                    label = f"{display_name} (%)"
                    min_val, max_val, step = (
                        MIN_PACKET_LOSS_PCT,
                        MAX_PACKET_LOSS_PCT,
                        0.1,
                    )
                    format_str = "%.3f"
                    current_value = convert_packet_loss_for_display(
                        state.thresholds[use_case_name][req_name]
                    )
                else:
                    label = display_name
                    min_val, max_val, step = MIN_SPEED, MAX_SPEED, SPEED_STEP
                    format_str = "%.1f"
                    current_value = state.thresholds[use_case_name][req_name]

                new_value = st.number_input(
                    label,
                    min_value=min_val,
                    max_value=max_val,
                    value=float(current_value),
                    step=step,
                    format=format_str,
                    key=f"threshold_{use_case_name}_{req_name}_{state.reset_counter}",
                )

                # Convert back for packet loss
                if req_type == RequirementType.PACKET_LOSS:
                    new_threshold = convert_packet_loss_from_display(new_value)
                else:
                    new_threshold = new_value

                # Update if changed
                if state.thresholds[use_case_name][req_name] != new_threshold:
                    state.thresholds[use_case_name][req_name] = new_threshold
                    needs_rerun = True

    if needs_rerun:
        st.rerun()

    # Reset button
    if st.button(
        "🔄 Reset Thresholds to Default",
        use_container_width=True,
        key="reset_thresholds_btn",
    ):
        state.reset_thresholds(IQB_CONFIG)
        st.rerun()


def render_requirement_weights_editor(state: IQBAppState) -> None:
    """Render the requirement weights configuration editor.

    Args:
        state: Application state object
    """
    st.subheader("Network Requirement Weights")
    st.caption("Edit the weights for each requirement per use case (0-5 scale)")

    use_case_tabs = st.tabs(
        [
            use_case.replace("_", " ").title()
            for use_case in IQB_CONFIG["use cases"].keys()
        ]
    )

    needs_rerun = False

    for use_case_name, tab in zip(IQB_CONFIG["use cases"].keys(), use_case_tabs):
        with tab:
            for req_name in state.requirement_weights.get(use_case_name, {}).keys():
                display_name = get_requirement_display_name(req_name)
                current_weight = state.requirement_weights[use_case_name][req_name]

                new_weight = st.number_input(
                    f"{display_name} Weight",
                    min_value=MIN_REQUIREMENT_WEIGHT,
                    max_value=MAX_REQUIREMENT_WEIGHT,
                    value=int(current_weight),
                    step=1,
                    key=f"req_weight_{use_case_name}_{req_name}_{state.reset_counter}",
                )

                if state.requirement_weights[use_case_name][req_name] != new_weight:
                    state.requirement_weights[use_case_name][req_name] = new_weight
                    needs_rerun = True

    if needs_rerun:
        st.rerun()

    # Reset button
    if st.button(
        "🔄 Reset Requirement Weights to Default",
        use_container_width=True,
        key="reset_req_weights_btn",
    ):
        state.reset_requirement_weights(IQB_CONFIG)
        st.rerun()


def render_use_case_weights_editor(state: IQBAppState) -> None:
    """Render the use case weights configuration editor.

    Args:
        state: Application state object
    """
    st.subheader("Use Case Weights")
    st.caption("Adjust the importance of each use case (0.0 to 1.0)")

    needs_rerun = False

    for use_case_name in IQB_CONFIG["use cases"].keys():
        display_use_case = use_case_name.replace("_", " ").title()
        current_weight = state.use_case_weights[use_case_name]

        new_weight = st.slider(
            display_use_case,
            min_value=MIN_USE_CASE_WEIGHT,
            max_value=MAX_USE_CASE_WEIGHT,
            value=float(current_weight),
            step=0.1,
            key=f"use_case_weight_{use_case_name}_{state.reset_counter}",
        )

        if state.use_case_weights[use_case_name] != new_weight:
            state.use_case_weights[use_case_name] = new_weight
            needs_rerun = True

    if needs_rerun:
        st.rerun()

    # Reset button
    if st.button(
        "🔄 Reset Use Case Weights to Default",
        use_container_width=True,
        key="reset_uc_weights_btn",
    ):
        state.reset_use_case_weights(IQB_CONFIG)
        st.rerun()


def render_dataset_weights_editor(state: IQBAppState) -> None:
    """Render the dataset weights configuration editor.

    Args:
        state: Application state object
    """
    st.subheader("Dataset Weights")
    st.caption(
        "Adjust the weight of each dataset for each requirement. "
        "Weights should sum to 1.0 for balanced scoring."
    )

    requirements = get_available_requirements()
    datasets = get_available_datasets()

    # Create tabs for each requirement
    req_tabs = st.tabs(
        [
            get_requirement_display_name(req)
            for req in get_ordered_requirements(requirements)
        ]
    )

    needs_rerun = False

    for tab, req in zip(req_tabs, get_ordered_requirements(requirements)):
        with tab:
            # Create columns for each dataset
            ds_cols = st.columns(len(datasets))

            total_weight = 0
            new_weights = {}

            for dataset, col in zip(datasets, ds_cols):
                with col:
                    current_weight = state.dataset_weights[req].get(dataset, 0.0)

                    new_weight = st.number_input(
                        f"{dataset.upper()}",
                        min_value=MIN_DATASET_WEIGHT,
                        max_value=MAX_DATASET_WEIGHT,
                        value=float(current_weight),
                        step=0.05,
                        format="%.2f",
                        key=f"dataset_weight_{req}_{dataset}_{state.reset_counter}",
                    )

                    new_weights[dataset] = new_weight
                    total_weight += new_weight

            # Show total and warning
            st.markdown(f"**Total Weight: {total_weight:.2f}**")
            if abs(total_weight - 1.0) > 0.01:
                st.warning(
                    f"Weights should sum to 1.0 for balanced scoring. Current total: {total_weight:.2f}"
                )

            # Update session state
            for dataset, new_weight in new_weights.items():
                if state.dataset_weights[req].get(dataset, 0.0) != new_weight:
                    state.dataset_weights[req][dataset] = new_weight
                    needs_rerun = True

    if needs_rerun:
        st.rerun()

    # Reset button
    if st.button(
        "🔄 Reset Dataset Weights to Default",
        use_container_width=True,
        key="reset_dataset_weights_btn",
    ):
        state.reset_dataset_weights(IQB_CONFIG, datasets)
        st.rerun()


def render_calculation_details(state: IQBAppState) -> None:
    """Render the calculation details expander.

    Args:
        state: Application state object
    """
    with st.expander("🔍 View Calculation Details"):
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Data Sent to Calculate Function")
            try:
                st.json(build_data_for_calculate(state))
            except Exception as e:
                st.error(f"Error building data: {str(e)}")

        with col2:
            st.subheader("Current Configuration")
            try:
                config_display = {}

                for use_case_name in IQB_CONFIG["use cases"].keys():
                    display_use_case = use_case_name.replace("_", " ").title()
                    config_display[display_use_case] = {
                        "Use Case Weight": state.use_case_weights.get(
                            use_case_name, 1.0
                        ),
                        "Thresholds": {},
                        "Requirement Weights": {},
                    }

                    # Add thresholds
                    if use_case_name in state.thresholds:
                        for req_name, threshold_value in state.thresholds[
                            use_case_name
                        ].items():
                            display_req = get_requirement_display_name(req_name)
                            config_display[display_use_case]["Thresholds"][
                                display_req
                            ] = threshold_value

                    # Add requirement weights
                    if use_case_name in state.requirement_weights:
                        for req_name, weight_value in state.requirement_weights[
                            use_case_name
                        ].items():
                            display_req = get_requirement_display_name(req_name)
                            config_display[display_use_case]["Requirement Weights"][
                                display_req
                            ] = weight_value

                # Add dataset weights
                config_display["Dataset Weights"] = {}
                for req_name, datasets_dict in state.dataset_weights.items():
                    display_req = get_requirement_display_name(req_name)
                    config_display["Dataset Weights"][display_req] = datasets_dict

                st.json(config_display)
            except Exception as e:
                st.error(f"Error displaying configuration: {str(e)}")


# ============================================================================
# MAIN APPLICATION
# ============================================================================


def main():
    """Main application entry point."""
    # Configure page
    st.set_page_config(page_title="IQB Prototype", page_icon="📊", layout="wide")

    # Initialize session state using dataclass
    initialize_session_state()

    # Get state reference for cleaner code
    state = st.session_state.app_state

    # Header
    st.title("Internet Quality Barometer (IQB)")
    st.write("Phase 1 Prototype - Streamlit Dashboard")

    st.markdown("""
    ### Welcome to the IQB Prototype

    This dashboard implements the Internet Quality Barometer framework, which assesses
    Internet quality beyond simple "speed" measurements by considering multiple use cases
    and their specific network requirements.

    **Current status**: Under active development
    """)

    # Create two-column layout
    left_col, right_col = st.columns([1, 3])

    # Left column - Manual measurement input
    with left_col:
        render_measurement_inputs(state)

    # Right column - Sunburst visualization
    with right_col:
        try:
            # Calculate IQB score
            data_for_calculation = build_data_for_calculate(state)
            iqb_score = state.iqb.calculate_iqb_score(
                data=data_for_calculation, print_details=False
            )

            # Prepare and create sunburst
            labels, parents, values, colors, ids, hover_text, zero_weight_nodes = (
                prepare_sunburst_data(state)
            )
            fig = create_sunburst_figure(
                labels, parents, values, colors, ids, hover_text
            )
            add_iqb_score_annotation(fig, iqb_score)

            st.plotly_chart(
                fig,
                use_container_width=True,
                config={"staticPlot": True},
            )

        except KeyError as e:
            st.error(f"Configuration error - missing required key: {str(e)}")
        except ValueError as e:
            st.error(f"Invalid configuration value: {str(e)}")
        except Exception as e:
            st.error(f"Error calculating IQB: {str(e)}")
            st.exception(e)

    # Show calculation details
    render_calculation_details(state)

    # Show config editor
    with st.expander("🔧 Modify Configurations"):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            render_threshold_editor(state)

        with col2:
            render_requirement_weights_editor(state)

        with col3:
            render_use_case_weights_editor(state)

        with col4:
            render_dataset_weights_editor(state)


if __name__ == "__main__":
    main()

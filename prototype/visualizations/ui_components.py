"""UI components for the IQB Streamlit app."""

from typing import List

import streamlit as st
from app_state import IQBAppState
from iqb import IQB_CONFIG
from session_state import (
    reset_dataset_weights,
    reset_manual_entry,
    reset_requirement_weights,
    reset_thresholds,
    reset_use_case_weights,
)
from utils.calculation_utils import build_data_for_calculate
from utils.constants import (
    MAX_DATASET_WEIGHT,
    MAX_LATENCY_MS,
    MAX_PACKET_LOSS_PCT,
    MAX_REQUIREMENT_WEIGHT,
    MAX_SPEED,
    MAX_USE_CASE_WEIGHT,
    MIN_DATASET_WEIGHT,
    MIN_LATENCY_MS,
    MIN_PACKET_LOSS_PCT,
    MIN_REQUIREMENT_WEIGHT,
    MIN_SPEED,
    MIN_USE_CASE_WEIGHT,
    SPEED_STEP,
    RequirementType,
)
from utils.data_utils import (
    convert_packet_loss_for_display,
    convert_packet_loss_from_display,
    get_available_datasets,
    get_available_requirements,
    get_ordered_requirements,
    get_requirement_display_name,
    get_requirement_input_config,
    identify_requirement_type,
)

from .sunburst_figure import add_iqb_score_annotation, create_sunburst_figure

# ============================================================================
# INPUT COMPONENTS
# ============================================================================


def render_measurement_inputs(state: IQBAppState) -> None:
    """Render the network measurement input section."""
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

    if st.button("🔄 Reset to Default", width="stretch"):
        reset_manual_entry(state)
        st.rerun()


def render_simple_mode_inputs(
    state: IQBAppState, requirements: List[str], datasets: List[str]
) -> None:
    """Render simple mode input controls."""
    st.caption("Enter your network measurements (applied to all datasets)")

    ordered_requirements = get_ordered_requirements(requirements)
    first_dataset = datasets[0]

    for req in ordered_requirements:
        config = get_requirement_input_config(req)
        req_type = identify_requirement_type(req)

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

        for dataset in datasets:
            state.manual_entry[dataset][req] = new_value


def render_advanced_mode_inputs(
    state: IQBAppState, requirements: List[str], datasets: List[str]
) -> None:
    """Render advanced mode input controls with dataset-specific tabs."""
    st.caption("Enter your network measurements per dataset")

    ordered_requirements = get_ordered_requirements(requirements)
    dataset_tabs = st.tabs([ds.upper() for ds in datasets])

    for dataset, tab in zip(datasets, dataset_tabs):
        with tab:
            for req in ordered_requirements:
                config = get_requirement_input_config(req)
                req_type = identify_requirement_type(req)

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


# ============================================================================
# EDITOR COMPONENTS
# ============================================================================


def render_threshold_editor(state: IQBAppState) -> None:
    """Render the threshold configuration editor."""
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

                if req_type == RequirementType.PACKET_LOSS:
                    new_threshold = convert_packet_loss_from_display(new_value)
                else:
                    new_threshold = new_value

                if state.thresholds[use_case_name][req_name] != new_threshold:
                    state.thresholds[use_case_name][req_name] = new_threshold
                    needs_rerun = True

    if needs_rerun:
        st.rerun()

    if st.button(
        "🔄 Reset Thresholds to Default",
        width="stretch",
        key="reset_thresholds_btn",
    ):
        reset_thresholds(state)
        st.rerun()


def render_requirement_weights_editor(state: IQBAppState) -> None:
    """Render the requirement weights configuration editor."""
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

    if st.button(
        "🔄 Reset Requirement Weights to Default",
        width="stretch",
        key="reset_req_weights_btn",
    ):
        reset_requirement_weights(state)
        st.rerun()


def render_use_case_weights_editor(state: IQBAppState) -> None:
    """Render the use case weights configuration editor."""
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

    if st.button(
        "🔄 Reset Use Case Weights to Default",
        width="stretch",
        key="reset_uc_weights_btn",
    ):
        reset_use_case_weights(state)
        st.rerun()


def render_dataset_weights_editor(state: IQBAppState) -> None:
    """Render the dataset weights configuration editor."""
    st.subheader("Dataset Weights")
    st.caption(
        "Adjust the weight of each dataset for each requirement. "
        "Weights should sum to 1.0 for balanced scoring."
    )

    requirements = get_available_requirements()
    datasets = get_available_datasets()

    req_tabs = st.tabs(
        [
            get_requirement_display_name(req)
            for req in get_ordered_requirements(requirements)
        ]
    )

    needs_rerun = False

    for tab, req in zip(req_tabs, get_ordered_requirements(requirements)):
        with tab:
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

            st.markdown(f"**Total Weight: {total_weight:.2f}**")
            if abs(total_weight - 1.0) > 0.01:
                st.warning(
                    f"Weights should sum to 1.0 for balanced scoring. Current total: {total_weight:.2f}"
                )

            for dataset, new_weight in new_weights.items():
                if state.dataset_weights[req].get(dataset, 0.0) != new_weight:
                    state.dataset_weights[req][dataset] = new_weight
                    needs_rerun = True

    if needs_rerun:
        st.rerun()

    if st.button(
        "🔄 Reset Dataset Weights to Default",
        width="stretch",
        key="reset_dataset_weights_btn",
    ):
        reset_dataset_weights(state)
        st.rerun()


# ============================================================================
# LAYOUT COMPONENTS
# ============================================================================


def render_calculation_details(state: IQBAppState) -> None:
    """Render the calculation details expander."""
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

                    if use_case_name in state.thresholds:
                        for req_name, threshold_value in state.thresholds[
                            use_case_name
                        ].items():
                            display_req = get_requirement_display_name(req_name)
                            config_display[display_use_case]["Thresholds"][
                                display_req
                            ] = threshold_value

                    if use_case_name in state.requirement_weights:
                        for req_name, weight_value in state.requirement_weights[
                            use_case_name
                        ].items():
                            display_req = get_requirement_display_name(req_name)
                            config_display[display_use_case]["Requirement Weights"][
                                display_req
                            ] = weight_value

                config_display["Dataset Weights"] = {}
                for req_name, datasets_dict in state.dataset_weights.items():
                    display_req = get_requirement_display_name(req_name)
                    config_display["Dataset Weights"][display_req] = datasets_dict

                st.json(config_display)
            except Exception as e:
                st.error(f"Error displaying configuration: {str(e)}")


def render_config_editor(state: IQBAppState) -> None:
    """Render the configuration editor expander."""
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


def render_sunburst(
    data, title: str, iqb_score: float, hierarchy_levels: int = 2, height: int = 300
):
    """Render a sunburst chart from SunburstData."""
    fig = create_sunburst_figure(
        data.labels,
        data.parents,
        data.values,
        data.colors,
        data.ids,
        data.hover_text,
        title=title,
        hierarchy_levels=hierarchy_levels,
        height=height,
    )
    add_iqb_score_annotation(fig, iqb_score)
    st.plotly_chart(fig, width="stretch", config={"staticPlot": True})

"""IQB Streamlit Prototype - Main Entry Point"""

import plotly.graph_objects as go
import streamlit as st
from iqb import IQB, IQB_CONFIG

st.set_page_config(page_title="IQB Prototype", page_icon="📊", layout="wide")

st.title("Internet Quality Barometer (IQB)")
st.write("Phase 1 Prototype - Streamlit Dashboard")

st.markdown("""
### Welcome to the IQB Prototype

This dashboard implements the Internet Quality Barometer framework, which assesses
Internet quality beyond simple "speed" measurements by considering multiple use cases
and their specific network requirements.

**Current status**: Under active development
""")


def get_available_datasets():
    """Extract list of datasets from config"""
    datasets = set()
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        for req_name, req_config in use_case_config["network requirements"].items():
            if "datasets" in req_config:
                datasets.update(req_config["datasets"].keys())
    return sorted(list(datasets))


def get_available_requirements():
    """Extract list of network requirements from config"""
    requirements = set()
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        requirements.update(use_case_config["network requirements"].keys())
    return sorted(list(requirements))


# Initialize session state for manual entry with test case values
if "manual_entry" not in st.session_state:
    requirements = get_available_requirements()

    st.session_state.manual_entry = {}
    for req in requirements:
        if "download" in req.lower():
            st.session_state.manual_entry[req] = 15.0
        elif "upload" in req.lower():
            st.session_state.manual_entry[req] = 20.0
        elif "latency" in req.lower():
            st.session_state.manual_entry[req] = 75.0
        elif "packet" in req.lower() or "loss" in req.lower():
            st.session_state.manual_entry[req] = 0.007
        else:
            st.session_state.manual_entry[req] = 0.0

# Initialize reset counter
if "reset_counter" not in st.session_state:
    st.session_state.reset_counter = 0

# Initialize IQB instance
if "iqb" not in st.session_state:
    st.session_state.iqb = IQB()


def build_data_for_calculate():
    """
    Build the data structure expected by calculate_iqb_score from manual entry.
    Takes the manual entry values and applies them to all datasets.
    """
    datasets = get_available_datasets()
    requirements = get_available_requirements()

    data = {}
    for dataset in datasets:
        data[dataset] = {}
        for req in requirements:
            # Use the manually entered value for all datasets
            data[dataset][req] = st.session_state.manual_entry[req]

    return data


def calculate_component_importance():
    """Calculate the importance of each network component for visualization"""
    importance = {}

    # Initialize importance for each requirement
    requirements = get_available_requirements()
    for req in requirements:
        importance[req] = 0.0

    # Calculate based on config weights
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


def calculate_dataset_importance_per_requirement():
    """Calculate dataset importance within each requirement"""
    dataset_importance = {}
    dataset_exists = {}  # Track which datasets are configured

    datasets = get_available_datasets()
    requirements = get_available_requirements()
    use_cases = IQB_CONFIG["use cases"]
    num_use_cases = len(use_cases)

    # Initialize
    for req in requirements:
        dataset_importance[req] = {ds: 0.0 for ds in datasets}
        dataset_exists[req] = {
            ds: False for ds in datasets
        }  # Track which datasets are configured

    # Calculate
    for use_case_name, use_case_config in use_cases.items():
        for req_name, req_config in use_case_config["network requirements"].items():
            req_weights = {
                r: rc.get("w", 1.0)
                for r, rc in use_case_config["network requirements"].items()
            }
            total_req_weight = sum(req_weights.values())

            if total_req_weight > 0:
                req_contribution = (
                    req_config.get("w", 1.0) / total_req_weight
                ) / num_use_cases

                if "datasets" in req_config:
                    dataset_weights = {
                        ds: dc.get("w", 1.0)
                        for ds, dc in req_config["datasets"].items()
                    }
                    total_ds_weight = sum(dataset_weights.values())

                    # Mark datasets as existing in config
                    for ds_name in req_config["datasets"].keys():
                        dataset_exists[req_name][ds_name] = True

                    if total_ds_weight > 0:
                        for ds_name, ds_weight in dataset_weights.items():
                            dataset_importance[req_name][ds_name] += (
                                req_contribution * (ds_weight / total_ds_weight)
                            )

    return dataset_importance, dataset_exists


# Create two-column layout
left_col, right_col = st.columns([1, 3])

# Left column - Manual measurement input
with left_col:
    st.header("🌐 Network Measurements")
    st.caption("Enter your network measurements")

    # Define the order explicitly
    requirements = get_available_requirements()

    # Create ordered list of requirements: Download, Upload, Latency, Packet Loss
    ordered_requirements = []
    for req in requirements:
        if "download" in req.lower():
            ordered_requirements.insert(0, req)
        elif "upload" in req.lower():
            ordered_requirements.insert(1 if len(ordered_requirements) > 0 else 0, req)
        elif "latency" in req.lower():
            ordered_requirements.insert(
                2 if len(ordered_requirements) > 1 else len(ordered_requirements), req
            )
        elif "packet" in req.lower() or "loss" in req.lower():
            ordered_requirements.append(req)
        else:
            ordered_requirements.append(req)  # Any others at the end

    for req in ordered_requirements:
        if "download" in req.lower():
            label = "Download Speed (Mbps)"
            min_val, max_val, step = 0.0, 10000.0, 1.0
            format_str = None
        elif "upload" in req.lower():
            label = "Upload Speed (Mbps)"
            min_val, max_val, step = 0.0, 10000.0, 1.0
            format_str = None
        elif "latency" in req.lower():
            label = "Latency (ms)"
            min_val, max_val, step = 0.0, 1000.0, 0.1
            format_str = None
        elif "packet" in req.lower() or "loss" in req.lower():
            label = "Packet Loss (%)"
            min_val, max_val, step = 0.0, 100.0, 0.001
            format_str = "%.4f"
        else:
            label = req.replace("_", " ").title()
            min_val, max_val, step = 0.0, 10000.0, 1.0
            format_str = None

        # Handle packet loss percentage conversion
        if "packet" in req.lower() or "loss" in req.lower():
            display_value = st.session_state.manual_entry[req] * 100
            new_value = (
                st.number_input(
                    label,
                    min_value=min_val,
                    max_value=max_val,
                    value=display_value,
                    step=step,
                    format=format_str,
                    key=f"manual_{req}_{st.session_state.reset_counter}",
                )
                / 100
            )
        else:
            new_value = st.number_input(
                label,
                min_value=min_val,
                max_value=max_val,
                value=st.session_state.manual_entry[req],
                step=step,
                key=f"manual_{req}_{st.session_state.reset_counter}",
            )

        st.session_state.manual_entry[req] = new_value

    # Reset button
    if st.button("🔄 Reset to Default", use_container_width=True):
        for req in requirements:
            if "download" in req.lower():
                st.session_state.manual_entry[req] = 15.0
            elif "upload" in req.lower():
                st.session_state.manual_entry[req] = 20.0
            elif "latency" in req.lower():
                st.session_state.manual_entry[req] = 75.0
            elif "packet" in req.lower() or "loss" in req.lower():
                st.session_state.manual_entry[req] = 0.007
        st.session_state.reset_counter += 1
        st.rerun()

# Right column - Sunburst visualization
with right_col:
    try:
        # Build data structure for calculate function
        data_for_calculation = build_data_for_calculate()

        # Calculate IQB score
        iqb_score = st.session_state.iqb.calculate_iqb_score(
            data=data_for_calculation, print_details=False
        )

        # Get importance metrics for sunburst visualization
        component_importance = calculate_component_importance()
        dataset_importance, dataset_exists = (
            calculate_dataset_importance_per_requirement()
        )

        # Build sunburst data
        labels = [""]
        parents = [" "]
        values = [0]
        colors = ["white"]
        ids = ["root"]

        # Component colors from IQB Paper
        component_colors_map = {
            "Download": "#64C6CD",
            "Upload": "#78acdb",
            "Latency": "#9b92c6",
            "Packet Loss": "#da93bf",
        }

        dataset_colors = {
            "cloudflare": "#B8E6E8",
            "m-lab": "#A8D5BA",
            "ookla": "#F9D5A7",
        }

        # Map requirements to display names
        req_to_display = {}
        requirements_sorted = sorted(get_available_requirements())
        for req in requirements_sorted:
            if "download" in req.lower():
                req_to_display[req] = "Download"
            elif "upload" in req.lower():
                req_to_display[req] = "Upload"
            elif "latency" in req.lower():
                req_to_display[req] = "Latency"
            elif "packet" in req.lower() or "loss" in req.lower():
                req_to_display[req] = "Packet Loss"

        datasets = get_available_datasets()
        zero_weight_nodes = set()

        # Add components and their datasets
        for req, importance in component_importance.items():
            display_name = req_to_display.get(req, req)
            base_color = component_colors_map.get(display_name, "#CCCCCC")

            # Add component
            labels.append(display_name)
            parents.append(" ")
            values.append(importance)
            colors.append(base_color)
            ids.append(display_name)

            for ds_idx, dataset in enumerate(datasets):
                ds_importance = dataset_importance[req].get(dataset, 0.0)

                # If dataset has zero weight, show it with small fixed size so it appears on the diagram
                if ds_importance == 0.0:
                    display_value = importance * 0.05
                    zero_weight_nodes.add(f"{dataset.upper()}-{display_name}")
                else:
                    display_value = ds_importance

                unique_id = f"{dataset}-{display_name}"

                labels.append(dataset.upper())
                parents.append(display_name)
                values.append(display_value)

                # Use dataset-specific color if available, otherwise default gray
                colors.append(dataset_colors.get(dataset, "#CCCCCC"))

                ids.append(unique_id)

        # Create custom hover text
        hover_text = []
        for i, (id_val, label, parent, value) in enumerate(
            zip(ids, labels, parents, values)
        ):
            if parent in ["Download", "Upload", "Latency", "Packet Loss"]:
                req_name = None
                for req, display in req_to_display.items():
                    if display == parent:
                        req_name = req
                        break

                dataset_name = label.lower()

                # Get the actual weight from config
                raw_weight = None
                if req_name:
                    # Look through config to find this requirement's dataset weight
                    for use_case_name, use_case_config in IQB_CONFIG[
                        "use cases"
                    ].items():
                        if req_name in use_case_config["network requirements"]:
                            req_config = use_case_config["network requirements"][
                                req_name
                            ]
                            if (
                                "datasets" in req_config
                                and dataset_name in req_config["datasets"]
                            ):
                                raw_weight = req_config["datasets"][dataset_name].get(
                                    "w", 1.0
                                )
                                break

                # Show "No Data Available" if not in config OR if weight is 0
                if raw_weight is None or raw_weight == 0.0:
                    hover_text.append(f"<b>{label}</b><br>No Data Available")
                else:
                    hover_text.append(f"<b>{label}</b><br>Weight: {raw_weight:.2f}")
            else:
                hover_text.append("")

        # Create sunburst
        font_sizes = []

        for label, parent in zip(labels, parents):
            if parent == "":
                font_sizes.append(1)
            elif parent in [" "]:  # Components (Download, Upload, etc.)
                font_sizes.append(18)  # Larger font for components
            else:  # Datasets
                font_sizes.append(16)  # Smaller font for datasets

        fig = go.Figure(
            go.Sunburst(
                ids=ids,
                labels=labels,
                parents=parents,
                values=values,
                marker=dict(colors=colors, line=dict(color="white", width=3)),
                hovertext=hover_text,
                hoverinfo="text",
                textfont=dict(
                    size=font_sizes,  # Use the list of sizes
                    color="#333333",
                    family="Arial",
                ),
            )
        )

        # Add IQB score in center
        fig.add_annotation(
            text=f"<b>IQB Score</b><br>{iqb_score:.3f}",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            font=dict(size=22, color="#333333", family="Arial"),
            showarrow=False,
            align="center",
        )
        fig.update_layout(
            height=700, margin=dict(t=10, b=10, l=10, r=10), showlegend=False
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error calculating IQB: {e}")
        st.exception(e)

# Show calculation details
with st.expander("🔍 View Calculation Details"):
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Manual Entry")
        st.json(st.session_state.manual_entry)
    with col2:
        st.subheader("Data Sent to Calculate Function")
        try:
            st.json(build_data_for_calculate())
        except Exception as e:
            st.error(f"Error building data: {e}")

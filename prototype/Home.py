"""IQB Streamlit Prototype - Main Entry Point"""

import streamlit as st
from session_state import initialize_app_state
from utils.calculation_utils import (
    build_data_for_calculate,
    calculate_iqb_score_with_custom_settings,
)
from visualizations.sunburst_data import (
    prepare_complete_hierarchy_sunburst_data,
    prepare_requirements_sunburst_data,
    prepare_use_cases_sunburst_data,
)
from visualizations.sunburst_figure import (
    add_iqb_score_annotation,
    create_sunburst_figure,
)
from visualizations.ui_components import (
    render_calculation_details,
    render_config_editor,
    render_measurement_inputs,
)


def initialize_session_state() -> None:
    """Initialize all session state variables using the IQBAppState dataclass."""
    if "app_state" not in st.session_state:
        st.session_state.app_state = initialize_app_state()


def render_sunburst(data, title: str, iqb_score: float, hierarchy_levels: int = 2):
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
    )
    add_iqb_score_annotation(fig, iqb_score)
    st.plotly_chart(fig, use_container_width=True, config={"staticPlot": True})


def main():
    """Main application entry point."""
    st.set_page_config(page_title="IQB Prototype", page_icon="📊", layout="wide")

    initialize_session_state()
    state = st.session_state.app_state

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
    # Right column - Sunburst diagrams
    with right_col:
        try:
            data_for_calculation = build_data_for_calculate(state)
            iqb_score = calculate_iqb_score_with_custom_settings(
                state, data=data_for_calculation, print_details=False
            )

            tab1, tab2, tab3 = st.tabs(["Requirements", "Use Cases", "Full Hierarchy"])

            with tab1:
                render_sunburst(
                    prepare_requirements_sunburst_data(state),
                    title="Requirements → Datasets",
                    iqb_score=iqb_score,
                )

            with tab2:
                render_sunburst(
                    prepare_use_cases_sunburst_data(state),
                    title="Use Cases → Datasets",
                    iqb_score=iqb_score,
                )

            with tab3:
                render_sunburst(
                    prepare_complete_hierarchy_sunburst_data(state),
                    title="Use Cases → Requirements → Datasets",
                    iqb_score=iqb_score,
                    hierarchy_levels=3,
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
    render_config_editor(state)


if __name__ == "__main__":
    main()

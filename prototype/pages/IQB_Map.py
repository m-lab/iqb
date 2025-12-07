"""
IQB Interactive Map Page

Displays world map with countries that have data, allows clicking for historical trends.
"""

import json
import re
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import pycountry
import streamlit as st
from plotly.subplots import make_subplots
from session_state import initialize_app_state
from visualizations.sunburst_data import (
    prepare_complete_hierarchy_sunburst_data,
    prepare_requirements_sunburst_data,
    prepare_use_cases_sunburst_data,
)
from visualizations.ui_components import render_sunburst

st.set_page_config(page_title="IQB Map", layout="wide")

st.title("Global Internet Quality Barometer Map")

SCRIPT_DIR = Path(__file__).parent
GEOJSON_DIR = SCRIPT_DIR / "natural_earth" / "geojson_countries"
CACHE_DIR = SCRIPT_DIR.parent / "cache" / "v0"


if "app_state" not in st.session_state:
    st.session_state.app_state = initialize_app_state()
state = st.session_state.app_state


def update_state_from_cache(state, metrics: dict, percentile: str):
    """Update state.manual_entry with cache metrics."""
    state.manual_entry["m-lab"]["download_throughput_mbps"] = metrics[
        "download_throughput_mbps"
    ][percentile]
    state.manual_entry["m-lab"]["upload_throughput_mbps"] = metrics[
        "upload_throughput_mbps"
    ][percentile]
    state.manual_entry["m-lab"]["latency_ms"] = metrics["latency_ms"][percentile]
    state.manual_entry["m-lab"]["packet_loss"] = metrics["packet_loss"][percentile]


def get_country_info(alpha_2: str) -> dict | None:
    """Get country info from pycountry using 2-letter code."""
    try:
        country = pycountry.countries.get(alpha_2=alpha_2.upper())
        if country:
            return {
                "alpha_2": country.alpha_2,
                "alpha_3": country.alpha_3,
                "name": country.name,
            }
    except (KeyError, LookupError):
        pass
    return None


@st.cache_data
def scan_available_data() -> dict:
    """
    Scan cache directory to find all available country/year/month combinations.
    Returns dict with country codes, date ranges, and file mappings.
    """
    if not CACHE_DIR.exists():
        st.error(f"Cache directory not found: {CACHE_DIR}")
        return {"countries": {}, "min_year": 2024, "max_year": 2024}

    pattern = re.compile(r"^([a-z]{2})_(\d{4})_(\d{1,2})\.json$", re.IGNORECASE)

    countries = {}
    all_years = set()

    for file_path in CACHE_DIR.glob("*.json"):
        match = pattern.match(file_path.name)
        if not match:
            continue

        code, year, month = match.groups()
        code = code.lower()
        year = int(year)
        month = int(month)

        all_years.add(year)

        if code not in countries:
            country_info = get_country_info(code)
            if not country_info:
                continue
            countries[code] = {
                "info": country_info,
                "files": {},
            }

        date_key = f"{year}_{month:02d}"
        countries[code]["files"][date_key] = file_path

    return {
        "countries": countries,
        "min_year": min(all_years) if all_years else 2024,
        "max_year": max(all_years) if all_years else 2024,
    }


@st.cache_data
def load_country_geojson(iso_a3: str):
    """Load GeoJSON for a specific country."""
    geojson_file = GEOJSON_DIR / f"{iso_a3}.geojson"

    if not geojson_file.exists():
        return None

    try:
        with open(geojson_file, "r") as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Error loading {geojson_file}: {e}")
        return None


@st.cache_data
def load_metric_data(file_path: str):
    """Load metric data from JSON file."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            return {
                "metrics": data.get("metrics", data),
                "sample_counts": data.get("sample_counts", {}),
            }
    except Exception:
        return None


def get_percentile_value(metrics, metric_name, percentile="p50"):
    """Extract a specific percentile value from metrics."""
    if not metrics or metric_name not in metrics:
        return None

    metric_data = metrics[metric_name]
    if isinstance(metric_data, dict):
        return metric_data.get(percentile)
    return metric_data


def calculate_iqb_score_from_metrics(
    metrics: dict, percentile: str = "p95"
) -> float | None:
    """Calculate actual IQB score from cache metrics."""
    if not metrics:
        return None

    try:
        state = initialize_app_state()

        # Populate state with cache data
        state.manual_entry["m-lab"]["download_throughput_mbps"] = metrics[
            "download_throughput_mbps"
        ][percentile]
        state.manual_entry["m-lab"]["upload_throughput_mbps"] = metrics[
            "upload_throughput_mbps"
        ][percentile]
        state.manual_entry["m-lab"]["latency_ms"] = metrics["latency_ms"][percentile]
        state.manual_entry["m-lab"]["packet_loss"] = metrics["packet_loss"][percentile]

        iqb_data = build_iqb_data_from_cache(metrics, percentile)
        return state.iqb.calculate_iqb_score(data=iqb_data, print_details=False)
    except Exception:
        return None


@st.cache_data
def load_country_data_for_date(year: int, month: int, percentile: str = "p95"):
    """Load data for all countries for a specific date."""
    available = scan_available_data()
    countries = available["countries"]

    date_key = f"{year}_{month:02d}"
    country_data = {}

    for code, country_info in countries.items():
        if date_key not in country_info["files"]:
            continue

        file_path = country_info["files"][date_key]
        data = load_metric_data(str(file_path))

        if data and data.get("metrics"):
            metrics = data["metrics"]
            sample_counts = data.get("sample_counts", {})
            iso_a3 = country_info["info"]["alpha_3"]
            country_data[iso_a3] = {
                "code": code,
                "iso_a3": iso_a3,
                "name": country_info["info"]["name"],
                "score": calculate_iqb_score_from_metrics(metrics, percentile),
                "download": get_percentile_value(
                    metrics, "download_throughput_mbps", percentile
                ),
                "upload": get_percentile_value(
                    metrics, "upload_throughput_mbps", percentile
                ),
                "latency": get_percentile_value(metrics, "latency_ms", percentile),
                "packet_loss": get_percentile_value(metrics, "packet_loss", percentile),
                "metrics": metrics,
                "sample_counts": sample_counts,
            }

    return country_data


def load_historical_data(country_code: str, percentile: str = "p95"):
    """Load historical data for a country across all available months."""
    available = scan_available_data()
    countries = available["countries"]

    if country_code not in countries:
        return pd.DataFrame()

    country_info = countries[country_code]
    data_points = []

    for date_key, file_path in sorted(country_info["files"].items()):
        year, month = date_key.split("_")
        year = int(year)
        month = int(month)

        data = load_metric_data(str(file_path))

        if data and data.get("metrics"):  # Check data exists and has metrics
            metrics = data["metrics"]  # Extract metrics from the dict
            data_points.append(
                {
                    "year": year,
                    "month": month,
                    "date": f"{year}-{month:02d}",
                    "download": get_percentile_value(
                        metrics, "download_throughput_mbps", percentile
                    ),
                    "upload": get_percentile_value(
                        metrics, "upload_throughput_mbps", percentile
                    ),
                    "latency": get_percentile_value(metrics, "latency_ms", percentile),
                    "packet_loss": get_percentile_value(
                        metrics, "packet_loss", percentile
                    ),
                    "iqb_score": calculate_iqb_score_from_metrics(metrics, percentile),
                }
            )

    return pd.DataFrame(data_points) if data_points else pd.DataFrame()


def create_world_map(country_data: dict, selected_country: str = None):
    """Create a world map highlighting countries with data."""

    if not country_data:
        st.warning("No country data available for selected date")
        return None

    iso_codes = list(country_data.keys())

    # Create hover text
    hover_text = []
    for iso_a3 in iso_codes:
        data = country_data[iso_a3]
        name = data.get("name", iso_a3)
        score = data.get("score")
        sample_counts = data.get("sample_counts", {})

        text = f"<b>{name}</b><br>"
        if score is not None:
            text += f"Score: {score:.1f}<br>"
        dl_samples = sample_counts.get("downloads")
        ul_samples = sample_counts.get("uploads")
        if dl_samples is not None:
            text += f"Download samples: {dl_samples:,}<br>"
        if ul_samples is not None:
            text += f"Upload samples: {ul_samples:,}<br>"
        text += "Click to view trends"

        hover_text.append(text)

    colors = [country_data[iso]["score"] or 0 for iso in iso_codes]

    fig = go.Figure(
        data=go.Choropleth(
            locations=iso_codes,
            z=colors,
            text=hover_text,
            customdata=iso_codes,
            colorscale="RdYlBu",
            showscale=True,
            zmin=0,
            zmax=1,
            marker_line_color=[
                "red" if selected_country and iso == selected_country else "white"
                for iso in iso_codes
            ],
            marker_line_width=[
                3 if selected_country and iso == selected_country else 0.5
                for iso in iso_codes
            ],
            hovertemplate="%{text}<extra></extra>",
            locationmode="ISO-3",
            colorbar_title="IQB Score",
        )
    )

    geo_settings = dict(
        showframe=False,
        showcoastlines=True,
        projection_type="natural earth",
        bgcolor="rgba(0,0,0,0)",
        coastlinecolor="#CCCCCC",
        showcountries=True,
        countrycolor="#E8E8E8",
        showland=True,
        landcolor="#F5F5F5",
    )

    fig.update_layout(
        geo=geo_settings,
        height=500,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )

    return fig


def create_trend_charts(country_code: str, country_name: str):
    """Create historical trend charts for a country with synchronized hover."""
    percentile = st.session_state.selected_percentile

    df = load_historical_data(country_code, percentile=percentile)

    if df.empty:
        st.warning(f"No historical data available for {country_name}")
        return

    st.markdown("### Historical Trends")
    st.caption(
        f"Showing {percentile} values • {len(df)} data points from {df['date'].min()} to {df['date'].max()}"
    )

    # Create subplots with shared x-axis
    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=(
            "IQB Score",
            "Throughput (Mbps)",
            "Latency (ms)",
            "Packet Loss (%)",
        ),
    )

    # IQB Score
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["iqb_score"],
            mode="lines+markers",
            name="IQB Score",
            line=dict(color="#F18F01", width=2),
            marker=dict(size=5),
            fill="tozeroy",
            fillcolor="rgba(241, 143, 1, 0.2)",
        ),
        row=1,
        col=1,
    )

    # Download/Upload
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["download"],
            mode="lines+markers",
            name="Download",
            line=dict(color="#2E86AB", width=2),
            marker=dict(size=5),
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["upload"],
            mode="lines+markers",
            name="Upload",
            line=dict(color="#A23B72", width=2, dash="dash"),
            marker=dict(size=5),
        ),
        row=2,
        col=1,
    )

    # Latency
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["latency"],
            mode="lines+markers",
            name="Latency",
            line=dict(color="#C7522A", width=2),
            marker=dict(size=5),
        ),
        row=3,
        col=1,
    )

    # Packet Loss
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["packet_loss"],
            mode="lines+markers",
            name="Packet Loss(%)",
            line=dict(color="#6B4C9A", width=2),
            marker=dict(size=5),
            fill="tozeroy",
            fillcolor="rgba(107, 76, 154, 0.2)",
        ),
        row=4,
        col=1,
    )

    # Update layout
    fig.update_layout(
        height=800,
        hovermode="x unified",
        template="plotly_white",
        showlegend=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    # Add hover labels to each trace
    fig.update_traces(
        hovertemplate="%{y:.3f}<extra>%{fullData.name}</extra>",
    )

    # Y-axis labels
    fig.update_yaxes(title_text="Score", row=1, col=1, range=[0, 1])
    fig.update_yaxes(title_text="Mbps", row=2, col=1)
    fig.update_yaxes(title_text="ms", row=3, col=1)
    fig.update_yaxes(title_text="Loss Rate", row=4, col=1, rangemode="tozero")

    # X-axis label only on bottom
    fig.update_xaxes(title_text="Date", row=4, col=1)

    # Vertical spike line across all charts
    fig.update_xaxes(
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
        spikecolor="black",
        spikethickness=1,
        spikedash="solid",
    )

    fig.update_traces(xaxis="x4")  # Link all traces to bottom x-axis for spike

    st.plotly_chart(fig, use_container_width=True)


def build_iqb_data_from_cache(metrics: dict, percentile: str = "p95") -> dict:
    """Convert cache JSON metrics to IQB calculation format."""
    empty = {
        "download_throughput_mbps": 0,
        "upload_throughput_mbps": 0,
        "latency_ms": 0,
        "packet_loss": 0,
    }

    return {
        "m-lab": {
            "download_throughput_mbps": metrics["download_throughput_mbps"][percentile],
            "upload_throughput_mbps": metrics["upload_throughput_mbps"][percentile],
            "latency_ms": metrics["latency_ms"][percentile],
            "packet_loss": metrics["packet_loss"][percentile],
        },
        "cloudflare": empty.copy(),
        "ookla": empty.copy(),
    }


# Scan available data
available_data = scan_available_data()
COUNTRY_CODES = list(available_data["countries"].keys())
MIN_YEAR = available_data["min_year"]
MAX_YEAR = available_data["max_year"]

# Initialize session state
if "selected_country" not in st.session_state:
    st.session_state.selected_country = None
if "selected_year" not in st.session_state:
    st.session_state.selected_year = MAX_YEAR
if "selected_month" not in st.session_state:
    st.session_state.selected_month = 10
if "selected_percentile" not in st.session_state:
    st.session_state.selected_percentile = "p95"


# Sidebar controls
with st.sidebar:
    st.header("Data Selection")

    year_options = list(range(MIN_YEAR, MAX_YEAR + 1))
    current_year_index = (
        year_options.index(st.session_state.selected_year)
        if st.session_state.selected_year in year_options
        else len(year_options) - 1
    )

    st.session_state.selected_year = st.selectbox(
        "Year", options=year_options, index=current_year_index
    )

    st.session_state.selected_percentile = st.selectbox(
        "Percentile",
        options=["p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99"],
        index=7,
        help="Which percentile to display from the data",
    )


# Main content
with st.spinner("Loading data..."):
    country_data = load_country_data_for_date(
        st.session_state.selected_year,
        st.session_state.selected_month,
        st.session_state.selected_percentile,
    )

if country_data:
    if st.session_state.selected_country:
        if st.button("← Clear Selection"):
            st.session_state.selected_country = None
            st.rerun()
    col_map, col_info = st.columns(2)

    with col_map:
        fig = create_world_map(country_data, st.session_state.selected_country)

        if fig:
            event = st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"world_map_{st.session_state.selected_country}",
                on_select="rerun",
            )

            if event and event.selection and event.selection.points:
                clicked_point = event.selection.points[0]

                if "customdata" in clicked_point:
                    clicked_iso_a3 = clicked_point["customdata"]
                    if clicked_iso_a3 in country_data:
                        if st.session_state.selected_country == clicked_iso_a3:
                            st.session_state.selected_country = None
                        else:
                            st.session_state.selected_country = clicked_iso_a3
                        st.rerun()

        if st.session_state.selected_country:
            col1, col2 = st.columns([1, 5])
            with col1:
                if st.button("← Clear Selection"):
                    st.session_state.selected_country = None
                    st.rerun()

            # Find country info
            data = country_data.get(st.session_state.selected_country)
            if data:
                country_code = data["code"]
                country_name = data["name"]
                st.markdown("---")
                create_trend_charts(country_code, country_name)
            else:
                st.error(
                    f"Country data not found for {st.session_state.selected_country}"
                )
    with col_info:
        if (
            st.session_state.selected_country
            and st.session_state.selected_country in country_data
        ):
            data = country_data[st.session_state.selected_country]
            metrics = data["metrics"]
            percentile = st.session_state.selected_percentile

            # Update state with cache data
            update_state_from_cache(state, metrics, percentile)

            st.subheader(country_name)
            st.subheader("IQB Score")

            # Tabs for sunbursts
            tab1, tab2, tab3 = st.tabs(["Requirements", "Use Cases", "Full Hierarchy"])

            try:
                iqb_data = build_iqb_data_from_cache(metrics, percentile)
                iqb_score = state.iqb.calculate_iqb_score(
                    data=iqb_data, print_details=False
                )

                with tab1:
                    render_sunburst(
                        prepare_requirements_sunburst_data(state),
                        title="Requirements → Datasets",
                        iqb_score=iqb_score,
                        height=400,
                    )

                with tab2:
                    render_sunburst(
                        prepare_use_cases_sunburst_data(state),
                        title="Use Cases → Datasets",
                        iqb_score=iqb_score,
                        height=400,
                    )

                with tab3:
                    render_sunburst(
                        prepare_complete_hierarchy_sunburst_data(state),
                        title="Use Cases → Requirements → Datasets",
                        iqb_score=iqb_score,
                        hierarchy_levels=3,
                        height=400,
                    )
            except Exception as e:
                st.error(f"Error rendering sunbursts: {e}")

            # Raw data table
            st.markdown("---")
            st.subheader("Raw Metrics")

            # Build table data
            percentiles = ["p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99"]
            table_data = {
                "Percentile": percentiles,
                "Download (Mbps)": [
                    metrics["download_throughput_mbps"][p] for p in percentiles
                ],
                "Upload (Mbps)": [
                    metrics["upload_throughput_mbps"][p] for p in percentiles
                ],
                "Latency (ms)": [metrics["latency_ms"][p] for p in percentiles],
                "Packet Loss(%)": [metrics["packet_loss"][p] for p in percentiles],
            }

            df = pd.DataFrame(table_data)

            # Highlight selected percentile
            def highlight_selected(row):
                if row["Percentile"] == percentile:
                    return ["background-color: #ffffcc"] * len(row)
                return [""] * len(row)

            st.dataframe(
                df.style.apply(highlight_selected, axis=1).format(
                    {
                        "Download (Mbps)": "{:.2f}",
                        "Upload (Mbps)": "{:.2f}",
                        "Latency (ms)": "{:.2f}",
                        " Packet Loss (%)": "{:.4f}",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

        else:
            st.info("Select a country on the map to view details")
else:
    st.warning(
        f"No data available for {st.session_state.selected_country} in {st.session_state.selected_year}"
    )

    with st.expander("🔍 Debug Info"):
        st.write(f"Cache directory: `{CACHE_DIR}`")
        st.write(f"Cache exists: {CACHE_DIR.exists()}")
        st.write(f"Countries found: {len(COUNTRY_CODES)}")
        if COUNTRY_CODES:
            st.write(f"Sample codes: {COUNTRY_CODES[:10]}")

        if CACHE_DIR.exists():
            files = list(CACHE_DIR.glob("*.json"))[:10]
            st.write(f"Sample files: {[f.name for f in files]}")
            st.write(f"Sample files: {[f.name for f in files]}")
            st.write(f"Sample files: {[f.name for f in files]}")

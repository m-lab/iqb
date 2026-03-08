"""
Country Comparison Dashboard

Compare internet quality metrics (download, upload, latency, packet loss)
side-by-side for multiple countries. Uses pre-computed cache data.
"""

import json
from pathlib import Path

import plotly.graph_objects as go
import pycountry
import streamlit as st
from plotly.subplots import make_subplots

st.set_page_config(page_title="Country Comparison", layout="wide")
st.title("🌍 Country Comparison")
st.markdown(
    "Compare internet quality metrics across countries side-by-side."
)

# --- Constants ---
CACHE_DIR = Path(__file__).parent.parent / "cache" / "v0"
PERCENTILES = ["p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99"]

# Color palette for up to 6 countries
COLORS = [
    "#2E86AB",  # blue
    "#F18F01",  # orange
    "#C73E1D",  # red
    "#3B1F2B",  # dark
    "#44BBA4",  # teal
    "#E94F37",  # coral
]


# --- Data Loading ---
def get_country_name(alpha_2: str) -> str:
    """Get country name from 2-letter code."""
    try:
        country = pycountry.countries.get(alpha_2=alpha_2.upper())
        if country:
            return country.name
    except (KeyError, LookupError):
        pass
    return alpha_2.upper()


@st.cache_data
def get_available_countries() -> list[tuple[str, str]]:
    """Scan cache directory for available country codes and return (code, name) pairs."""
    if not CACHE_DIR.exists():
        return []

    codes = set()
    for f in CACHE_DIR.glob("*.json"):
        # Filenames like us_2024_10.json => country code is first part
        parts = f.stem.split("_")
        if len(parts) >= 3:
            codes.add(parts[0].upper())

    result = []
    for code in sorted(codes):
        name = get_country_name(code)
        result.append((code, name))
    return sorted(result, key=lambda x: x[1])


@st.cache_data
def get_available_periods() -> list[tuple[str, str]]:
    """Get available (year, month) periods from cache filenames."""
    if not CACHE_DIR.exists():
        return []

    periods = set()
    for f in CACHE_DIR.glob("*.json"):
        parts = f.stem.split("_")
        if len(parts) >= 3:
            year, month = parts[1], parts[2]
            periods.add((year, month))
    return sorted(periods, reverse=True)


@st.cache_data
def load_country_data(country_code: str, year: str, month: str) -> dict | None:
    """Load cached data for a country and period."""
    filename = f"{country_code.lower()}_{year}_{month}.json"
    filepath = CACHE_DIR / filename
    if not filepath.exists():
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# --- UI ---
available_countries = get_available_countries()
available_periods = get_available_periods()

if not available_countries or not available_periods:
    st.warning("No cached data found. Run the pipeline first to populate the cache.")
    st.stop()

# Period selector
col_period, _ = st.columns([1, 3])
with col_period:
    period_labels = [f"{p[0]}-{p[1]}" for p in available_periods]
    selected_period_label = st.selectbox("Select Period", period_labels)
    selected_period = available_periods[period_labels.index(selected_period_label)]

# Country selector
country_names = [f"{name} ({code})" for code, name in available_countries]
default_countries = []
for code in ["US", "DE", "JP", "BR"]:
    for i, (c, n) in enumerate(available_countries):
        if c == code:
            default_countries.append(country_names[i])
            break

selected_country_labels = st.multiselect(
    "Select Countries to Compare (up to 6)",
    country_names,
    default=default_countries[:4],
    max_selections=6,
)

if not selected_country_labels:
    st.info("Select at least one country to see the comparison.")
    st.stop()

# Percentile selector
col_pct, _ = st.columns([1, 3])
with col_pct:
    selected_percentile = st.selectbox(
        "Percentile",
        PERCENTILES,
        index=PERCENTILES.index("p50"),
        help="p50 = median user experience, p95 = top 5% of users",
    )

# --- Load Data ---
selected_codes = []
for label in selected_country_labels:
    # Extract code from "Name (CODE)" format
    code = label.split("(")[-1].rstrip(")")
    selected_codes.append(code)

year, month = selected_period
country_data = {}
for code in selected_codes:
    data = load_country_data(code, year, month)
    if data:
        country_data[code] = data

if not country_data:
    st.warning("No data available for the selected countries and period.")
    st.stop()

# --- Charts ---
st.markdown("---")

# Extract metric values for all countries
names = []
downloads = []
uploads = []
latencies = []
losses = []

for code in selected_codes:
    if code not in country_data:
        continue
    d = country_data[code]
    metrics = d.get("metrics", {})
    names.append(f"{get_country_name(code)} ({code})")
    downloads.append(metrics.get("download_throughput_mbps", {}).get(selected_percentile, 0))
    uploads.append(metrics.get("upload_throughput_mbps", {}).get(selected_percentile, 0))
    latencies.append(metrics.get("latency_ms", {}).get(selected_percentile, 0))
    losses.append(metrics.get("packet_loss", {}).get(selected_percentile, 0) * 100)

colors = COLORS[: len(names)]

# 2x2 grid of charts
fig = make_subplots(
    rows=2,
    cols=2,
    subplot_titles=(
        "Download Speed (Mbps)",
        "Upload Speed (Mbps)",
        "Latency (ms)",
        "Packet Loss (%)",
    ),
    vertical_spacing=0.15,
    horizontal_spacing=0.10,
)

fig.add_trace(
    go.Bar(x=names, y=downloads, marker_color=colors, showlegend=False),
    row=1,
    col=1,
)
fig.add_trace(
    go.Bar(x=names, y=uploads, marker_color=colors, showlegend=False),
    row=1,
    col=2,
)
fig.add_trace(
    go.Bar(x=names, y=latencies, marker_color=colors, showlegend=False),
    row=2,
    col=1,
)
fig.add_trace(
    go.Bar(x=names, y=losses, marker_color=colors, showlegend=False),
    row=2,
    col=2,
)

fig.update_layout(
    height=600,
    margin=dict(t=60, b=40, l=40, r=20),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)
fig.update_yaxes(gridcolor="rgba(128,128,128,0.2)")

st.plotly_chart(fig, width="stretch")

# --- Percentile Distribution Chart ---
st.markdown("---")
st.subheader("Percentile Distribution")

metric_choice = st.selectbox(
    "Select Metric",
    [
        "download_throughput_mbps",
        "upload_throughput_mbps",
        "latency_ms",
        "packet_loss",
    ],
    format_func=lambda x: {
        "download_throughput_mbps": "Download Speed (Mbps)",
        "upload_throughput_mbps": "Upload Speed (Mbps)",
        "latency_ms": "Latency (ms)",
        "packet_loss": "Packet Loss Rate",
    }[x],
)

dist_fig = go.Figure()

for i, code in enumerate(selected_codes):
    if code not in country_data:
        continue
    metrics = country_data[code].get("metrics", {}).get(metric_choice, {})
    pct_labels = []
    pct_values = []
    for p in PERCENTILES:
        if p in metrics:
            pct_labels.append(p)
            val = metrics[p]
            if metric_choice == "packet_loss":
                val = val * 100
            pct_values.append(val)

    dist_fig.add_trace(
        go.Scatter(
            x=pct_labels,
            y=pct_values,
            mode="lines+markers",
            name=f"{get_country_name(code)} ({code})",
            line=dict(color=COLORS[i % len(COLORS)], width=2),
            marker=dict(size=6),
        )
    )

unit = {
    "download_throughput_mbps": "Mbps",
    "upload_throughput_mbps": "Mbps",
    "latency_ms": "ms",
    "packet_loss": "%",
}[metric_choice]

dist_fig.update_layout(
    height=400,
    xaxis_title="Percentile",
    yaxis_title=unit,
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    margin=dict(t=20, b=40, l=40, r=20),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
)
dist_fig.update_yaxes(gridcolor="rgba(128,128,128,0.2)")

st.plotly_chart(dist_fig, width="stretch")

# --- Summary Table ---
st.markdown("---")
st.subheader("Summary Table")

table_data = []
for code in selected_codes:
    if code not in country_data:
        continue
    d = country_data[code]
    metrics = d.get("metrics", {})
    samples = d.get("sample_counts", {})
    table_data.append(
        {
            "Country": get_country_name(code),
            "Code": code,
            "Download (Mbps)": round(
                metrics.get("download_throughput_mbps", {}).get(selected_percentile, 0), 2
            ),
            "Upload (Mbps)": round(
                metrics.get("upload_throughput_mbps", {}).get(selected_percentile, 0), 2
            ),
            "Latency (ms)": round(
                metrics.get("latency_ms", {}).get(selected_percentile, 0), 2
            ),
            "Packet Loss (%)": round(
                metrics.get("packet_loss", {}).get(selected_percentile, 0) * 100, 4
            ),
            "Download Samples": f"{samples.get('downloads', 0):,}",
            "Upload Samples": f"{samples.get('uploads', 0):,}",
        }
    )

if table_data:
    st.dataframe(table_data, width="stretch")

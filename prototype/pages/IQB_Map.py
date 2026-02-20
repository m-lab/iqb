"""
IQB Interactive Map Page

Displays world map with countries that have data, allows clicking for historical trends.
Uses IQBCache API for data fetching. Supports drill-down to subdivision/city level.

Data sources:
- geojson_admin1: Admin1/subdivision boundaries (when country clicked)
- simplemaps_worldcities: City coordinates (when subdivision clicked)
"""

import json
import re
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen

import pandas as pd
import plotly.graph_objects as go
import pycountry
import streamlit as st
from dacite import from_dict
from iqb import IQB, IQBCache, IQBDatasetGranularity
from iqb.ghremote.cache import IQBRemoteCache, Manifest, data_dir_or_default
from plotly.subplots import make_subplots
from session_state import initialize_app_state
from utils.calculation_utils import (
    calculate_iqb_score_with_custom_settings,
    get_config_with_custom_settings,
)
from visualizations.sunburst_data import (
    prepare_complete_hierarchy_sunburst_data,
    prepare_requirements_sunburst_data,
    prepare_use_cases_sunburst_data,
)
from visualizations.ui_components import render_sunburst

st.set_page_config(page_title="IQB Map", layout="wide")
st.title("Global Internet Quality Barometer Map")

# --- Constants ---
SCRIPT_DIR = Path(__file__).parent
GEOJSON_ADMIN1_DIR = SCRIPT_DIR.parent / "natural_earth" / "geojson_admin1"
MANIFEST_URL = (
    "https://raw.githubusercontent.com/m-lab/iqb/main/data/state/ghremote/manifest.json"
)
PERCENTILES = ["p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99"]
JOIN_COLS = [
    "country_code",
    "subdivision1_iso_code",
    "city",
    "period_start",
    "period_end",
    "period_label",
]

# Country coordinates: (lat, lon, zoom)
COUNTRY_COORDS = {
    "USA": (39.8, -98.5, 3.0),
    "CAN": (56.1, -106.3, 2.0),
    "MEX": (23.6, -102.5, 4.0),
    "BRA": (-14.2, -51.9, 3.0),
    "ARG": (-38.4, -63.6, 3.5),
    "COL": (4.5, -74.3, 5.0),
    "CHL": (-35.7, -71.5, 3.0),
    "PER": (-9.2, -75.0, 4.0),
    "VEN": (6.4, -66.6, 5.0),
    "ECU": (-1.8, -78.2, 7.0),
    "BOL": (-16.3, -63.6, 5.0),
    "PRY": (-23.4, -58.4, 6.0),
    "URY": (-32.5, -55.8, 8.0),
    "GBR": (55.4, -3.4, 10.0),
    "DEU": (51.2, 10.5, 10.0),
    "FRA": (46.2, 2.2, 8.0),
    "ITA": (41.9, 12.6, 8.0),
    "ESP": (40.5, -3.7, 7.0),
    "PRT": (39.4, -8.2, 10.0),
    "NLD": (52.1, 5.3, 15.0),
    "BEL": (50.5, 4.5, 18.0),
    "CHE": (46.8, 8.2, 15.0),
    "AUT": (47.5, 14.6, 12.0),
    "POL": (51.9, 19.1, 8.0),
    "SWE": (60.1, 18.6, 5.0),
    "NOR": (60.5, 8.5, 5.0),
    "FIN": (61.9, 25.7, 6.0),
    "DNK": (56.3, 9.5, 12.0),
    "IRL": (53.1, -8.2, 12.0),
    "GRC": (39.1, 21.8, 8.0),
    "CZE": (49.8, 15.5, 12.0),
    "ROU": (45.9, 25.0, 8.0),
    "HUN": (47.2, 19.5, 12.0),
    "UKR": (48.4, 31.2, 5.0),
    "BGR": (42.7, 25.5, 10.0),
    "SRB": (44.0, 21.0, 12.0),
    "HRV": (45.1, 15.2, 10.0),
    "SVK": (48.7, 19.7, 12.0),
    "SVN": (46.2, 14.9, 15.0),
    "LTU": (55.2, 23.9, 12.0),
    "LVA": (56.9, 24.6, 12.0),
    "EST": (58.6, 25.0, 12.0),
    "CHN": (35.9, 104.2, 3.0),
    "JPN": (36.2, 138.3, 6.0),
    "KOR": (35.9, 127.8, 10.0),
    "IND": (20.6, 79.0, 3.5),
    "IDN": (-0.8, 113.9, 3.5),
    "THA": (15.9, 100.9, 6.0),
    "VNM": (14.1, 108.3, 5.0),
    "MYS": (4.2, 101.9, 6.0),
    "PHL": (12.9, 121.8, 6.0),
    "SGP": (1.4, 103.8, 60.0),
    "PAK": (30.4, 69.3, 5.0),
    "BGD": (23.7, 90.4, 8.0),
    "TWN": (23.7, 121.0, 12.0),
    "HKG": (22.4, 114.1, 50.0),
    "KAZ": (48.0, 68.0, 3.5),
    "UZB": (41.4, 64.6, 6.0),
    "MMR": (19.8, 96.1, 5.0),
    "NPL": (28.4, 84.1, 8.0),
    "LKA": (7.9, 80.8, 12.0),
    "SAU": (23.9, 45.1, 4.5),
    "ARE": (23.4, 53.8, 10.0),
    "ISR": (31.0, 34.9, 15.0),
    "TUR": (38.9, 35.2, 5.0),
    "IRN": (32.4, 53.7, 4.5),
    "IRQ": (33.2, 43.7, 6.0),
    "QAT": (25.4, 51.2, 25.0),
    "KWT": (29.3, 47.5, 20.0),
    "OMN": (21.5, 55.9, 8.0),
    "JOR": (30.6, 36.2, 12.0),
    "LBN": (33.9, 35.9, 18.0),
    "ZAF": (-30.6, 22.9, 4.5),
    "EGY": (26.8, 30.8, 5.5),
    "NGA": (9.1, 8.7, 5.0),
    "KEN": (-0.0, 38.0, 6.0),
    "MAR": (31.8, -7.1, 6.0),
    "ETH": (9.1, 40.5, 5.0),
    "GHA": (7.9, -1.0, 8.0),
    "TZA": (-6.4, 34.9, 5.5),
    "DZA": (28.0, 1.7, 4.0),
    "TUN": (34.0, 9.5, 10.0),
    "UGA": (1.4, 32.3, 8.0),
    "SEN": (14.5, -14.5, 8.0),
    "CIV": (7.5, -5.5, 7.0),
    "CMR": (7.4, 12.4, 6.0),
    "AGO": (-11.2, 17.9, 4.5),
    "MOZ": (-18.7, 35.5, 4.5),
    "ZWE": (-19.0, 29.2, 7.0),
    "ZMB": (-13.1, 27.8, 5.5),
    "AUS": (-25.3, 133.8, 2.8),
    "NZL": (-40.9, 174.9, 6.0),
    "PNG": (-6.3, 143.9, 6.0),
    "FJI": (-17.7, 178.0, 12.0),
    "RUS": (61.5, 105.3, 1.5),
}


# --- Remote Cache ---
class GitHubURLRemoteCache(IQBRemoteCache):
    """Remote cache that fetches manifest from a URL."""

    def __init__(self, manifest_url: str, data_dir=None):
        self.data_dir = data_dir_or_default(data_dir)
        self.manifest = self._load_manifest_from_url(manifest_url)

    def _load_manifest_from_url(self, url: str) -> Manifest:
        with urlopen(url) as resp:
            data = json.load(resp)
        return from_dict(Manifest, data)


# --- Session State ---
if "app_state" not in st.session_state:
    st.session_state.app_state = initialize_app_state()
if "selected_country" not in st.session_state:
    st.session_state.selected_country = None
if "selected_subdivision" not in st.session_state:
    st.session_state.selected_subdivision = None
if "selected_percentile" not in st.session_state:
    st.session_state.selected_percentile = "p95"

state = st.session_state.app_state
custom_config = get_config_with_custom_settings(state)


# --- Helper Functions ---
def get_country_center(iso_a3: str) -> tuple | None:
    """Return (latitude, longitude, zoom_scale) for a country."""
    return COUNTRY_COORDS.get(iso_a3)


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


def extract_percentiles_from_columns(columns: list[str]) -> list[int]:
    """Extract sorted percentile numbers from column names."""
    return sorted(
        {
            int(col.split("_p")[1])
            for col in columns
            if "_p" in col and col.split("_p")[1].isdigit()
        }
    )


def build_metrics_dict(
    row: pd.Series, percentiles: list[int], is_upload: bool = False
) -> dict:
    """Build metrics dictionary from a DataFrame row."""
    suffix = "" if not is_upload else "_up"
    metrics = {
        "download_throughput_mbps": {},
        "upload_throughput_mbps": {},
        "latency_ms": {},
        "packet_loss": {},
    }
    for p in percentiles:
        pkey = f"p{p}"
        metrics["download_throughput_mbps"][pkey] = float(row[f"download_p{p}"])
        metrics["upload_throughput_mbps"][pkey] = float(row[f"upload_p{p}"])
        metrics["latency_ms"][pkey] = float(row[f"latency_p{p}"])
        metrics["packet_loss"][pkey] = float(row[f"loss_p{p}"])
    return metrics


def build_iqb_data_from_cache(metrics: dict, percentile: str = "p95") -> dict:
    """Convert cache metrics to IQB calculation format."""
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


def calculate_iqb_score_from_metrics(
    metrics: dict, percentile: str = "p95", custom_config: dict | None = None
) -> float | None:
    """Calculate actual IQB score from cache metrics."""
    if not metrics:
        return None
    try:
        iqb_data = build_iqb_data_from_cache(metrics, percentile)
        calculator = IQB(config=custom_config) if custom_config is not None else IQB()
        return calculator.calculate_iqb_score(data=iqb_data, print_details=False)
    except Exception:
        return None


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


# --- Cache Functions ---
@st.cache_resource
def get_iqb_cache() -> IQBCache:
    remote_cache = GitHubURLRemoteCache(MANIFEST_URL)
    return IQBCache(remote_cache=remote_cache)


@st.cache_data
def get_available_periods(_manifest_files: tuple) -> list[tuple[str, str, str]]:
    """Parse available periods from manifest files."""
    periods = set()
    for key in _manifest_files:
        match = re.match(
            r"cache/v1/(\d{4})(\d{2})(\d{2})T.*/(\d{4})(\d{2})(\d{2})T.*/downloads_by_country_city/",
            key,
        )
        if match:
            sy, sm, sd, ey, em, ed = match.groups()
            start_date = f"{sy}-{sm}-{sd}"
            end_date = f"{ey}-{em}-{ed}"
            start_label = datetime.strptime(start_date, "%Y-%m-%d").strftime("%b %Y")
            end_label = datetime.strptime(end_date, "%Y-%m-%d").strftime("%b %Y")
            label = f"{start_label} - {end_label}"
            periods.add((label, start_date, end_date))
    return sorted(periods, key=lambda x: x[1], reverse=True)


# --- Data Fetching ---
@st.cache_data
def fetch_map_data(_cache: IQBCache, start_date: str, end_date: str) -> dict[str, dict]:
    """Fetch M-Lab data for all countries for map visualization."""
    entry = _cache.get_cache_entry(
        start_date=start_date,
        end_date=end_date,
        granularity=IQBDatasetGranularity.COUNTRY,
    )
    download_df = entry.mlab.read_download_data_frame()
    upload_df = entry.mlab.read_upload_data_frame()
    merged = download_df.merge(upload_df, on="country_code", suffixes=("", "_up"))
    percentiles = extract_percentiles_from_columns(download_df.columns)

    results = {}
    for _, row in merged.iterrows():
        alpha_2 = row["country_code"]
        country_info = get_country_info(alpha_2)
        if not country_info:
            continue

        iso_a3 = country_info["alpha_3"]
        metrics = build_metrics_dict(row, percentiles)

        results[iso_a3] = {
            "code": alpha_2.lower(),
            "iso_a3": iso_a3,
            "name": country_info["name"],
            "metrics": metrics,
            "sample_counts": {
                "downloads": int(row.get("sample_count", 0)),
                "uploads": int(row.get("sample_count_up", row.get("sample_count", 0))),
            },
        }
    return results


@st.cache_data
def fetch_subdivision_data(
    _cache: IQBCache, country_code: str, start_date: str, end_date: str
) -> dict[str, dict]:
    """Fetch M-Lab data aggregated by subdivision for a country."""
    entry = _cache.get_cache_entry(
        start_date=start_date,
        end_date=end_date,
        granularity=IQBDatasetGranularity.COUNTRY_CITY,
    )
    download_df = entry.mlab.read_download_data_frame(country_code=country_code)
    upload_df = entry.mlab.read_upload_data_frame(country_code=country_code)

    if download_df.empty or upload_df.empty:
        return {}

    merge_cols = [
        c for c in JOIN_COLS if c in download_df.columns and c in upload_df.columns
    ]
    merged = download_df.merge(upload_df, on=merge_cols, suffixes=("", "_up"))
    percentiles = extract_percentiles_from_columns(download_df.columns)

    subdivision_data = {}
    for subdivision_code, group in merged.groupby("subdivision1_iso_code"):
        if not subdivision_code or subdivision_code == "" or pd.isna(subdivision_code):
            continue

        full_code = (
            f"{country_code}-{subdivision_code}"
            if "-" not in str(subdivision_code)
            else subdivision_code
        )
        total_samples = group["sample_count"].sum()
        total_samples_up = group.get("sample_count_up", group["sample_count"]).sum()

        metrics = {
            "download_throughput_mbps": {},
            "upload_throughput_mbps": {},
            "latency_ms": {},
            "packet_loss": {},
        }
        for p in percentiles:
            pkey = f"p{p}"
            if total_samples > 0:
                metrics["download_throughput_mbps"][pkey] = float(
                    (group[f"download_p{p}"] * group["sample_count"]).sum()
                    / total_samples
                )
                metrics["latency_ms"][pkey] = float(
                    (group[f"latency_p{p}"] * group["sample_count"]).sum()
                    / total_samples
                )
                metrics["packet_loss"][pkey] = float(
                    (group[f"loss_p{p}"] * group["sample_count"]).sum() / total_samples
                )
            if total_samples_up > 0:
                sample_col = (
                    "sample_count_up"
                    if "sample_count_up" in group.columns
                    else "sample_count"
                )
                metrics["upload_throughput_mbps"][pkey] = float(
                    (group[f"upload_p{p}"] * group[sample_col]).sum() / total_samples_up
                )

        subdivision_data[full_code] = {
            "subdivision_code": full_code,
            "subdivision_name": group["subdivision1_name"].iloc[0]
            if "subdivision1_name" in group.columns
            else None,
            "metrics": metrics,
            "sample_counts": {
                "downloads": int(total_samples),
                "uploads": int(total_samples_up),
            },
            "city_count": len(group),
        }
    return subdivision_data


@st.cache_data
def fetch_city_data(
    _cache: IQBCache, country_code: str, start_date: str, end_date: str
) -> dict[str, dict]:
    """Fetch M-Lab data for all cities in a country."""
    entry = _cache.get_cache_entry(
        start_date=start_date,
        end_date=end_date,
        granularity=IQBDatasetGranularity.COUNTRY_CITY,
    )
    download_df = entry.mlab.read_download_data_frame(country_code=country_code)
    upload_df = entry.mlab.read_upload_data_frame(country_code=country_code)

    if download_df.empty or upload_df.empty:
        return {}

    merge_cols = [
        c for c in JOIN_COLS if c in download_df.columns and c in upload_df.columns
    ]
    merged = download_df.merge(upload_df, on=merge_cols, suffixes=("", "_up"))
    percentiles = extract_percentiles_from_columns(download_df.columns)

    results = {}
    for _, row in merged.iterrows():
        city = row["city"]
        subdivision = row.get("subdivision1_iso_code", "")
        city_key = f"{city}" if not subdivision else f"{city} ({subdivision})"
        metrics = build_metrics_dict(row, percentiles)

        results[city_key] = {
            "city": city,
            "subdivision": subdivision,
            "metrics": metrics,
            "sample_counts": {
                "downloads": int(row.get("sample_count", 0)),
                "uploads": int(row.get("sample_count_up", row.get("sample_count", 0))),
            },
        }
    return results


@st.cache_data
def load_historical_data(
    _cache: IQBCache,
    country_code: str,
    available_periods: list[tuple[str, str, str]],
    percentile: str = "p95",
    subdivision_code: str | None = None,
    custom_config: dict | None = None,
) -> pd.DataFrame:
    """Load historical data for a country or subdivision across all available periods."""
    rows = []
    p_num = percentile[1:]
    granularity = (
        IQBDatasetGranularity.COUNTRY_CITY
        if subdivision_code
        else IQBDatasetGranularity.COUNTRY
    )

    for label, start_date, end_date in available_periods:
        try:
            entry = _cache.get_cache_entry(
                start_date=start_date, end_date=end_date, granularity=granularity
            )

            if subdivision_code:
                # Subdivision-level data
                download_df = entry.mlab.read_download_data_frame(
                    country_code=country_code
                )
                upload_df = entry.mlab.read_upload_data_frame(country_code=country_code)
                if download_df.empty or upload_df.empty:
                    continue

                subdivision_iso = (
                    subdivision_code.split("-")[-1]
                    if "-" in subdivision_code
                    else subdivision_code
                )
                dl_data = download_df[
                    download_df["subdivision1_iso_code"] == subdivision_iso
                ]
                ul_data = upload_df[
                    upload_df["subdivision1_iso_code"] == subdivision_iso
                ]

                if dl_data.empty or ul_data.empty:
                    continue

                total_samples = dl_data["sample_count"].sum()
                total_samples_up = ul_data["sample_count"].sum()
                if total_samples == 0 or total_samples_up == 0:
                    continue

                download = (
                    dl_data[f"download_p{p_num}"] * dl_data["sample_count"]
                ).sum() / total_samples
                upload = (
                    ul_data[f"upload_p{p_num}"] * ul_data["sample_count"]
                ).sum() / total_samples_up
                latency = (
                    dl_data[f"latency_p{p_num}"] * dl_data["sample_count"]
                ).sum() / total_samples
                packet_loss = (
                    dl_data[f"loss_p{p_num}"] * dl_data["sample_count"]
                ).sum() / total_samples
            else:
                # Country-level data
                download_df = entry.mlab.read_download_data_frame()
                upload_df = entry.mlab.read_upload_data_frame()

                dl_row = download_df[download_df["country_code"] == country_code]
                ul_row = upload_df[upload_df["country_code"] == country_code]
                if dl_row.empty or ul_row.empty:
                    continue

                dl_row, ul_row = dl_row.iloc[0], ul_row.iloc[0]
                download = float(dl_row[f"download_p{p_num}"])
                upload = float(ul_row[f"upload_p{p_num}"])
                latency = float(dl_row[f"latency_p{p_num}"])
                packet_loss = float(dl_row[f"loss_p{p_num}"])

            metrics = {
                "download_throughput_mbps": {percentile: float(download)},
                "upload_throughput_mbps": {percentile: float(upload)},
                "latency_ms": {percentile: float(latency)},
                "packet_loss": {percentile: float(packet_loss)},
            }
            score = calculate_iqb_score_from_metrics(metrics, percentile, custom_config)

            rows.append(
                {
                    "date": start_date,
                    "iqb_score": score,
                    "download": metrics["download_throughput_mbps"][percentile],
                    "upload": metrics["upload_throughput_mbps"][percentile],
                    "latency": metrics["latency_ms"][percentile],
                    "packet_loss": metrics["packet_loss"][percentile] * 100,
                }
            )
        except Exception:
            continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


@st.cache_data
def load_country_data_for_date(
    _cache: IQBCache,
    start_date: str,
    end_date: str,
    percentile: str = "p95",
    custom_config: dict | None = None,
) -> dict[str, dict]:
    """Load and enrich country data with IQB scores for a specific date range."""
    raw_data = fetch_map_data(_cache, start_date, end_date)
    for iso_a3, data in raw_data.items():
        metrics = data["metrics"]
        score = calculate_iqb_score_from_metrics(metrics, percentile, custom_config)
        data["score"] = score
        data["download"] = metrics["download_throughput_mbps"].get(percentile)
        data["upload"] = metrics["upload_throughput_mbps"].get(percentile)
        data["latency"] = metrics["latency_ms"].get(percentile)
        data["packet_loss"] = metrics["packet_loss"].get(percentile)
    return raw_data


@st.cache_data
def load_admin1_geojson(alpha_3: str) -> dict | None:
    """Load admin1 GeoJSON for a country."""
    geojson_file = GEOJSON_ADMIN1_DIR / f"{alpha_3}_admin1.geojson"
    if not geojson_file.exists():
        return None
    try:
        with open(geojson_file, "r") as f:
            return json.load(f)
    except Exception:
        return None


@st.cache_data
def load_simplemaps_cities() -> dict[tuple, tuple[float, float]]:
    """Load SimpleMaps world cities database."""
    cities_path = (
        SCRIPT_DIR.parent
        / "natural_earth"
        / "simplemaps_worldcities"
        / "worldcities.csv"
    )
    if not cities_path.exists():
        return {}

    try:
        df = pd.read_csv(cities_path, encoding="utf-8")
    except Exception:
        return {}

    cities = {}
    for _, row in df.iterrows():
        name, country, admin1 = (
            row.get("city", ""),
            row.get("iso2", ""),
            row.get("admin_name", ""),
        )
        lat, lon = row.get("lat"), row.get("lng")

        if pd.isna(name) or pd.isna(country) or pd.isna(lat) or pd.isna(lon):
            continue

        name, country = str(name), str(country)
        admin1 = str(admin1) if pd.notna(admin1) else ""

        key = (country.upper(), admin1.lower(), name.lower())
        cities[key] = (float(lat), float(lon))

        key_simple = (country.upper(), name.lower())
        if key_simple not in cities:
            cities[key_simple] = (float(lat), float(lon))

    return cities


def normalize_name(name: str) -> str:
    """Normalize a name for fuzzy matching."""
    import unicodedata

    # Remove accents
    normalized = unicodedata.normalize("NFD", name)
    normalized = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    # Lowercase and strip
    normalized = normalized.lower().strip()
    # Remove common prefixes/suffixes that vary between datasets
    for prefix in [
        "state of ",
        "land ",
        "region ",
        "province of ",
        "republic of ",
        "free state of ",
    ]:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
    return normalized


def lookup_city_coords(
    country_code: str,
    subdivision_name: str,
    city_names: list[str],
) -> dict[str, tuple[float, float]]:
    """Lookup city coordinates from SimpleMaps database, prioritizing subdivision match."""
    sm_cities = load_simplemaps_cities()
    coords = {}
    norm_subdiv = normalize_name(subdivision_name)

    for city in city_names:
        city_lower = city.lower()
        city_norm = normalize_name(city)

        # First priority: match (country, admin, city) with normalized names
        found = False
        for sm_key, coord in sm_cities.items():
            if len(sm_key) == 3 and sm_key[0] == country_code.upper():
                # Check if admin matches our subdivision
                if normalize_name(sm_key[1]) == norm_subdiv:
                    # Check if city matches
                    if (
                        sm_key[2].lower() == city_lower
                        or normalize_name(sm_key[2]) == city_norm
                    ):
                        coords[city] = coord
                        found = True
                        break

        # Skip country-level fallback to avoid wrong-state matches
        # Cities not in SimpleMaps for this subdivision simply won't show

    return coords


# --- UI Components ---
def render_time_range_selector(chart_key: str) -> int:
    """Render time range selector buttons and return number of periods."""
    st.markdown(
        '<style>div[data-testid="stHorizontalBlock"] > div:has(button) { gap: 0.25rem; }</style>',
        unsafe_allow_html=True,
    )
    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
    btn_1m = c1.button("1M", key=f"{chart_key}_1m")
    btn_3m = c2.button("3M", key=f"{chart_key}_3m")
    btn_6m = c3.button("6M", key=f"{chart_key}_6m")
    btn_1y = c4.button("1Y", key=f"{chart_key}_1y")

    range_key = f"{chart_key}_range"
    if range_key not in st.session_state:
        st.session_state[range_key] = 2

    if btn_1m:
        st.session_state[range_key] = 2
    elif btn_3m:
        st.session_state[range_key] = 3
    elif btn_6m:
        st.session_state[range_key] = 6
    elif btn_1y:
        st.session_state[range_key] = 12

    return st.session_state[range_key]


def create_trend_figure(
    df: pd.DataFrame,
    compare_df: pd.DataFrame = None,
    compare_name: str = None,
    primary_name: str = None,
) -> go.Figure:
    """Create the trend chart figure from DataFrame, optionally with comparison."""
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

    # Primary data - legendgroup keeps them together, legendgrouptitle adds header
    primary_label = primary_name if primary_name else "Selected"
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
            legendgroup="primary",
            legendgrouptitle_text=primary_label,
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["download"],
            mode="lines+markers",
            name="Download",
            line=dict(color="#2E86AB", width=2),
            marker=dict(size=5),
            legendgroup="primary",
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
            legendgroup="primary",
        ),
        row=2,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["latency"],
            mode="lines+markers",
            name="Latency",
            line=dict(color="#C7522A", width=2),
            marker=dict(size=5),
            legendgroup="primary",
        ),
        row=3,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["packet_loss"],
            mode="lines+markers",
            name="Packet Loss",
            line=dict(color="#6B4C9A", width=2),
            marker=dict(size=5),
            fill="tozeroy",
            fillcolor="rgba(107, 76, 154, 0.2)",
            legendgroup="primary",
        ),
        row=4,
        col=1,
    )

    # Comparison data (dashed lines, muted colors)
    if compare_df is not None and not compare_df.empty:
        fig.add_trace(
            go.Scatter(
                x=compare_df["date"],
                y=compare_df["iqb_score"],
                mode="lines+markers",
                name="IQB Score",
                line=dict(color="#F18F01", width=2, dash="dash"),
                marker=dict(size=4, symbol="diamond"),
                opacity=0.6,
                legendgroup="compare",
                legendgrouptitle_text=compare_name,
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=compare_df["date"],
                y=compare_df["download"],
                mode="lines+markers",
                name="Download",
                line=dict(color="#2E86AB", width=2, dash="dot"),
                marker=dict(size=4, symbol="diamond"),
                opacity=0.6,
                legendgroup="compare",
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=compare_df["date"],
                y=compare_df["upload"],
                mode="lines+markers",
                name="Upload",
                line=dict(color="#A23B72", width=2, dash="dot"),
                marker=dict(size=4, symbol="diamond"),
                opacity=0.6,
                legendgroup="compare",
            ),
            row=2,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=compare_df["date"],
                y=compare_df["latency"],
                mode="lines+markers",
                name="Latency",
                line=dict(color="#C7522A", width=2, dash="dot"),
                marker=dict(size=4, symbol="diamond"),
                opacity=0.6,
                legendgroup="compare",
            ),
            row=3,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=compare_df["date"],
                y=compare_df["packet_loss"],
                mode="lines+markers",
                name="Packet Loss",
                line=dict(color="#6B4C9A", width=2, dash="dot"),
                marker=dict(size=4, symbol="diamond"),
                opacity=0.6,
                legendgroup="compare",
            ),
            row=4,
            col=1,
        )

    fig.update_layout(
        height=800 if not (compare_df is not None and not compare_df.empty) else 900,
        hovermode="x unified",
        template="plotly_white",
        showlegend=compare_df is not None and not compare_df.empty,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.15,
            xanchor="center",
            x=0.5,
            groupclick="toggleitem",
        ),
        margin=dict(b=180 if compare_df is not None and not compare_df.empty else 50),
    )
    fig.update_traces(xaxis="x4")
    fig.update_yaxes(title_text="Score", row=1, col=1, range=[0, 1])
    fig.update_yaxes(title_text="Mbps", row=2, col=1)
    fig.update_yaxes(title_text="ms", row=3, col=1)
    fig.update_yaxes(title_text="Loss Rate", row=4, col=1, rangemode="tozero")
    fig.update_xaxes(title_text="Date", row=4, col=1)

    return fig


def get_all_countries_for_comparison(
    cache: IQBCache, start_date: str, end_date: str
) -> list[tuple[str, str]]:
    """Get list of all countries with data for comparison dropdown."""
    data = fetch_map_data(cache, start_date, end_date)
    countries = []
    for iso_a3, info in data.items():
        countries.append((info["code"].upper(), info["name"]))
    return sorted(countries, key=lambda x: x[1])


def get_subdivisions_for_comparison(
    cache: IQBCache, country_code: str, start_date: str, end_date: str
) -> list[tuple[str, str]]:
    """Get list of subdivisions for a country for comparison dropdown."""
    data = fetch_subdivision_data(cache, country_code, start_date, end_date)
    subdivisions = []
    for code, info in data.items():
        name = info.get("subdivision_name", code)
        subdivisions.append((code, name))
    return sorted(subdivisions, key=lambda x: x[1])


def get_cities_for_comparison(
    cache: IQBCache,
    country_code: str,
    subdivision_code: str,
    start_date: str,
    end_date: str,
) -> list[str]:
    """Get list of cities for a subdivision for comparison dropdown."""
    data = fetch_city_data(cache, country_code, start_date, end_date)
    subdivision_iso = (
        subdivision_code.split("-")[-1] if "-" in subdivision_code else subdivision_code
    )
    cities = [
        info["city"]
        for info in data.values()
        if info.get("subdivision") == subdivision_iso
    ]
    return sorted(set(cities))


def render_comparison_sidebar(
    cache: IQBCache,
    start_date: str,
    end_date: str,
    chart_key: str,
) -> tuple[str | None, str | None, str | None, str | None]:
    """
    Render comparison dropdowns in sidebar.
    Returns: (country_code, subdivision_code, city_name, display_name) or (None, None, None, None)
    """
    with st.sidebar:
        st.markdown("### Compare Trends")

        compare_country_key = f"{chart_key}_compare_country"
        compare_subdiv_key = f"{chart_key}_compare_subdiv"

        def _clear_comparison():
            st.session_state[compare_country_key] = 0
            st.session_state[compare_subdiv_key] = 0

        st.button(
            "Clear Comparison",
            key=f"{chart_key}_clear_compare",
            on_click=_clear_comparison,
        )

        # Country dropdown
        countries = get_all_countries_for_comparison(cache, start_date, end_date)
        country_options = [("", "Select Country")] + countries

        compare_country_key = f"{chart_key}_compare_country"
        selected_country_idx = st.selectbox(
            "Country",
            range(len(country_options)),
            format_func=lambda i: country_options[i][1],
            key=compare_country_key,
        )

        selected_country_code = country_options[selected_country_idx][0]
        selected_country_name = country_options[selected_country_idx][1]

        if not selected_country_code:
            return None, None, None, None

        # Subdivision dropdown (populated from selected country)
        subdivisions = get_subdivisions_for_comparison(
            cache, selected_country_code, start_date, end_date
        )
        subdiv_options = [("", "All (Country Level)")] + subdivisions

        compare_subdiv_key = f"{chart_key}_compare_subdiv"
        selected_subdiv_idx = st.selectbox(
            "State/Region",
            range(len(subdiv_options)),
            format_func=lambda i: subdiv_options[i][1],
            key=compare_subdiv_key,
        )

        selected_subdiv_code = subdiv_options[selected_subdiv_idx][0]
        selected_subdiv_name = subdiv_options[selected_subdiv_idx][1]

        if not selected_subdiv_code:
            # Country-level comparison
            return selected_country_code, None, None, f"{selected_country_name}"

        # City dropdown (populated from selected subdivision)
        cities = get_cities_for_comparison(
            cache, selected_country_code, selected_subdiv_code, start_date, end_date
        )
        city_options = ["All (Region Level)"] + cities

        compare_city_key = f"{chart_key}_compare_city"
        selected_city = st.selectbox(
            "City",
            city_options,
            key=compare_city_key,
        )

        if selected_city == "All (Region Level)":
            # Subdivision-level comparison
            return (
                selected_country_code,
                selected_subdiv_code,
                None,
                f"{selected_subdiv_name}, {selected_country_name}",
            )

        # City-level comparison
        return (
            selected_country_code,
            selected_subdiv_code,
            selected_city,
            f"{selected_city}, {selected_subdiv_name}",
        )


@st.cache_data
def load_historical_data_city(
    _cache: IQBCache,
    country_code: str,
    subdivision_code: str,
    city_name: str,
    available_periods: list[tuple[str, str, str]],
    percentile: str = "p95",
    custom_config: dict | None = None,
) -> pd.DataFrame:
    """Load historical data for a specific city across all available periods."""
    rows = []
    p_num = percentile[1:]
    subdivision_iso = (
        subdivision_code.split("-")[-1] if "-" in subdivision_code else subdivision_code
    )

    for label, start_date, end_date in available_periods:
        try:
            entry = _cache.get_cache_entry(
                start_date=start_date,
                end_date=end_date,
                granularity=IQBDatasetGranularity.COUNTRY_CITY,
            )
            download_df = entry.mlab.read_download_data_frame(country_code=country_code)
            upload_df = entry.mlab.read_upload_data_frame(country_code=country_code)

            if download_df.empty or upload_df.empty:
                continue

            # Filter for specific city in subdivision
            dl_city = download_df[
                (download_df["subdivision1_iso_code"] == subdivision_iso)
                & (download_df["city"] == city_name)
            ]
            ul_city = upload_df[
                (upload_df["subdivision1_iso_code"] == subdivision_iso)
                & (upload_df["city"] == city_name)
            ]

            if dl_city.empty or ul_city.empty:
                continue

            # Use first matching row (should be only one per period)
            dl_row = dl_city.iloc[0]
            ul_row = ul_city.iloc[0]

            metrics = {
                "download_throughput_mbps": {
                    percentile: float(dl_row[f"download_p{p_num}"])
                },
                "upload_throughput_mbps": {
                    percentile: float(ul_row[f"upload_p{p_num}"])
                },
                "latency_ms": {percentile: float(dl_row[f"latency_p{p_num}"])},
                "packet_loss": {percentile: float(dl_row[f"loss_p{p_num}"])},
            }

            score = calculate_iqb_score_from_metrics(metrics, percentile, custom_config)

            rows.append(
                {
                    "date": start_date,
                    "iqb_score": score,
                    "download": metrics["download_throughput_mbps"][percentile],
                    "upload": metrics["upload_throughput_mbps"][percentile],
                    "latency": metrics["latency_ms"][percentile],
                    "packet_loss": metrics["packet_loss"][percentile] * 100,
                }
            )
        except Exception:
            continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


def create_trend_charts(
    cache: IQBCache,
    country_code: str,
    entity_name: str,
    available_periods: list[tuple[str, str, str]],
    chart_key: str = "trend_charts",
    subdivision_code: str | None = None,
    country_data: dict | None = None,
    subdivision_data: dict | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    custom_config: dict | None = None,
):
    """Create historical trend charts for a country or subdivision with optional comparison."""

    # Header row with title and time range selector
    header_col, btn_col = st.columns([5, 1])
    with header_col:
        st.markdown("### Historical Trends")
    with btn_col:
        num_periods = render_time_range_selector(chart_key)

    periods_to_load = available_periods[:num_periods]
    percentile = st.session_state.selected_percentile

    # Comparison selector in sidebar
    compare_country, compare_subdiv, compare_city, compare_name = None, None, None, None
    if start_date and end_date:
        compare_country, compare_subdiv, compare_city, compare_name = (
            render_comparison_sidebar(cache, start_date, end_date, chart_key)
        )

    # Load primary data
    df = load_historical_data(
        cache,
        country_code,
        periods_to_load,
        percentile,
        subdivision_code,
        custom_config,
    )

    if df.empty:
        st.warning(f"No historical data available for {entity_name}")
        return

    # Load comparison data if selected
    compare_df = None
    if compare_country:
        if compare_city:
            # City-level comparison
            compare_df = load_historical_data_city(
                cache,
                compare_country,
                compare_subdiv,
                compare_city,
                periods_to_load,
                percentile,
                custom_config,
            )
        elif compare_subdiv:
            # Subdivision-level comparison
            compare_df = load_historical_data(
                cache,
                compare_country,
                periods_to_load,
                percentile,
                compare_subdiv,
                custom_config,
            )
        else:
            # Country-level comparison
            compare_df = load_historical_data(
                cache,
                compare_country,
                periods_to_load,
                percentile,
                None,
                custom_config,
            )

        if compare_df is not None and compare_df.empty:
            st.info(f"No comparison data available for {compare_name}")
            compare_df = None

    # Caption
    caption = f"Showing {percentile} values • {len(df)} data points from {df['date'].min().date()} to {df['date'].max().date()}"
    if compare_df is not None:
        caption += f" • Comparing with {compare_name}"
    st.caption(caption)

    # Create and display chart
    fig = create_trend_figure(
        df, compare_df, compare_name if compare_df is not None else None, entity_name
    )
    st.plotly_chart(fig, width="stretch", key=chart_key)


# --- Map Creation ---
def create_world_map(
    country_data: dict, selected_country: str = None
) -> go.Figure | None:
    """Create a world map highlighting countries with data."""
    if not country_data:
        st.warning("No country data available for selected date")
        return None

    iso_codes = list(country_data.keys())
    hover_text = []
    for iso_a3 in iso_codes:
        data = country_data[iso_a3]
        name, score = data.get("name", iso_a3), data.get("score")
        sample_counts = data.get("sample_counts", {})

        text = f"<b>{name}</b><br>"
        if score is not None:
            text += f"Score: {score:.1f}<br>"
        if dl := sample_counts.get("downloads"):
            text += f"Download samples: {dl:,}<br>"
        if ul := sample_counts.get("uploads"):
            text += f"Upload samples: {ul:,}<br>"
        text += "Click to view details"
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

    if selected_country and (center := get_country_center(selected_country)):
        lat, lon, scale = center
        geo_settings["center"] = dict(lat=lat, lon=lon)
        geo_settings["projection_scale"] = scale

    fig.update_layout(
        geo=geo_settings, height=800, margin={"r": 0, "t": 0, "l": 0, "b": 0}
    )
    return fig


def _build_subdivision_map_data(
    geojson: dict,
    subdivision_data: dict[str, dict],
    percentile: str,
    custom_config: dict | None = None,
) -> tuple[list, list, list, list, dict]:
    """Build data arrays for subdivision choropleth map."""
    name_to_data = {
        data.get("subdivision_name", "").lower().strip(): data
        for data in subdivision_data.values()
        if data.get("subdivision_name")
    }

    locations, z_values, hover_texts, customdata = [], [], [], []

    for idx, feature in enumerate(geojson.get("features", [])):
        props = feature.get("properties", {})
        iso_code = props.get("iso_3166_2")
        region_code = props.get("region_cod")
        name = props.get("name") or props.get("NAME_1", "")
        location_id = iso_code if iso_code else f"feature_{idx}"

        # Match data by various keys
        matched_data = None
        if iso_code and iso_code in subdivision_data:
            matched_data = subdivision_data[iso_code]
        if not matched_data and region_code and region_code in subdivision_data:
            matched_data = subdivision_data[region_code]
        if not matched_data and name:
            matched_data = name_to_data.get(name.lower().strip())

        if matched_data:
            metrics = matched_data["metrics"]
            score = calculate_iqb_score_from_metrics(metrics, percentile, custom_config)
            locations.append(location_id)
            z_values.append(score if score is not None else 0)
            customdata.append(region_code or iso_code or location_id)

            hover_text = f"<b>{name}</b><br>"
            if (
                matched_data.get("subdivision_name")
                and matched_data["subdivision_name"] != name
            ):
                hover_text += f"Region: {matched_data['subdivision_name']}<br>"
            if score is not None:
                hover_text += f"IQB Score: {score:.3f}<br>"
            hover_text += f"Cities: {matched_data['city_count']}<br>"
            hover_text += f"Samples: {matched_data['sample_counts']['downloads']:,}"
            hover_texts.append(hover_text)
        else:
            locations.append(location_id)
            z_values.append(None)
            customdata.append(region_code or iso_code or location_id)
            hover_texts.append(f"<b>{name}</b><br>No data available")

    return locations, z_values, hover_texts, customdata, name_to_data


def create_subdivision_map(
    geojson: dict,
    subdivision_data: dict[str, dict],
    country_alpha_3: str,
    percentile: str = "p95",
    selected_subdivision: str | None = None,
    city_coords: dict[str, tuple[float, float]] | None = None,
    city_data: dict | None = None,
    custom_config: dict | None = None,
) -> go.Figure | None:
    """Create choropleth map of subdivisions, optionally with city overlay."""
    if not geojson or not subdivision_data:
        return None

    locations, z_values, hover_texts, customdata, _ = _build_subdivision_map_data(
        geojson, subdivision_data, percentile, custom_config
    )

    # Always show the choropleth layer
    fig = go.Figure(
        go.Choroplethmap(
            geojson=geojson,
            locations=locations,
            z=z_values,
            featureidkey="properties.iso_3166_2",
            colorscale="RdYlBu",
            zmin=0,
            zmax=1,
            marker_opacity=0.5
            if selected_subdivision
            else 0.7,  # Dimmer when showing cities
            marker_line_width=2 if selected_subdivision else 1,
            marker_line_color="white",
            text=hover_texts,
            customdata=customdata,
            hovertemplate="%{text}<extra></extra>",
            colorbar_title="IQB Score",
            showscale=not selected_subdivision,  # Hide colorbar when showing cities
        )
    )

    # Default map center
    center_lat, center_lon, zoom = 0, 0, 2
    if center := get_country_center(country_alpha_3):
        center_lat, center_lon, _ = center
        zoom = 3.5

    # Add city scatter layer if subdivision is selected
    if selected_subdivision and city_coords and city_data:
        city_name_to_data = {v.get("city", k): v for k, v in city_data.items()}
        lats, lons, texts, colors, sizes = [], [], [], [], []

        for city, (lat, lon) in city_coords.items():
            if data := city_name_to_data.get(city):
                score = calculate_iqb_score_from_metrics(
                    data["metrics"], percentile, custom_config
                )
                samples = data["sample_counts"].get("downloads", 0)
                lats.append(lat)
                lons.append(lon)
                colors.append(score if score else 0)
                # Scale marker size by sample count (min 10, max 25)
                size = min(25, max(10, 10 + (samples / 1000)))
                sizes.append(size)
                texts.append(
                    f"<b>{city}</b><br>IQB Score: {f'{score:.3f}' if score else 'N/A'}<br>Samples: {samples:,}"
                )

        if lats:
            fig.add_trace(
                go.Scattermap(
                    lat=lats,
                    lon=lons,
                    mode="markers",
                    marker=dict(
                        size=sizes,
                        color=colors,
                        colorscale="RdYlBu",
                        cmin=0,
                        cmax=1,
                        colorbar=dict(title="IQB Score", x=1.02),
                        opacity=0.9,
                    ),
                    text=texts,
                    hovertemplate="%{text}<extra></extra>",
                    name="Cities",
                    showlegend=False,
                )
            )
            # Center on cities
            center_lat = sum(lats) / len(lats)
            center_lon = sum(lons) / len(lons)
            # Calculate zoom based on spread
            lat_range = max(lats) - min(lats) if len(lats) > 1 else 1
            lon_range = max(lons) - min(lons) if len(lats) > 1 else 1
            spread = max(lat_range, lon_range)
            zoom = 7 if spread < 2 else 6 if spread < 5 else 5 if spread < 10 else 4

    fig.update_layout(
        map_style="carto-positron",
        map_zoom=zoom,
        map_center={"lat": center_lat, "lon": center_lon},
        height=800,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    return fig


# --- Main Application ---
cache = get_iqb_cache()

# Sidebar
with st.sidebar:
    st.header("Data Selection")
    manifest_files = tuple(cache.manager.remote_cache.manifest.files.keys())
    available_periods = get_available_periods(manifest_files)

    if not available_periods:
        st.error("No data periods available in manifest")
        st.stop()

    selected_period = st.selectbox(
        "Time Period",
        options=available_periods,
        format_func=lambda x: x[0],
        key="time_period_select",
    )
    START_DATE, END_DATE = selected_period[1], selected_period[2]

    st.session_state.selected_percentile = st.selectbox(
        "Percentile",
        options=PERCENTILES,
        index=7,
        help="Which percentile to display from the data",
        key="percentile_select",
    )

# Main content
with st.spinner("Loading data..."):
    country_data = load_country_data_for_date(
        cache,
        START_DATE,
        END_DATE,
        st.session_state.selected_percentile,
        custom_config,
    )

if not country_data:
    st.warning("No data available. Check that the IQB cache is populated.")
    with st.expander("🔍 Debug Info"):
        st.write(f"Cache data_dir: `{cache.data_dir}`")
        st.write(f"Start date: {START_DATE}")
        st.write(f"End date: {END_DATE}")
    st.stop()

# Navigation buttons
col1, col2, col3 = st.columns([1, 1, 4])
with col1:
    if st.session_state.selected_country:
        if st.button("← World Map"):
            st.session_state.selected_country = None
            st.session_state.selected_subdivision = None
            st.rerun()
percentile = st.session_state.selected_percentile

# Drill-down logic
if (
    st.session_state.selected_country
    and st.session_state.selected_country in country_data
):
    selected_data = country_data[st.session_state.selected_country]
    country_code = selected_data["code"].upper()
    country_name = selected_data["name"]
    iso_a3 = selected_data["iso_a3"]

    with st.spinner(f"Loading {country_name} data..."):
        subdivision_data = fetch_subdivision_data(
            cache, country_code, START_DATE, END_DATE
        )

    # LEVEL 3: City view
    if st.session_state.selected_subdivision:
        selected_code = st.session_state.selected_subdivision
        subdivision_info = subdivision_data.get(selected_code, {})
        subdivision_name = subdivision_info.get("subdivision_name", selected_code)

        st.subheader(f"{country_name} > {subdivision_name} - Cities")
        admin1_geojson = load_admin1_geojson(iso_a3)

        with st.spinner("Loading city data..."):
            city_data = fetch_city_data(cache, country_code, START_DATE, END_DATE)
            subdivision_iso = (
                selected_code.split("-")[-1] if "-" in selected_code else selected_code
            )
            filtered_city_data = {
                k: v
                for k, v in city_data.items()
                if v.get("subdivision") == subdivision_iso
            }

        city_coords = {}
        if filtered_city_data:
            city_names = [v.get("city") for v in filtered_city_data.values()]
            city_coords = lookup_city_coords(country_code, subdivision_name, city_names)

        # Build region code lookup for click handling
        feature_region_codes = (
            [
                f.get("properties", {}).get("region_cod")
                or f.get("properties", {}).get("iso_3166_2")
                for f in admin1_geojson.get("features", [])
            ]
            if admin1_geojson
            else []
        )

        fig = create_subdivision_map(
            admin1_geojson,
            subdivision_data,
            iso_a3,
            percentile,
            selected_code,
            city_coords,
            filtered_city_data,
            custom_config,
        )
        if fig:
            event = st.plotly_chart(
                fig, width="stretch", key="subdivision_city_map", on_select="rerun"
            )

            # Handle clicks on other subdivisions
            if event and event.selection and event.selection.points:
                clicked_point = event.selection.points[0]
                customdata = clicked_point.get("customdata")
                if (
                    customdata
                    and customdata in subdivision_data
                    and customdata != selected_code
                ):
                    st.session_state.selected_subdivision = customdata
                    st.rerun()

                point_index = clicked_point.get("pointIndex") or clicked_point.get(
                    "pointNumber"
                )
                if point_index is not None and point_index < len(feature_region_codes):
                    region_code = feature_region_codes[point_index]
                    if (
                        region_code
                        and region_code in subdivision_data
                        and region_code != selected_code
                    ):
                        st.session_state.selected_subdivision = region_code
                        st.rerun()

        st.markdown("---")
        metrics = subdivision_info.get("metrics", selected_data["metrics"])
        update_state_from_cache(state, metrics, percentile)

        col_sunburst, col_details = st.columns(2)

        with col_sunburst:
            st.subheader(f"{subdivision_name} - IQB Score")
            tab1, tab2, tab3 = st.tabs(["Requirements", "Use Cases", "Full Hierarchy"])
            try:
                iqb_data = build_iqb_data_from_cache(metrics, percentile)
                iqb_score = calculate_iqb_score_with_custom_settings(
                    state, data=iqb_data, print_details=False
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

        with col_details:
            st.subheader("Raw Metrics")
            table_data = {
                "Percentile": PERCENTILES,
                "Download (Mbps)": [
                    metrics["download_throughput_mbps"][p] for p in PERCENTILES
                ],
                "Upload (Mbps)": [
                    metrics["upload_throughput_mbps"][p] for p in PERCENTILES
                ],
                "Latency (ms)": [metrics["latency_ms"][p] for p in PERCENTILES],
                "Packet Loss (%)": [metrics["packet_loss"][p] for p in PERCENTILES],
            }
            df = pd.DataFrame(table_data)

            def highlight_selected(row):
                return (
                    ["background-color: #ffffcc"] * len(row)
                    if row["Percentile"] == percentile
                    else [""] * len(row)
                )

            st.dataframe(
                df.style.apply(highlight_selected, axis=1).format(
                    {
                        "Download (Mbps)": "{:.2f}",
                        "Upload (Mbps)": "{:.2f}",
                        "Latency (ms)": "{:.2f}",
                        "Packet Loss (%)": "{:.4f}",
                    }
                ),
                width="stretch",
                hide_index=True,
            )

        create_trend_charts(
            cache,
            country_code,
            subdivision_name,
            available_periods,
            "trend_subdivision",
            selected_code,
            country_data=country_data,
            subdivision_data=subdivision_data,
            start_date=START_DATE,
            end_date=END_DATE,
            custom_config=custom_config,
        )

    # LEVEL 2: Subdivision view
    else:
        admin1_geojson = load_admin1_geojson(iso_a3)

        if admin1_geojson and admin1_geojson.get("features") and subdivision_data:
            st.subheader(f"{country_name} - Subdivision View")

            feature_region_codes = [
                f.get("properties", {}).get("region_cod")
                or f.get("properties", {}).get("iso_3166_2")
                for f in admin1_geojson.get("features", [])
            ]

            fig = create_subdivision_map(
                admin1_geojson,
                subdivision_data,
                iso_a3,
                percentile,
                custom_config=custom_config,
            )
            if fig:
                event = st.plotly_chart(
                    fig, width="stretch", key="subdivision_map", on_select="rerun"
                )

                if event and event.selection and event.selection.points:
                    clicked_point = event.selection.points[0]
                    customdata = clicked_point.get("customdata")
                    if customdata and customdata in subdivision_data:
                        st.session_state.selected_subdivision = customdata
                        st.rerun()

                    point_index = clicked_point.get("pointIndex") or clicked_point.get(
                        "pointNumber"
                    )
                    if point_index is not None and point_index < len(
                        feature_region_codes
                    ):
                        region_code = feature_region_codes[point_index]
                        if region_code and region_code in subdivision_data:
                            st.session_state.selected_subdivision = region_code
                            st.rerun()
        else:
            st.info(
                f"No subdivision map available for {country_name}, showing country view"
            )
            fig = create_world_map(country_data, st.session_state.selected_country)
            if fig:
                st.plotly_chart(fig, width="stretch", key="world_map_zoomed")

        # Country details
        metrics = selected_data["metrics"]
        update_state_from_cache(state, metrics, percentile)

        st.markdown("---")
        col_sunburst, col_details = st.columns(2)

        with col_sunburst:
            st.subheader(f"{country_name} - IQB Score")
            tab1, tab2, tab3 = st.tabs(["Requirements", "Use Cases", "Full Hierarchy"])
            try:
                iqb_data = build_iqb_data_from_cache(metrics, percentile)
                iqb_score = calculate_iqb_score_with_custom_settings(
                    state, data=iqb_data, print_details=False
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

        with col_details:
            st.subheader("Raw Metrics")
            table_data = {
                "Percentile": PERCENTILES,
                "Download (Mbps)": [
                    metrics["download_throughput_mbps"][p] for p in PERCENTILES
                ],
                "Upload (Mbps)": [
                    metrics["upload_throughput_mbps"][p] for p in PERCENTILES
                ],
                "Latency (ms)": [metrics["latency_ms"][p] for p in PERCENTILES],
                "Packet Loss (%)": [metrics["packet_loss"][p] for p in PERCENTILES],
            }
            df = pd.DataFrame(table_data)

            def highlight_selected(row):
                return (
                    ["background-color: #ffffcc"] * len(row)
                    if row["Percentile"] == percentile
                    else [""] * len(row)
                )

            st.dataframe(
                df.style.apply(highlight_selected, axis=1).format(
                    {
                        "Download (Mbps)": "{:.2f}",
                        "Upload (Mbps)": "{:.2f}",
                        "Latency (ms)": "{:.2f}",
                        "Packet Loss (%)": "{:.4f}",
                    }
                ),
                width="stretch",
                hide_index=True,
            )

        st.markdown("---")
        create_trend_charts(
            cache,
            country_code,
            country_name,
            available_periods,
            country_data=country_data,
            subdivision_data=subdivision_data,
            start_date=START_DATE,
            end_date=END_DATE,
            custom_config=custom_config,
        )

# LEVEL 1: World map
else:
    fig = create_world_map(country_data, None)
    if fig:
        event = st.plotly_chart(
            fig, width="stretch", key="world_map", on_select="rerun"
        )
        if event and event.selection and event.selection.points:
            clicked_point = event.selection.points[0]
            if "customdata" in clicked_point:
                clicked_iso_a3 = clicked_point["customdata"]
                if clicked_iso_a3 in country_data:
                    st.session_state.selected_country = clicked_iso_a3
                    st.session_state.selected_subdivision = None
                    st.rerun()

    st.info("Click a country on the map to view subdivision details")

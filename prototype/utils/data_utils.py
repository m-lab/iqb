"""Utility functions for data extraction and conversion."""

from typing import Dict, List

import streamlit as st
from iqb import IQB_CONFIG
from utils.constants import RequirementType


@st.cache_data
def get_available_datasets() -> List[str]:
    """Extract list of datasets from config."""
    datasets = set()
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        for req_name, req_config in use_case_config["network requirements"].items():
            if "datasets" in req_config:
                datasets.update(req_config["datasets"].keys())
    return sorted(list(datasets))


@st.cache_data
def get_available_requirements() -> List[str]:
    """Extract list of network requirements from config."""
    requirements = set()
    for use_case_name, use_case_config in IQB_CONFIG["use cases"].items():
        requirements.update(use_case_config["network requirements"].keys())
    return sorted(list(requirements))


def identify_requirement_type(req_name: str) -> RequirementType:
    """Identify the type of a network requirement."""
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
    """Return requirements in display order: Download, Upload, Latency, Packet Loss."""
    order_map = {
        RequirementType.DOWNLOAD: 0,
        RequirementType.UPLOAD: 1,
        RequirementType.LATENCY: 2,
        RequirementType.PACKET_LOSS: 3,
    }

    def get_sort_key(req):
        req_type = identify_requirement_type(req)
        return order_map.get(req_type, 999)

    return sorted(requirements, key=get_sort_key)


def get_requirement_display_name(req_name: str) -> str:
    """Get display name for a requirement."""
    req_type = identify_requirement_type(req_name)
    display_map = {
        RequirementType.DOWNLOAD: "Download",
        RequirementType.UPLOAD: "Upload",
        RequirementType.LATENCY: "Latency",
        RequirementType.PACKET_LOSS: "Packet Loss",
    }
    return display_map.get(req_type, req_name.replace("_", " ").title())


def get_requirement_input_config(req_name: str) -> Dict:
    """Get input configuration for a requirement type."""
    from utils.constants import (
        LATENCY_STEP,
        MAX_LATENCY_MS,
        MAX_PACKET_LOSS_PCT,
        MAX_SPEED,
        MIN_LATENCY_MS,
        MIN_PACKET_LOSS_PCT,
        MIN_SPEED,
        PACKET_LOSS_STEP,
        SPEED_STEP,
    )

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


def convert_packet_loss_for_display(decimal_value: float) -> float:
    """Convert packet loss from decimal (0-1) to percentage (0-100)."""
    return decimal_value * 100


def convert_packet_loss_from_display(percentage_value: float) -> float:
    """Convert packet loss from percentage (0-100) to decimal (0-1)."""
    return percentage_value / 100


def get_default_value_for_requirement(req_name: str) -> float:
    """Get default value for a requirement type."""
    from utils.constants import (
        DEFAULT_DOWNLOAD_SPEED,
        DEFAULT_LATENCY,
        DEFAULT_PACKET_LOSS,
        DEFAULT_UPLOAD_SPEED,
    )

    req_type = identify_requirement_type(req_name)
    defaults = {
        RequirementType.DOWNLOAD: DEFAULT_DOWNLOAD_SPEED,
        RequirementType.UPLOAD: DEFAULT_UPLOAD_SPEED,
        RequirementType.LATENCY: DEFAULT_LATENCY,
        RequirementType.PACKET_LOSS: DEFAULT_PACKET_LOSS,
    }
    return defaults.get(req_type, 0.0)

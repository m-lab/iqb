"""Configuration constants for IQB application."""

from enum import Enum

# ============================================================================
# DEFAULT VALUES
# ============================================================================
DEFAULT_DOWNLOAD_SPEED = 15.0
DEFAULT_UPLOAD_SPEED = 20.0
DEFAULT_LATENCY = 75.0
DEFAULT_PACKET_LOSS = 0.007

# ============================================================================
# COLORS
# ============================================================================
COMPONENT_COLORS = {
    "Download": "#64C6CD",
    "Upload": "#78acdb",
    "Latency": "#9b92c6",
    "Packet Loss": "#da93bf",
}

USE_CASE_COLORS = {
    "web_browsing": "#FFB6C1",
    "video_streaming": "#FF69B4",
    "video_conferencing": "#FF1493",
    "cloud_gaming": "#C71585",
    "file_download": "#DB7093",
}

DATASET_COLORS = {
    "cloudflare": "#B8E6E8",
    "m-lab": "#A8D5BA",
    "ookla": "#F9D5A7",
}

# ============================================================================
# VISUALIZATION SETTINGS
# ============================================================================
SUNBURST_HEIGHT = 700
COMPONENT_FONT_SIZE = 20
DATASET_FONT_SIZE = 16
USE_CASE_FONT_SIZE = 18
REQUIREMENT_FONT_SIZE = 14
CENTER_FONT_SIZE = 22
ROOT_FONT_SIZE = 1
ZERO_WEIGHT_DISPLAY_RATIO = 0.05

# Ring width multipliers
RING_WIDTH_MULTIPLIER_LEVEL_1 = 1.0
RING_WIDTH_MULTIPLIER_LEVEL_2 = 1.5
RING_WIDTH_MULTIPLIER_LEVEL_3 = 2.0

# ============================================================================
# WEIGHT RANGES
# ============================================================================
MIN_REQUIREMENT_WEIGHT = 0
MAX_REQUIREMENT_WEIGHT = 5
MIN_USE_CASE_WEIGHT = 0.0
MAX_USE_CASE_WEIGHT = 1.0
MIN_DATASET_WEIGHT = 0.0
MAX_DATASET_WEIGHT = 1.0

# ============================================================================
# INPUT RANGES
# ============================================================================
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

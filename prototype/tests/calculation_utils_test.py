"""Tests for prototype calculation utilities."""

from session_state import initialize_app_state
from utils.calculation_utils import (
    build_data_for_calculate,
    calculate_iqb_score_with_custom_settings,
    get_config_with_custom_settings,
)


def test_get_config_with_custom_settings_applies_state_values():
    """Config builder should apply threshold and weight overrides from state."""
    state = initialize_app_state()

    state.thresholds["web browsing"]["download_throughput_mbps"] = 500
    state.requirement_weights["web browsing"]["latency_ms"] = 1
    state.use_case_weights["gaming"] = 2

    config = get_config_with_custom_settings(state)

    assert (
        config["use cases"]["web browsing"]["network requirements"][
            "download_throughput_mbps"
        ]["threshold min"]
        == 500
    )
    assert (
        config["use cases"]["web browsing"]["network requirements"]["latency_ms"]["w"] == 1
    )
    assert config["use cases"]["gaming"]["w"] == 2


def test_calculate_iqb_score_with_custom_settings_changes_result():
    """Applying strict thresholds in state should lower score for same input data."""
    state = initialize_app_state()
    data = build_data_for_calculate(state)

    baseline_score = calculate_iqb_score_with_custom_settings(state, data)

    for use_case in state.thresholds.values():
        use_case["download_throughput_mbps"] = 500
        use_case["upload_throughput_mbps"] = 500
        use_case["latency_ms"] = 5
        use_case["packet_loss"] = 0.001

    strict_score = calculate_iqb_score_with_custom_settings(state, data)

    assert strict_score < baseline_score

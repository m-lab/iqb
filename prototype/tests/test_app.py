"""Unit tests for IQB Streamlit application.

Run with: pytest test_app.py -v
"""

import pytest
from Home import (
    RequirementType,
    # Conversion functions
    convert_packet_loss_for_display,
    convert_packet_loss_from_display,
    # Default values
    get_default_value_for_requirement,
    get_ordered_requirements,
    get_requirement_display_name,
    # Config functions
    get_requirement_input_config,
    # Requirement identification
    identify_requirement_type,
)


class TestPacketLossConversion:
    """Tests for packet loss conversion functions."""

    def test_convert_to_display(self):
        """Test decimal to percentage conversion."""
        assert convert_packet_loss_for_display(0.0) == 0.0
        assert convert_packet_loss_for_display(0.5) == 50.0
        assert convert_packet_loss_for_display(1.0) == 100.0
        assert convert_packet_loss_for_display(0.007) == pytest.approx(0.7)

    def test_convert_from_display(self):
        """Test percentage to decimal conversion."""
        assert convert_packet_loss_from_display(0.0) == 0.0
        assert convert_packet_loss_from_display(50.0) == 0.5
        assert convert_packet_loss_from_display(100.0) == 1.0
        assert convert_packet_loss_from_display(0.7) == pytest.approx(0.007)

    def test_round_trip_conversion(self):
        """Test that converting back and forth preserves value."""
        test_values = [0.0, 0.001, 0.5, 0.999, 1.0]
        for value in test_values:
            result = convert_packet_loss_from_display(
                convert_packet_loss_for_display(value)
            )
            assert abs(result - value) < 1e-10


class TestRequirementTypeIdentification:
    """Tests for requirement type identification."""

    def test_identify_download(self):
        """Test download requirement identification."""
        assert identify_requirement_type("download_speed") == RequirementType.DOWNLOAD
        assert identify_requirement_type("DOWNLOAD_SPEED") == RequirementType.DOWNLOAD
        assert identify_requirement_type("min_download") == RequirementType.DOWNLOAD

    def test_identify_upload(self):
        """Test upload requirement identification."""
        assert identify_requirement_type("upload_speed") == RequirementType.UPLOAD
        assert identify_requirement_type("UPLOAD_SPEED") == RequirementType.UPLOAD
        assert identify_requirement_type("min_upload") == RequirementType.UPLOAD

    def test_identify_latency(self):
        """Test latency requirement identification."""
        assert identify_requirement_type("latency") == RequirementType.LATENCY
        assert identify_requirement_type("LATENCY") == RequirementType.LATENCY
        assert identify_requirement_type("max_latency") == RequirementType.LATENCY

    def test_identify_packet_loss(self):
        """Test packet loss requirement identification."""
        assert identify_requirement_type("packet_loss") == RequirementType.PACKET_LOSS
        assert identify_requirement_type("PACKET_LOSS") == RequirementType.PACKET_LOSS
        assert identify_requirement_type("loss_rate") == RequirementType.PACKET_LOSS

    def test_identify_unknown(self):
        """Test unknown requirement type."""
        assert identify_requirement_type("unknown_metric") is None
        assert identify_requirement_type("") is None


class TestRequirementDisplayNames:
    """Tests for requirement display name generation."""

    def test_display_names(self):
        """Test that display names are human-readable."""
        assert get_requirement_display_name("download_speed") == "Download"
        assert get_requirement_display_name("upload_speed") == "Upload"
        assert get_requirement_display_name("latency") == "Latency"
        assert get_requirement_display_name("packet_loss") == "Packet Loss"

    def test_unknown_requirement(self):
        """Test display name for unknown requirement."""
        result = get_requirement_display_name("custom_metric")
        assert "Custom" in result
        assert "Metric" in result


class TestRequirementOrdering:
    """Tests for requirement ordering."""

    def test_order_mixed_requirements(self):
        """Test that requirements are ordered correctly."""
        unordered = ["packet_loss", "latency", "download_speed", "upload_speed"]
        ordered = get_ordered_requirements(unordered)

        # Check order: Download, Upload, Latency, Packet Loss
        download_idx = ordered.index("download_speed")
        upload_idx = ordered.index("upload_speed")
        latency_idx = ordered.index("latency")
        packet_loss_idx = ordered.index("packet_loss")

        assert download_idx < upload_idx < latency_idx < packet_loss_idx

    def test_order_empty_list(self):
        """Test ordering empty list."""
        assert get_ordered_requirements([]) == []

    def test_order_single_requirement(self):
        """Test ordering single requirement."""
        assert get_ordered_requirements(["download_speed"]) == ["download_speed"]


class TestDefaultValues:
    """Tests for default value generation."""

    def test_download_default(self):
        """Test default download speed."""
        value = get_default_value_for_requirement("download_speed")
        assert value > 0
        assert isinstance(value, float)

    def test_upload_default(self):
        """Test default upload speed."""
        value = get_default_value_for_requirement("upload_speed")
        assert value > 0
        assert isinstance(value, float)

    def test_latency_default(self):
        """Test default latency."""
        value = get_default_value_for_requirement("latency")
        assert value > 0
        assert isinstance(value, float)

    def test_packet_loss_default(self):
        """Test default packet loss."""
        value = get_default_value_for_requirement("packet_loss")
        assert 0 <= value <= 1  # Should be decimal, not percentage
        assert isinstance(value, float)

    def test_unknown_default(self):
        """Test default for unknown requirement."""
        value = get_default_value_for_requirement("unknown_metric")
        assert value == 0.0


class TestInputConfiguration:
    """Tests for input configuration generation."""

    def test_download_config(self):
        """Test download input configuration."""
        config = get_requirement_input_config("download_speed")
        assert "Mbps" in config["label"]
        assert config["min_value"] >= 0
        assert config["max_value"] > config["min_value"]
        assert config["step"] > 0

    def test_upload_config(self):
        """Test upload input configuration."""
        config = get_requirement_input_config("upload_speed")
        assert "Mbps" in config["label"]
        assert config["min_value"] >= 0
        assert config["max_value"] > config["min_value"]

    def test_latency_config(self):
        """Test latency input configuration."""
        config = get_requirement_input_config("latency")
        assert "ms" in config["label"]
        assert config["min_value"] >= 0
        assert config["max_value"] > config["min_value"]

    def test_packet_loss_config(self):
        """Test packet loss input configuration."""
        config = get_requirement_input_config("packet_loss")
        assert "%" in config["label"]
        assert config["format"] is not None
        assert config["min_value"] == 0.0
        assert config["max_value"] == 100.0


class TestConfigFunctions:
    """Tests for configuration-related functions."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock IQB configuration."""
        return {
            "use cases": {
                "video_streaming": {
                    "w": 1.0,
                    "network requirements": {
                        "download_speed": {
                            "w": 3,
                            "threshold min": 5.0,
                            "datasets": {"cloudflare": {"w": 0.5}, "m-lab": {"w": 0.5}},
                        },
                        "latency": {
                            "w": 2,
                            "threshold min": 100.0,
                            "datasets": {"cloudflare": {"w": 1.0}},
                        },
                    },
                }
            }
        }

    def test_config_structure(self, mock_config):
        """Test that mock config has expected structure."""
        assert "use cases" in mock_config
        assert "video_streaming" in mock_config["use cases"]
        assert "network requirements" in mock_config["use cases"]["video_streaming"]


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_negative_packet_loss_display(self):
        """Test handling of negative values."""
        # This should work but produce negative percentage
        result = convert_packet_loss_for_display(-0.1)
        assert result == -10.0

    def test_large_packet_loss_values(self):
        """Test handling of values > 100%."""
        result = convert_packet_loss_for_display(2.0)
        assert result == 200.0

    def test_case_insensitive_requirement_matching(self):
        """Test that requirement matching is case-insensitive."""
        assert identify_requirement_type("DOWNLOAD") == RequirementType.DOWNLOAD
        assert identify_requirement_type("download") == RequirementType.DOWNLOAD
        assert identify_requirement_type("DoWnLoAd") == RequirementType.DOWNLOAD


class TestIntegration:
    """Integration tests combining multiple functions."""

    def test_full_requirement_processing_pipeline(self):
        """Test complete processing of a requirement."""
        req_name = "download_speed"

        # Identify type
        req_type = identify_requirement_type(req_name)
        assert req_type == RequirementType.DOWNLOAD

        # Get display name
        display_name = get_requirement_display_name(req_name)
        assert display_name == "Download"

        # Get default value
        default_value = get_default_value_for_requirement(req_name)
        assert default_value > 0

        # Get input config
        config = get_requirement_input_config(req_name)
        assert config["min_value"] <= default_value <= config["max_value"]

    def test_ordering_with_display_names(self):
        """Test that ordered requirements have correct display names."""
        requirements = [
            "packet_loss_rate",
            "min_latency",
            "download_mbps",
            "upload_mbps",
        ]

        ordered = get_ordered_requirements(requirements)
        display_names = [get_requirement_display_name(r) for r in ordered]

        # Verify order through display names
        assert display_names.index("Download") < display_names.index("Upload")
        assert display_names.index("Upload") < display_names.index("Latency")
        assert display_names.index("Latency") < display_names.index("Packet Loss")


# Pytest configuration
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

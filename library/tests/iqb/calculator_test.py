"""Tests for the IQBCalculator score calculation module."""

import copy

import pytest

from iqb import IQB_CONFIG, IQBCalculator


class TestIQBCalculatorInitialization:
    """Tests for IQBCalculator class initialization."""

    def test_init_with_name(self):
        """Test that IQBCalculator can be instantiated with a name."""
        iqb = IQBCalculator(name="test1")
        assert iqb.name == "test1"

    def test_init_without_name(self):
        """Test that IQBCalculator can be instantiated without a name."""
        iqb = IQBCalculator()
        assert iqb.name is None

    def test_init_uses_default_config(self):
        """Test that IQBCalculator uses default config when none provided."""
        iqb = IQBCalculator()
        assert iqb.config == IQB_CONFIG

    def test_init_accepts_dict_config(self):
        """Test that IQBCalculator accepts an in-memory config dict."""
        custom_config = copy.deepcopy(IQB_CONFIG)
        custom_config["use cases"]["web browsing"]["network requirements"][
            "download_throughput_mbps"
        ]["threshold min"] = 500

        iqb = IQBCalculator(config=custom_config)

        assert (
            iqb.config["use cases"]["web browsing"]["network requirements"][
                "download_throughput_mbps"
            ]["threshold min"]
            == 500
        )

    def test_init_with_invalid_config_raises_error(self):
        """Test that providing a non-existent config file raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            IQBCalculator(config="non_existing_file.json")


class TestBinaryRequirementScore:
    """Tests for binary requirement score calculation."""

    def test_download_throughput_above_threshold(self):
        """Test download throughput binary score when value exceeds threshold."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("download_throughput_mbps", 50, 25)
        assert score == 1

    def test_download_throughput_below_threshold(self):
        """Test download throughput binary score when value is below threshold."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("download_throughput_mbps", 20, 25)
        assert score == 0

    def test_upload_throughput_above_threshold(self):
        """Test upload throughput binary score when value exceeds threshold."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("upload_throughput_mbps", 30, 10)
        assert score == 1

    def test_upload_throughput_below_threshold(self):
        """Test upload throughput binary score when value is below threshold."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("upload_throughput_mbps", 5, 10)
        assert score == 0

    def test_latency_below_threshold(self):
        """Test latency binary score when value is below threshold (good)."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("latency_ms", 50, 100)
        assert score == 1

    def test_latency_above_threshold(self):
        """Test latency binary score when value exceeds threshold (bad)."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("latency_ms", 150, 100)
        assert score == 0

    def test_packet_loss_below_threshold(self):
        """Test packet loss binary score when value is below threshold (good)."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("packet_loss", 0.005, 0.01)
        assert score == 1

    def test_packet_loss_above_threshold(self):
        """Test packet loss binary score when value exceeds threshold (bad)."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("packet_loss", 0.02, 0.01)
        assert score == 0

    def test_invalid_network_requirement_raises_error(self):
        """Test that an invalid network requirement raises ValueError."""
        iqb = IQBCalculator()
        with pytest.raises(ValueError, match="not implemented for the network_requirement"):
            iqb.calculate_binary_requirement_score("invalid_requirement", 50, 25)


class TestIQBCalculatorScoreCalculation:
    """Tests for IQBCalculator score calculation."""

    def test_calculate_iqb_score_default_data(self):
        """Test that IQBCalculator score can be calculated with default data."""
        iqb = IQBCalculator(name="test1")
        score = iqb.calculate_iqb_score()
        # The score should be a float between 0 and 1
        assert isinstance(score, (int, float))
        assert 0 <= score <= 1

    def test_calculate_iqb_score_with_custom_data(self):
        """Test that passing custom data raises NotImplementedError."""
        iqb = IQBCalculator()
        sample_data = {
            "m-lab": {
                "download_throughput_mbps": 15,
                "upload_throughput_mbps": 20,
                "latency_ms": 75,
                "packet_loss": 0.007,
            }
        }
        score = iqb.calculate_iqb_score(data=sample_data)
        assert isinstance(score, (int, float))
        assert 0 <= score <= 1

    def test_calculate_iqb_score_print_details(self):
        """Test that IQBCalculator score calculation works with print_details=True."""
        iqb = IQBCalculator()
        score = iqb.calculate_iqb_score(print_details=True)
        assert isinstance(score, (int, float))
        assert 0 <= score <= 1

    def test_calculate_iqb_score_consistency(self):
        """Test that IQBCalculator score calculation is consistent across calls."""
        iqb = IQBCalculator()
        score1 = iqb.calculate_iqb_score()
        score2 = iqb.calculate_iqb_score()
        assert score1 == score2

    def test_calculate_iqb_score_changes_with_custom_thresholds(self):
        """Test that stricter thresholds lower the IQB score for the same data."""
        sample_data = {
            "m-lab": {
                "download_throughput_mbps": 15,
                "upload_throughput_mbps": 20,
                "latency_ms": 75,
                "packet_loss": 0.007,
            }
        }

        default_score = IQBCalculator().calculate_iqb_score(data=sample_data)

        strict_config = copy.deepcopy(IQB_CONFIG)
        for use_case in strict_config["use cases"].values():
            use_case["network requirements"]["download_throughput_mbps"]["threshold min"] = (
                500
            )
            use_case["network requirements"]["upload_throughput_mbps"]["threshold min"] = (
                500
            )
            use_case["network requirements"]["latency_ms"]["threshold min"] = 5
            use_case["network requirements"]["packet_loss"]["threshold min"] = 0.001

        strict_score = IQBCalculator(config=strict_config).calculate_iqb_score(
            data=sample_data
        )

        assert strict_score < default_score


class TestIQBCalculatorConfig:
    """Tests for IQBCalculator configuration."""

    def test_config_has_use_cases(self):
        """Test that config contains use cases."""
        iqb = IQBCalculator()
        assert "use cases" in iqb.config
        assert len(iqb.config["use cases"]) > 0

    def test_config_use_cases_have_network_requirements(self):
        """Test that each use case has network requirements."""
        iqb = IQBCalculator()
        for use_case in iqb.config["use cases"].values():
            assert "network requirements" in use_case
            assert len(use_case["network requirements"]) > 0

    def test_config_network_requirements_have_weights(self):
        """Test that each network requirement has weights."""
        iqb = IQBCalculator()
        for use_case in iqb.config["use cases"].values():
            for nr in use_case["network requirements"].values():
                assert "w" in nr
                assert isinstance(nr["w"], (int, float))

    def test_config_network_requirements_have_thresholds(self):
        """Test that each network requirement has thresholds."""
        iqb = IQBCalculator()
        for use_case in iqb.config["use cases"].values():
            for nr in use_case["network requirements"].values():
                assert "threshold min" in nr


class TestIQBCalculatorMethods:
    """Tests for IQBCalculator utility methods."""

    def test_print_config_runs_without_error(self, capsys):
        """Test that print_config method runs without errors."""
        iqb = IQBCalculator()
        iqb.print_config()
        captured = capsys.readouterr()
        assert "### IQB formula weights and thresholds" in captured.out
        assert "### Use cases" in captured.out
        assert "### Network requirements" in captured.out
        assert "### Weights & Thresholds" in captured.out

    def test_set_config_with_none(self):
        """Test that set_config works with None."""
        iqb = IQBCalculator()
        iqb.set_config(None)
        assert iqb.config == IQB_CONFIG

    def test_set_config_with_dict(self):
        """Test that set_config accepts a dict."""
        iqb = IQBCalculator()
        custom_config = copy.deepcopy(IQB_CONFIG)
        custom_config["use cases"]["gaming"]["w"] = 2

        iqb.set_config(custom_config)

        assert iqb.config["use cases"]["gaming"]["w"] == 2

    def test_set_config_with_file_raises_error(self):
        """Test that set_config raises error for file paths."""
        iqb = IQBCalculator()
        with pytest.raises(NotImplementedError):
            iqb.set_config("some_file.json")

    def test_set_config_with_invalid_type_raises_type_error(self):
        """Test that set_config rejects unsupported config types."""
        iqb = IQBCalculator()
        with pytest.raises(TypeError):
            iqb.set_config(123)

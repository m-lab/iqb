"""Tests for the IQB score calculation module."""

import pytest

from iqb import IQB, IQB_CONFIG


class TestIQBInitialization:
    """Tests for IQB class initialization."""

    def test_init_with_name(self):
        """Test that IQB can be instantiated with a name."""
        iqb = IQB(name="test1")
        assert iqb.name == "test1"

    def test_init_without_name(self):
        """Test that IQB can be instantiated without a name."""
        iqb = IQB()
        assert iqb.name is None

    def test_init_uses_default_config(self):
        """Test that IQB uses default config when none provided."""
        iqb = IQB()
        assert iqb.config == IQB_CONFIG

    def test_init_with_invalid_config_raises_error(self):
        """Test that providing a non-existent config file raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            IQB(config="non_existing_file.json")


class TestBinaryRequirementScore:
    """Tests for binary requirement score calculation."""

    def test_download_throughput_above_threshold(self):
        """Test download throughput binary score when value exceeds threshold."""
        iqb = IQB()
        score = iqb.calculate_binary_requirement_score("download throughput", 50, 25)
        assert score == 1

    def test_download_throughput_below_threshold(self):
        """Test download throughput binary score when value is below threshold."""
        iqb = IQB()
        score = iqb.calculate_binary_requirement_score("download throughput", 20, 25)
        assert score == 0

    def test_upload_throughput_above_threshold(self):
        """Test upload throughput binary score when value exceeds threshold."""
        iqb = IQB()
        score = iqb.calculate_binary_requirement_score("upload throughput", 30, 10)
        assert score == 1

    def test_upload_throughput_below_threshold(self):
        """Test upload throughput binary score when value is below threshold."""
        iqb = IQB()
        score = iqb.calculate_binary_requirement_score("upload throughput", 5, 10)
        assert score == 0

    def test_latency_below_threshold(self):
        """Test latency binary score when value is below threshold (good)."""
        iqb = IQB()
        score = iqb.calculate_binary_requirement_score("latency", 50, 100)
        assert score == 1

    def test_latency_above_threshold(self):
        """Test latency binary score when value exceeds threshold (bad)."""
        iqb = IQB()
        score = iqb.calculate_binary_requirement_score("latency", 150, 100)
        assert score == 0

    def test_packet_loss_below_threshold(self):
        """Test packet loss binary score when value is below threshold (good)."""
        iqb = IQB()
        score = iqb.calculate_binary_requirement_score("packet loss", 0.005, 0.01)
        assert score == 1

    def test_packet_loss_above_threshold(self):
        """Test packet loss binary score when value exceeds threshold (bad)."""
        iqb = IQB()
        score = iqb.calculate_binary_requirement_score("packet loss", 0.02, 0.01)
        assert score == 0

    def test_invalid_network_requirement_raises_error(self):
        """Test that an invalid network requirement raises ValueError."""
        iqb = IQB()
        with pytest.raises(ValueError, match="not implemented for the network_requirement"):
            iqb.calculate_binary_requirement_score("invalid_requirement", 50, 25)


class TestIQBScoreCalculation:
    """Tests for IQB score calculation."""

    def test_calculate_iqb_score_default_data(self):
        """Test that IQB score can be calculated with default data."""
        iqb = IQB(name="test1")
        score = iqb.calculate_iqb_score()
        # The score should be a float between 0 and 1
        assert isinstance(score, (int, float))
        assert 0 <= score <= 1

    def test_calculate_iqb_score_with_custom_data_raises_error(self):
        """Test that passing custom data raises NotImplementedError."""
        iqb = IQB()
        with pytest.raises(NotImplementedError):
            iqb.calculate_iqb_score(data={})

    def test_calculate_iqb_score_print_details(self):
        """Test that IQB score calculation works with print_details=True."""
        iqb = IQB()
        score = iqb.calculate_iqb_score(print_details=True)
        assert isinstance(score, (int, float))
        assert 0 <= score <= 1

    def test_calculate_iqb_score_consistency(self):
        """Test that IQB score calculation is consistent across calls."""
        iqb = IQB()
        score1 = iqb.calculate_iqb_score()
        score2 = iqb.calculate_iqb_score()
        assert score1 == score2


class TestIQBConfig:
    """Tests for IQB configuration."""

    def test_config_has_use_cases(self):
        """Test that config contains use cases."""
        iqb = IQB()
        assert "use cases" in iqb.config
        assert len(iqb.config["use cases"]) > 0

    def test_config_use_cases_have_network_requirements(self):
        """Test that each use case has network requirements."""
        iqb = IQB()
        for use_case in iqb.config["use cases"].values():
            assert "network requirements" in use_case
            assert len(use_case["network requirements"]) > 0

    def test_config_network_requirements_have_weights(self):
        """Test that each network requirement has weights."""
        iqb = IQB()
        for use_case in iqb.config["use cases"].values():
            for nr in use_case["network requirements"].values():
                assert "w" in nr
                assert isinstance(nr["w"], (int, float))

    def test_config_network_requirements_have_thresholds(self):
        """Test that each network requirement has thresholds."""
        iqb = IQB()
        for use_case in iqb.config["use cases"].values():
            for nr in use_case["network requirements"].values():
                assert "threshold min" in nr


class TestIQBMethods:
    """Tests for IQB utility methods."""

    def test_print_config_runs_without_error(self, capsys):
        """Test that print_config method runs without errors."""
        iqb = IQB()
        iqb.print_config()
        captured = capsys.readouterr()
        assert "### IQB formula weights and thresholds" in captured.out
        assert "### Use cases" in captured.out
        assert "### Network requirements" in captured.out
        assert "### Weights & Thresholds" in captured.out

    def test_set_config_with_none(self):
        """Test that set_config works with None."""
        iqb = IQB()
        iqb.set_config(None)
        assert iqb.config == IQB_CONFIG

    def test_set_config_with_file_raises_error(self):
        """Test that set_config raises error for file paths."""
        iqb = IQB()
        with pytest.raises(NotImplementedError):
            iqb.set_config("some_file.json")

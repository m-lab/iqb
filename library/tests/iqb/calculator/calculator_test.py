"""Tests for the IQBCalculator score calculation module."""

import json

import pytest

from iqb import IQB_CONFIG, IQB_DEFAULT_CONFIG, IQBCalculator, IQBData, IQBDataMLab


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
        assert iqb.config == IQB_DEFAULT_CONFIG

    def test_init_with_dict_config(self):
        """Test that IQBCalculator converts a dict config to IQBConfig."""
        iqb = IQBCalculator(config=IQB_CONFIG)
        assert iqb.config == IQB_DEFAULT_CONFIG

    def test_init_with_iqb_config(self):
        """Test that IQBCalculator accepts an IQBConfig dataclass directly."""
        iqb = IQBCalculator(config=IQB_DEFAULT_CONFIG)
        assert iqb.config is IQB_DEFAULT_CONFIG

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

    def test_download_throughput_at_threshold_scores_zero(self):
        """Download throughput exactly at threshold scores 0 (strict >)."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("download_throughput_mbps", 25, 25)
        assert score == 0

    def test_upload_throughput_at_threshold_scores_zero(self):
        """Upload throughput exactly at threshold scores 0 (strict >)."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("upload_throughput_mbps", 10, 10)
        assert score == 0

    def test_latency_at_threshold_scores_zero(self):
        """Latency exactly at threshold scores 0 (strict <)."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("latency_ms", 100, 100)
        assert score == 0

    def test_packet_loss_at_threshold_scores_zero(self):
        """Packet loss exactly at threshold scores 0 (strict <)."""
        iqb = IQBCalculator()
        score = iqb.calculate_binary_requirement_score("packet_loss", 0.01, 0.01)
        assert score == 0

    def test_invalid_network_requirement_raises_error(self):
        """Test that an invalid network requirement raises ValueError."""
        iqb = IQBCalculator()
        with pytest.raises(ValueError, match="not implemented for the network_requirement"):
            iqb.calculate_binary_requirement_score("invalid_requirement", 50, 25)


class TestIQBCalculatorScoreCalculation:
    """Tests for IQBCalculator score calculation."""

    _SAMPLE_DATA = {
        "m-lab": {
            "download_throughput_mbps": 15,
            "upload_throughput_mbps": 20,
            "latency_ms": 75,
            "packet_loss": 0.007,
        }
    }

    def test_calculate_iqb_score_with_custom_data(self):
        """Test that IQBCalculator score can be calculated with custom data."""
        iqb = IQBCalculator()
        score = iqb.calculate_iqb_score(data=self._SAMPLE_DATA)
        assert score == pytest.approx(4 / 7)

    def test_calculate_iqb_score_consistency(self):
        """Test that IQBCalculator score calculation is consistent across calls."""
        iqb = IQBCalculator()
        score1 = iqb.calculate_iqb_score(data=self._SAMPLE_DATA)
        score2 = iqb.calculate_iqb_score(data=self._SAMPLE_DATA)
        assert score1 == score2


class TestIQBCalculatorScoreExtremes:
    """Tests for IQB score at the extremes (all-pass and all-fail)."""

    def test_all_pass_scores_one(self):
        """Data exceeding every threshold across all use cases scores 1.0."""
        data = {
            "m-lab": {
                "download_throughput_mbps": 100,
                "upload_throughput_mbps": 100,
                "latency_ms": 1,
                "packet_loss": 0.001,
            }
        }
        iqb = IQBCalculator()
        score = iqb.calculate_iqb_score(data=data)
        assert score == pytest.approx(1.0)

    def test_all_fail_scores_zero(self):
        """Data failing every threshold across all use cases scores 0.0."""
        data = {
            "m-lab": {
                "download_throughput_mbps": 1,
                "upload_throughput_mbps": 1,
                "latency_ms": 200,
                "packet_loss": 0.1,
            }
        }
        iqb = IQBCalculator()
        score = iqb.calculate_iqb_score(data=data)
        assert score == pytest.approx(0.0)


class TestIQBCalculatorCustomConfig:
    """Tests using minimal custom configs to isolate the aggregation math."""

    _MINIMAL_CONFIG = {
        "use cases": {
            "test": {
                "w": 1,
                "network requirements": {
                    "download_throughput_mbps": {
                        "w": 1,
                        "threshold min": 10,
                        "datasets": {"m-lab": {"w": 1}},
                    },
                },
            },
        },
    }

    def test_minimal_config_pass(self):
        """Minimal config with passing data scores 1.0."""
        data = {"m-lab": {"download_throughput_mbps": 50}}
        iqb = IQBCalculator(config=self._MINIMAL_CONFIG)
        score = iqb.calculate_iqb_score(data=data)
        assert score == pytest.approx(1.0)

    def test_minimal_config_fail(self):
        """Minimal config with failing data scores 0.0."""
        data = {"m-lab": {"download_throughput_mbps": 5}}
        iqb = IQBCalculator(config=self._MINIMAL_CONFIG)
        score = iqb.calculate_iqb_score(data=data)
        assert score == pytest.approx(0.0)

    def test_weighted_average_across_requirements(self):
        """Two requirements with weights 3 and 1: pass/fail → 0.75."""
        config = {
            "use cases": {
                "test": {
                    "w": 1,
                    "network requirements": {
                        "download_throughput_mbps": {
                            "w": 3,
                            "threshold min": 10,
                            "datasets": {"m-lab": {"w": 1}},
                        },
                        "latency_ms": {
                            "w": 1,
                            "threshold min": 50,
                            "datasets": {"m-lab": {"w": 1}},
                        },
                    },
                },
            },
        }
        data = {"m-lab": {"download_throughput_mbps": 50, "latency_ms": 100}}
        iqb = IQBCalculator(config=config)
        score = iqb.calculate_iqb_score(data=data)
        assert score == pytest.approx(3 / 4)

    def test_weighted_average_across_use_cases(self):
        """Two use cases with weights 3 and 1: pass/fail → 0.75."""
        config = {
            "use cases": {
                "important": {
                    "w": 3,
                    "network requirements": {
                        "download_throughput_mbps": {
                            "w": 1,
                            "threshold min": 10,
                            "datasets": {"m-lab": {"w": 1}},
                        },
                    },
                },
                "minor": {
                    "w": 1,
                    "network requirements": {
                        "download_throughput_mbps": {
                            "w": 1,
                            "threshold min": 100,
                            "datasets": {"m-lab": {"w": 1}},
                        },
                    },
                },
            },
        }
        data = {"m-lab": {"download_throughput_mbps": 50}}
        iqb = IQBCalculator(config=config)
        score = iqb.calculate_iqb_score(data=data)
        assert score == pytest.approx(3 / 4)


class TestIQBCalculatorDatasetAgreement:
    """Tests for the dataset agreement score (averaging across datasets)."""

    def test_two_datasets_one_pass_one_fail(self):
        """Two datasets both with weight=1: one passes, one fails → score 0.5."""
        config = {
            "use cases": {
                "test": {
                    "w": 1,
                    "network requirements": {
                        "download_throughput_mbps": {
                            "w": 1,
                            "threshold min": 10,
                            "datasets": {"m-lab": {"w": 1}, "ookla": {"w": 1}},
                        },
                    },
                },
            },
        }
        data = {
            "m-lab": {"download_throughput_mbps": 50},
            "ookla": {"download_throughput_mbps": 5},
        }
        iqb = IQBCalculator(config=config)
        score = iqb.calculate_iqb_score(data=data)
        assert score == pytest.approx(0.5)

    def test_absent_dataset_is_skipped_not_counted_as_failure(self):
        """A dataset in config but absent from data is excluded, not scored 0."""
        config = {
            "use cases": {
                "test": {
                    "w": 1,
                    "network requirements": {
                        "download_throughput_mbps": {
                            "w": 1,
                            "threshold min": 10,
                            "datasets": {"m-lab": {"w": 1}, "ookla": {"w": 1}},
                        },
                    },
                },
            },
        }
        data = {"m-lab": {"download_throughput_mbps": 50}}
        iqb = IQBCalculator(config=config)
        score = iqb.calculate_iqb_score(data=data)
        assert score == pytest.approx(1.0)

    def test_zero_weight_dataset_is_skipped(self):
        """A dataset with weight=0 is not counted in the agreement average."""
        config = {
            "use cases": {
                "test": {
                    "w": 1,
                    "network requirements": {
                        "download_throughput_mbps": {
                            "w": 1,
                            "threshold min": 10,
                            "datasets": {"m-lab": {"w": 1}, "ookla": {"w": 0}},
                        },
                    },
                },
            },
        }
        data = {
            "m-lab": {"download_throughput_mbps": 50},
            "ookla": {"download_throughput_mbps": 5},
        }
        iqb = IQBCalculator(config=config)
        score = iqb.calculate_iqb_score(data=data)
        assert score == pytest.approx(1.0)


class TestCalculateIQBScoreWithIQBData:
    """Tests for calculate_iqb_score accepting IQBData."""

    _SAMPLE_MLAB = IQBDataMLab(download=15, upload=20, latency=75, loss=0.007)
    _SAMPLE_DICT = {
        "m-lab": {
            "download_throughput_mbps": 15,
            "upload_throughput_mbps": 20,
            "latency_ms": 75,
            "packet_loss": 0.007,
        }
    }

    def test_iqb_data_produces_same_score_as_dict(self):
        """IQBData input produces the same score as the equivalent dict."""
        iqb = IQBCalculator()
        dict_score = iqb.calculate_iqb_score(data=self._SAMPLE_DICT)
        data_score = iqb.calculate_iqb_score(data=IQBData(mlab=self._SAMPLE_MLAB))
        assert data_score == dict_score

    def test_iqb_data_to_dict_only_contains_present_datasets(self):
        """IQBData.to_dict only includes datasets that have data."""
        data = IQBData(mlab=self._SAMPLE_MLAB)
        d = data.to_dict()
        assert "m-lab" in d
        assert "cloudflare" not in d
        assert "ookla" not in d


class TestIQBCalculatorConfig:
    """Tests for IQBCalculator configuration."""

    def test_config_has_use_cases(self):
        """Test that config contains use cases."""
        iqb = IQBCalculator()
        assert len(iqb.config.use_cases) > 0

    def test_config_use_cases_have_network_requirements(self):
        """Test that each use case has at least one network requirement."""
        iqb = IQBCalculator()
        for use_case in iqb.config.use_cases.values():
            nrs = use_case.network_requirements
            nr_names = (
                "download_throughput_mbps",
                "upload_throughput_mbps",
                "latency_ms",
                "packet_loss",
            )
            has_any = any(
                getattr(nrs, f) is not None
                for f in nr_names
            )
            assert has_any

    def test_config_network_requirements_have_weights(self):
        """Test that each network requirement has weights."""
        nr_names = (
            "download_throughput_mbps",
            "upload_throughput_mbps",
            "latency_ms",
            "packet_loss",
        )
        iqb = IQBCalculator()
        for use_case in iqb.config.use_cases.values():
            nrs = use_case.network_requirements
            for f in nr_names:
                nr = getattr(nrs, f)
                if nr is not None:
                    assert isinstance(nr.weight, (int, float))

    def test_config_network_requirements_have_thresholds(self):
        """Test that each network requirement has thresholds."""
        nr_names = (
            "download_throughput_mbps",
            "upload_throughput_mbps",
            "latency_ms",
            "packet_loss",
        )
        iqb = IQBCalculator()
        for use_case in iqb.config.use_cases.values():
            nrs = use_case.network_requirements
            for f in nr_names:
                nr = getattr(nrs, f)
                if nr is not None:
                    assert isinstance(nr.threshold_min, (int, float))


class TestIQBCalculatorMethods:
    """Tests for IQBCalculator utility methods."""

    def test_print_config_outputs_valid_json(self, capsys):
        """Test that print_config outputs valid JSON matching the config."""
        iqb = IQBCalculator()
        iqb.print_config()
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert "use_cases" in parsed
        assert "web browsing" in parsed["use_cases"]
        assert "gaming" in parsed["use_cases"]

    def test_print_config_uses_instance_config(self, capsys):
        """Test that print_config prints the instance's config, not the global."""
        config = {
            "use cases": {
                "test": {
                    "w": 1,
                    "network requirements": {
                        "download_throughput_mbps": {
                            "w": 1,
                            "threshold min": 10,
                            "datasets": {"m-lab": {"w": 1}},
                        },
                    },
                },
            },
        }
        iqb = IQBCalculator(config=config)
        iqb.print_config()
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert list(parsed["use_cases"].keys()) == ["test"]

    def test_set_config_with_none(self):
        """Test that set_config works with None."""
        iqb = IQBCalculator()
        iqb.set_config(None)
        assert iqb.config == IQB_DEFAULT_CONFIG

    def test_set_config_with_file_raises_error(self):
        """Test that set_config raises error for file paths."""
        iqb = IQBCalculator()
        with pytest.raises(NotImplementedError):
            iqb.set_config("some_file.json")

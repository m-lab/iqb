"""Unit tests for app_state module.

Run with: pytest test_app_state.py -v
"""

import pytest
from app_state import IQBAppState


class TestIQBAppStateInitialization:
    """Tests for IQBAppState initialization."""

    def test_default_initialization(self):
        """Test that state initializes with empty collections."""
        state = IQBAppState()

        assert state.manual_entry == {}
        assert state.thresholds == {}
        assert state.requirement_weights == {}
        assert state.use_case_weights == {}
        assert state.dataset_weights == {}
        assert state.dataset_exists_in_config == {}
        assert state.reset_counter == 0
        assert state.iqb is not None

    def test_multiple_instances_are_independent(self):
        """Test that multiple state instances don't share data."""
        state1 = IQBAppState()
        state2 = IQBAppState()

        state1.manual_entry["test"] = {"value": 1.0}

        assert "test" in state1.manual_entry
        assert "test" not in state2.manual_entry


class TestResetManualEntry:
    """Tests for reset_manual_entry method."""

    def test_reset_manual_entry(self):
        """Test that manual entry is reset to defaults."""
        state = IQBAppState()

        # Mock data
        datasets = ["cloudflare", "m-lab"]
        requirements = ["download_speed", "upload_speed"]

        def get_default(req):
            return 10.0 if "download" in req else 5.0

        # Reset
        state.reset_manual_entry(datasets, requirements, get_default)

        # Verify structure
        assert len(state.manual_entry) == 2
        assert "cloudflare" in state.manual_entry
        assert "m-lab" in state.manual_entry

        # Verify values
        assert state.manual_entry["cloudflare"]["download_speed"] == 10.0
        assert state.manual_entry["cloudflare"]["upload_speed"] == 5.0
        assert state.manual_entry["m-lab"]["download_speed"] == 10.0
        assert state.manual_entry["m-lab"]["upload_speed"] == 5.0

    def test_reset_increments_counter(self):
        """Test that reset increments the counter."""
        state = IQBAppState()
        initial_counter = state.reset_counter

        state.reset_manual_entry([], [], lambda x: 0.0)

        assert state.reset_counter == initial_counter + 1

    def test_reset_overwrites_existing_data(self):
        """Test that reset overwrites existing manual entry data."""
        state = IQBAppState()

        # Set initial data
        state.manual_entry = {"old_dataset": {"old_req": 999.0}}

        # Reset with new data
        datasets = ["new_dataset"]
        requirements = ["new_req"]
        state.reset_manual_entry(datasets, requirements, lambda x: 1.0)

        # Verify old data is gone
        assert "old_dataset" not in state.manual_entry
        assert "new_dataset" in state.manual_entry
        assert state.manual_entry["new_dataset"]["new_req"] == 1.0


class TestResetThresholds:
    """Tests for reset_thresholds method."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock IQB configuration."""
        return {
            "use cases": {
                "video_streaming": {
                    "network requirements": {
                        "download_speed": {"threshold min": 5.0},
                        "latency": {"threshold min": 100.0},
                    }
                },
                "web_browsing": {
                    "network requirements": {"download_speed": {"threshold min": 1.0}}
                },
            }
        }

    def test_reset_thresholds(self, mock_config):
        """Test that thresholds are reset from config."""
        state = IQBAppState()
        state.reset_thresholds(mock_config)

        # Verify structure
        assert "video_streaming" in state.thresholds
        assert "web_browsing" in state.thresholds

        # Verify values
        assert state.thresholds["video_streaming"]["download_speed"] == 5.0
        assert state.thresholds["video_streaming"]["latency"] == 100.0
        assert state.thresholds["web_browsing"]["download_speed"] == 1.0

    def test_reset_thresholds_increments_counter(self, mock_config):
        """Test that reset increments counter."""
        state = IQBAppState()
        initial_counter = state.reset_counter

        state.reset_thresholds(mock_config)

        assert state.reset_counter == initial_counter + 1

    def test_reset_thresholds_handles_missing_threshold(self, mock_config):
        """Test handling of requirements without threshold min."""
        config = {
            "use cases": {
                "test": {
                    "network requirements": {
                        "req_without_threshold": {"some_other_field": "value"}
                    }
                }
            }
        }

        state = IQBAppState()
        state.reset_thresholds(config)

        # Should not include requirement without threshold
        assert "req_without_threshold" not in state.thresholds.get("test", {})


class TestResetRequirementWeights:
    """Tests for reset_requirement_weights method."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock IQB configuration."""
        return {
            "use cases": {
                "video_streaming": {
                    "network requirements": {
                        "download_speed": {"w": 3},
                        "upload_speed": {"w": 1},
                        "latency": {},  # Missing weight
                    }
                }
            }
        }

    def test_reset_requirement_weights(self, mock_config):
        """Test that requirement weights are reset from config."""
        state = IQBAppState()
        state.reset_requirement_weights(mock_config)

        assert state.requirement_weights["video_streaming"]["download_speed"] == 3
        assert state.requirement_weights["video_streaming"]["upload_speed"] == 1
        assert state.requirement_weights["video_streaming"]["latency"] == 1.0  # Default

    def test_reset_requirement_weights_increments_counter(self, mock_config):
        """Test that reset increments counter."""
        state = IQBAppState()
        initial_counter = state.reset_counter

        state.reset_requirement_weights(mock_config)

        assert state.reset_counter == initial_counter + 1


class TestResetUseCaseWeights:
    """Tests for reset_use_case_weights method."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock IQB configuration."""
        return {
            "use cases": {
                "video_streaming": {"w": 0.8, "network requirements": {}},
                "web_browsing": {"w": 0.6, "network requirements": {}},
                "gaming": {"network requirements": {}},  # Missing weight
            }
        }

    def test_reset_use_case_weights(self, mock_config):
        """Test that use case weights are reset from config."""
        state = IQBAppState()
        state.reset_use_case_weights(mock_config)

        assert state.use_case_weights["video_streaming"] == 0.8
        assert state.use_case_weights["web_browsing"] == 0.6
        assert state.use_case_weights["gaming"] == 1.0  # Default

    def test_reset_use_case_weights_increments_counter(self, mock_config):
        """Test that reset increments counter."""
        state = IQBAppState()
        initial_counter = state.reset_counter

        state.reset_use_case_weights(mock_config)

        assert state.reset_counter == initial_counter + 1


class TestResetDatasetWeights:
    """Tests for reset_dataset_weights method."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock IQB configuration."""
        return {
            "use cases": {
                "video_streaming": {
                    "network requirements": {
                        "download_speed": {
                            "datasets": {"cloudflare": {"w": 0.5}, "m-lab": {"w": 0.5}}
                        },
                        "upload_speed": {"datasets": {"ookla": {"w": 1.0}}},
                    }
                }
            }
        }

    def test_reset_dataset_weights(self, mock_config):
        """Test that dataset weights are reset from config."""
        state = IQBAppState()
        datasets = ["cloudflare", "m-lab", "ookla"]

        state.reset_dataset_weights(mock_config, datasets)

        # Verify download_speed datasets
        assert state.dataset_weights["download_speed"]["cloudflare"] == 0.5
        assert state.dataset_weights["download_speed"]["m-lab"] == 0.5
        assert state.dataset_weights["download_speed"]["ookla"] == 0.0  # Not in config

        # Verify upload_speed datasets
        assert state.dataset_weights["upload_speed"]["ookla"] == 1.0
        assert state.dataset_weights["upload_speed"]["cloudflare"] == 0.0
        assert state.dataset_weights["upload_speed"]["m-lab"] == 0.0

    def test_reset_dataset_exists_flags(self, mock_config):
        """Test that dataset_exists_in_config flags are set correctly."""
        state = IQBAppState()
        datasets = ["cloudflare", "m-lab", "ookla"]

        state.reset_dataset_weights(mock_config, datasets)

        # Verify existence flags for download_speed
        assert state.dataset_exists_in_config["download_speed"]["cloudflare"] is True
        assert state.dataset_exists_in_config["download_speed"]["m-lab"] is True
        assert state.dataset_exists_in_config["download_speed"]["ookla"] is False

        # Verify existence flags for upload_speed
        assert state.dataset_exists_in_config["upload_speed"]["ookla"] is True
        assert state.dataset_exists_in_config["upload_speed"]["cloudflare"] is False

    def test_reset_dataset_weights_increments_counter(self, mock_config):
        """Test that reset increments counter."""
        state = IQBAppState()
        initial_counter = state.reset_counter

        state.reset_dataset_weights(mock_config, [])

        assert state.reset_counter == initial_counter + 1

    def test_handles_requirement_without_datasets(self):
        """Test handling requirements without dataset configuration."""
        config = {
            "use cases": {
                "test": {
                    "network requirements": {
                        "latency": {}  # No datasets defined
                    }
                }
            }
        }

        state = IQBAppState()
        datasets = ["cloudflare"]

        state.reset_dataset_weights(config, datasets)

        # Should mark as not existing
        assert state.dataset_weights["latency"]["cloudflare"] == 0.0
        assert state.dataset_exists_in_config["latency"]["cloudflare"] is False


class TestStateIntegration:
    """Integration tests for the state management."""

    def test_multiple_resets(self):
        """Test that multiple resets work correctly."""
        state = IQBAppState()

        config = {
            "use cases": {
                "test": {
                    "w": 0.5,
                    "network requirements": {
                        "download_speed": {"w": 2, "threshold min": 10.0}
                    },
                }
            }
        }

        initial_counter = state.reset_counter

        state.reset_thresholds(config)
        state.reset_requirement_weights(config)
        state.reset_use_case_weights(config)

        assert state.reset_counter == initial_counter + 3
        assert state.thresholds["test"]["download_speed"] == 10.0
        assert state.requirement_weights["test"]["download_speed"] == 2
        assert state.use_case_weights["test"] == 0.5

    def test_state_independence_after_reset(self):
        """Test that state is properly isolated after resets."""
        state = IQBAppState()

        # Set some manual data
        state.manual_entry = {"dataset1": {"req1": 100.0}}

        # Reset other fields
        config = {"use cases": {"test": {"network requirements": {}}}}
        state.reset_thresholds(config)

        # Manual entry should be unaffected
        assert state.manual_entry == {"dataset1": {"req1": 100.0}}


# Pytest configuration
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

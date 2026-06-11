"""Tests for the IQB configuration dataclasses and legacy loading."""

import pytest

from iqb import (
    IQB_CONFIG,
    IQB_DEFAULT_CONFIG,
    IQBConfig,
    IQBConfigDatasetWeights,
    IQBConfigNetworkRequirements,
    IQBConfigNetworkRequirementSpeed,
    IQBConfigUseCase,
    iqb_config_from_legacy,
)


class TestIQBConfigFromLegacyPreservesAllData:
    """Verify that iqb_config_from_legacy does not lose any information."""

    def test_all_use_cases_present(self):
        """Every use case in the legacy dict appears in the dataclass."""
        legacy_ucs = set(IQB_CONFIG["use cases"].keys())
        config_ucs = set(IQB_DEFAULT_CONFIG.use_cases.keys())
        assert config_ucs == legacy_ucs

    def test_all_network_requirements_present(self):
        """Every network requirement in each legacy use case appears in the dataclass."""
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            nrs = IQB_DEFAULT_CONFIG.use_cases[uc_name].network_requirements
            for nr_name in uc_dict["network requirements"]:
                assert getattr(nrs, nr_name) is not None, (
                    f"missing {nr_name!r} in use case {uc_name!r}"
                )

    def test_all_datasets_present(self):
        """Every dataset in each legacy network requirement appears in the dataclass."""
        ds_name_to_field = {
            "m-lab": "mlab",
            "cloudflare": "cloudflare",
            "ookla": "ookla",
        }
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            for nr_name, nr_dict in uc_dict["network requirements"].items():
                nrs = IQB_DEFAULT_CONFIG.use_cases[uc_name].network_requirements
                nr_cfg = getattr(nrs, nr_name)
                for ds_name, ds_dict in nr_dict["datasets"].items():
                    actual = getattr(nr_cfg.dataset_weights, ds_name_to_field[ds_name])
                    assert actual == ds_dict["w"], (
                        f"weight mismatch in {uc_name!r}/{nr_name!r}/{ds_name!r}"
                    )

    def test_use_case_weights_match(self):
        """Use case weights match the legacy dict values."""
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            assert IQB_DEFAULT_CONFIG.use_cases[uc_name].weight == uc_dict["w"]

    _DS_NAME_TO_FIELD: dict[str, str] = {
        "m-lab": "mlab",
        "cloudflare": "cloudflare",
        "ookla": "ookla",
    }

    def test_network_requirement_weights_match(self):
        """Network requirement weights match the legacy dict values."""
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            nrs = IQB_DEFAULT_CONFIG.use_cases[uc_name].network_requirements
            for nr_name, nr_dict in uc_dict["network requirements"].items():
                nr = getattr(nrs, nr_name)
                assert nr.weight == nr_dict["w"], f"weight mismatch in {uc_name!r}/{nr_name!r}"

    def test_network_requirement_thresholds_match(self):
        """Network requirement threshold_min values match the legacy dict values."""
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            nrs = IQB_DEFAULT_CONFIG.use_cases[uc_name].network_requirements
            for nr_name, nr_dict in uc_dict["network requirements"].items():
                nr = getattr(nrs, nr_name)
                assert nr.threshold_min == nr_dict["threshold min"], (
                    f"threshold mismatch in {uc_name!r}/{nr_name!r}"
                )

    def test_dataset_weights_match(self):
        """Dataset weights match the legacy dict values."""
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            nrs = IQB_DEFAULT_CONFIG.use_cases[uc_name].network_requirements
            for nr_name, nr_dict in uc_dict["network requirements"].items():
                nr = getattr(nrs, nr_name)
                for ds_name, ds_dict in nr_dict["datasets"].items():
                    actual = getattr(nr.dataset_weights, self._DS_NAME_TO_FIELD[ds_name])
                    assert actual == ds_dict["w"], (
                        f"weight mismatch in {uc_name!r}/{nr_name!r}/{ds_name!r}"
                    )


class TestIQBDefaultConfigIsDerivedFromLegacy:
    """Verify that IQB_DEFAULT_CONFIG is the same object as iqb_config_from_legacy(IQB_CONFIG)."""

    def test_default_equals_from_legacy(self):
        """IQB_DEFAULT_CONFIG equals a fresh conversion of IQB_CONFIG."""
        fresh = iqb_config_from_legacy(IQB_CONFIG)
        assert fresh == IQB_DEFAULT_CONFIG

    def test_idempotent_conversion(self):
        """Converting IQB_CONFIG twice yields equal results."""
        first = iqb_config_from_legacy(IQB_CONFIG)
        second = iqb_config_from_legacy(IQB_CONFIG)
        assert first == second


class TestIQBConfigDataclasses:
    """Tests for the dataclass types themselves."""

    def test_iqb_config_is_frozen(self):
        """IQBConfig instances are immutable."""
        with pytest.raises(AttributeError):
            IQB_DEFAULT_CONFIG.use_cases = {}  # type: ignore[misc]

    def test_iqb_config_use_case_is_frozen(self):
        """IQBConfigUseCase instances are immutable."""
        uc = next(iter(IQB_DEFAULT_CONFIG.use_cases.values()))
        with pytest.raises(AttributeError):
            uc.weight = 99  # type: ignore[misc]

    def test_iqb_config_network_requirement_speed_is_frozen(self):
        """IQBConfigNetworkRequirementSpeed instances are immutable."""
        uc = next(iter(IQB_DEFAULT_CONFIG.use_cases.values()))
        nr = uc.network_requirements.download_throughput_mbps
        assert nr is not None
        with pytest.raises(AttributeError):
            nr.weight = 99  # type: ignore[misc]

    def test_iqb_config_dataset_weights_is_frozen(self):
        """IQBConfigDatasetWeights instances are immutable."""
        uc = next(iter(IQB_DEFAULT_CONFIG.use_cases.values()))
        nr = uc.network_requirements.download_throughput_mbps
        assert nr is not None
        with pytest.raises(AttributeError):
            nr.dataset_weights.mlab = 99  # type: ignore[misc]

    def test_construct_programmatically(self):
        """Verify that configs can be built manually without the legacy loader."""
        config = IQBConfig(
            use_cases={
                "test": IQBConfigUseCase(
                    weight=1.0,
                    network_requirements=IQBConfigNetworkRequirements(
                        download_throughput_mbps=IQBConfigNetworkRequirementSpeed(
                            weight=3.0,
                            threshold_min=10.0,
                            dataset_weights=IQBConfigDatasetWeights(mlab=1.0),
                        ),
                    ),
                ),
            },
        )
        assert config.use_cases["test"].weight == 1.0
        nr = config.use_cases["test"].network_requirements.download_throughput_mbps
        assert nr is not None
        assert nr.weight == 3.0
        assert nr.threshold_min == 10.0
        assert nr.dataset_weights.mlab == 1.0


class TestIQBConfigFromLegacyErrors:
    """Tests for error handling in iqb_config_from_legacy."""

    def test_missing_use_cases_key(self):
        """Raises KeyError when 'use cases' key is missing."""
        with pytest.raises(KeyError):
            iqb_config_from_legacy({})

    def test_missing_network_requirements_key(self):
        """Raises KeyError when 'network requirements' key is missing."""
        with pytest.raises(KeyError):
            iqb_config_from_legacy({"use cases": {"test": {"w": 1}}})

    def test_missing_threshold_min_key(self):
        """Raises KeyError when 'threshold min' key is missing."""
        with pytest.raises(KeyError):
            iqb_config_from_legacy(
                {
                    "use cases": {
                        "test": {
                            "w": 1,
                            "network requirements": {
                                "download_throughput_mbps": {
                                    "w": 3,
                                    "datasets": {"m-lab": {"w": 1}},
                                }
                            },
                        }
                    }
                }
            )

    def test_missing_datasets_key(self):
        """Raises KeyError when 'datasets' key is missing."""
        with pytest.raises(KeyError):
            iqb_config_from_legacy(
                {
                    "use cases": {
                        "test": {
                            "w": 1,
                            "network requirements": {
                                "download_throughput_mbps": {
                                    "w": 3,
                                    "threshold min": 10,
                                }
                            },
                        }
                    }
                }
            )

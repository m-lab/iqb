"""Tests for the IQB configuration dataclasses and legacy loading."""

import pytest

from iqb import (
    IQB_CONFIG,
    IQB_DEFAULT_CONFIG,
    IQBConfig,
    IQBConfigDataset,
    IQBConfigNetworkRequirement,
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
            legacy_nrs = set(uc_dict["network requirements"].keys())
            config_nrs = set(IQB_DEFAULT_CONFIG.use_cases[uc_name].network_requirements.keys())
            assert config_nrs == legacy_nrs, f"mismatch in use case {uc_name!r}"

    def test_all_datasets_present(self):
        """Every dataset in each legacy network requirement appears in the dataclass."""
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            for nr_name, nr_dict in uc_dict["network requirements"].items():
                legacy_ds = set(nr_dict["datasets"].keys())
                config_ds = set(
                    IQB_DEFAULT_CONFIG.use_cases[uc_name]
                    .network_requirements[nr_name]
                    .datasets.keys()
                )
                assert config_ds == legacy_ds, f"mismatch in {uc_name!r}/{nr_name!r}"

    def test_use_case_weights_match(self):
        """Use case weights match the legacy dict values."""
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            assert IQB_DEFAULT_CONFIG.use_cases[uc_name].weight == uc_dict["w"]

    def test_network_requirement_weights_match(self):
        """Network requirement weights match the legacy dict values."""
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            for nr_name, nr_dict in uc_dict["network requirements"].items():
                nr = IQB_DEFAULT_CONFIG.use_cases[uc_name].network_requirements[nr_name]
                assert nr.weight == nr_dict["w"], f"weight mismatch in {uc_name!r}/{nr_name!r}"

    def test_network_requirement_thresholds_match(self):
        """Network requirement threshold_min values match the legacy dict values."""
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            for nr_name, nr_dict in uc_dict["network requirements"].items():
                nr = IQB_DEFAULT_CONFIG.use_cases[uc_name].network_requirements[nr_name]
                assert nr.threshold_min == nr_dict["threshold min"], (
                    f"threshold mismatch in {uc_name!r}/{nr_name!r}"
                )

    def test_dataset_weights_match(self):
        """Dataset weights match the legacy dict values."""
        for uc_name, uc_dict in IQB_CONFIG["use cases"].items():
            for nr_name, nr_dict in uc_dict["network requirements"].items():
                for ds_name, ds_dict in nr_dict["datasets"].items():
                    ds = (
                        IQB_DEFAULT_CONFIG.use_cases[uc_name]
                        .network_requirements[nr_name]
                        .datasets[ds_name]
                    )
                    assert ds.weight == ds_dict["w"], (
                        f"weight mismatch in {uc_name!r}/{nr_name!r}/{ds_name!r}"
                    )


class TestIQBDefaultConfigIsDerivedFromLegacy:
    """Verify that IQB_DEFAULT_CONFIG is the same object as iqb_config_from_legacy(IQB_CONFIG)."""

    def test_default_equals_from_legacy(self):
        """IQB_DEFAULT_CONFIG equals a fresh conversion of IQB_CONFIG."""
        fresh = iqb_config_from_legacy(IQB_CONFIG)
        assert IQB_DEFAULT_CONFIG == fresh

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

    def test_iqb_config_network_requirement_is_frozen(self):
        """IQBConfigNetworkRequirement instances are immutable."""
        uc = next(iter(IQB_DEFAULT_CONFIG.use_cases.values()))
        nr = next(iter(uc.network_requirements.values()))
        with pytest.raises(AttributeError):
            nr.weight = 99  # type: ignore[misc]

    def test_iqb_config_dataset_is_frozen(self):
        """IQBConfigDataset instances are immutable."""
        uc = next(iter(IQB_DEFAULT_CONFIG.use_cases.values()))
        nr = next(iter(uc.network_requirements.values()))
        ds = next(iter(nr.datasets.values()))
        with pytest.raises(AttributeError):
            ds.weight = 99  # type: ignore[misc]

    def test_construct_programmatically(self):
        """Verify that configs can be built manually without the legacy loader."""
        config = IQBConfig(
            use_cases={
                "test": IQBConfigUseCase(
                    weight=1.0,
                    network_requirements={
                        "download_throughput_mbps": IQBConfigNetworkRequirement(
                            weight=3.0,
                            threshold_min=10.0,
                            datasets={"m-lab": IQBConfigDataset(weight=1.0)},
                        ),
                    },
                ),
            },
        )
        assert config.use_cases["test"].weight == 1.0
        nr = config.use_cases["test"].network_requirements["download_throughput_mbps"]
        assert nr.weight == 3.0
        assert nr.threshold_min == 10.0
        assert nr.datasets["m-lab"].weight == 1.0


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

"""Module that implements calculating IQB scores."""

import dataclasses
import json

from ..cache.cache import IQBData
from .config import (
    IQB_DEFAULT_CONFIG,
    IQBConfig,
    IQBConfigNetworkRequirement,
    IQBConfigUseCase,
    iqb_config_from_legacy,
)


def _calculate_binary_requirement_score(
    *,
    network_requirement: str,
    value: float,
    threshold: float,
) -> int:
    """
    Calculates binary requirement score for the given network requirement, value (i.e., data), and threshold (of the net requirement).
    - If the requirement is **throughput**, then the score is 1 if the given value is **larger** than the given threshold, and otherwise 0.
    - If the requirement is **latency or packet loss**, then the score is 1 if the given value is **smaller** than the given threshold, and otherwise 0.
    """
    if network_requirement == "download_throughput_mbps":
        return 1 if value > threshold else 0
    elif network_requirement == "upload_throughput_mbps":
        return 1 if value > threshold else 0
    elif network_requirement == "latency_ms":
        return 1 if value < threshold else 0
    elif network_requirement == "packet_loss":
        return 1 if value < threshold else 0
    else:
        raise ValueError(
            f"The binary requirement score method is not implemented for the network_requirement: {network_requirement}"
        )


def _calculate_requirement_agreement_score(
    *,
    nr_name: str,
    nr_cfg: IQBConfigNetworkRequirement,
    data: dict[str, dict[str, float]],
) -> float:
    """Calculates requirement agreement score across all datasets for one network requirement."""
    ds_scores: list[float] = []
    ds_weights: list[float] = []

    for ds_name, ds_cfg in nr_cfg.datasets.items():
        if ds_name not in data:
            continue
        if ds_cfg.weight > 0:
            # binary requirement score (dataset, network requirement)
            brs = _calculate_binary_requirement_score(
                network_requirement=nr_name,
                value=data[ds_name][nr_name],
                threshold=nr_cfg.threshold_min,
            )
            ds_scores.append(brs * ds_cfg.weight)
            ds_weights.append(ds_cfg.weight)

    # requirement agreement score (weighted average across datasets)
    return sum(ds_scores) / sum(ds_weights)


def _calculate_use_case_score(
    *,
    uc_cfg: IQBConfigUseCase,
    data: dict[str, dict[str, float]],
) -> float:
    """Calculates use case score across all network requirements for one use case."""
    nr_scores: list[float] = []
    nr_weights: list[float] = []
    for nr_name, nr_cfg in uc_cfg.network_requirements.items():
        ras = _calculate_requirement_agreement_score(
            nr_name=nr_name,
            nr_cfg=nr_cfg,
            data=data,
        )
        nr_scores.append(ras * nr_cfg.weight)
        nr_weights.append(nr_cfg.weight)
    # use case score (weighted average of all requirements for this use case)
    return sum(nr_scores) / sum(nr_weights)


def _calculate_iqb_score(*, config: IQBConfig, data: dict | IQBData) -> float:
    """Calculates IQB score based on given config and data."""

    if isinstance(data, IQBData):
        data = data.to_dict()

    uc_scores: list[float] = []
    uc_weights: list[float] = []

    for _uc_name, uc_cfg in config.use_cases.items():
        ucs = _calculate_use_case_score(uc_cfg=uc_cfg, data=data)
        uc_scores.append(ucs * uc_cfg.weight)
        uc_weights.append(uc_cfg.weight)

    iqb_score = sum(uc_scores) / sum(uc_weights)
    return iqb_score


class IQBCalculator:
    """Component that calculates IQB scores."""

    def __init__(self, config: IQBConfig | dict | str | None = None, name=None):
        """
        Initialize a new instance of IQBCalculator.

        Parameters:
            config: IQBConfig dataclass, legacy dict, file path, or None for default config.
            name (str): [Optional] name for the IQBCalculator instance.
        """
        self.set_config(config)
        self.name = name

    def set_config(self, config: IQBConfig | dict | str | None):
        """Sets up configuration parameters. If None, uses the default config."""
        if config is None:
            self.config = IQB_DEFAULT_CONFIG
        elif isinstance(config, IQBConfig):
            self.config = config
        elif isinstance(config, dict):
            self.config = iqb_config_from_legacy(config)
        else:
            # TODO(bassosimone): implement loading config from file (json, yaml)
            raise NotImplementedError(
                "method for reading from configuration file other than the default not implemented"
            )

    def print_config(self):
        """Prints the current IQB configuration as JSON."""
        print(json.dumps(dataclasses.asdict(self.config), indent=2))

    def calculate_binary_requirement_score(
        self,
        network_requirement: str,
        value: float,
        threshold: float,
    ) -> int:
        """
        Calculates binary requirement score for the given network requirement, value (i.e., data), and threshold (of the net requirement).
        - If the requirement is **throughput**, then the score is 1 if the given value is **larger** than the given threshold, and otherwise 0.
        - If the requirement is **latency or packet loss**, then the score is 1 if the given value is **smaller** than the given threshold, and otherwise 0.
        """
        return _calculate_binary_requirement_score(
            network_requirement=network_requirement,
            value=value,
            threshold=threshold,
        )

    def calculate_iqb_score(self, data: dict | IQBData) -> float:
        """Calculates IQB score based on given data."""
        return _calculate_iqb_score(config=self.config, data=data)

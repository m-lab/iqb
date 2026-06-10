"""Module that implements calculating IQB scores."""

import dataclasses
import json

from ..cache.cache import IQBData
from .config import IQB_DEFAULT_CONFIG, IQBConfig, iqb_config_from_legacy


def _calculate_binary_requirement_score(
    network_requirement: str, value: float, threshold: float
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


def _calculate_iqb_score(config: IQBConfig, data: dict | IQBData) -> float:
    """Calculates IQB score based on given config and data."""

    if isinstance(data, IQBData):
        data = data.to_dict()

    uc_scores = []
    uc_weights = []

    for _uc_name, uc_cfg in config.use_cases.items():
        nr_scores = []
        nr_weights = []
        for nr_name, nr_cfg in uc_cfg.network_requirements.items():
            # TODO: TEMP method for calculating binary requirement scores. To be
            # updated with weighted average of scores per dataset.
            ds_s = []
            for ds_name, ds_cfg in nr_cfg.datasets.items():
                if ds_name not in data:
                    continue
                if ds_cfg.weight > 0:
                    # binary requirement score (dataset, network requirement)
                    brs = _calculate_binary_requirement_score(
                        nr_name, data[ds_name][nr_name], nr_cfg.threshold_min
                    )
                    ds_s.append(brs)

            # requirement agreement score (all datasets for this requirement)
            ras = sum(ds_s) / len(ds_s)

            nr_scores.append(ras * nr_cfg.weight)
            nr_weights.append(nr_cfg.weight)

        # use case score (all requirements for this use case)
        ucs = sum(nr_scores) / sum(nr_weights)
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

    def calculate_binary_requirement_score(self, network_requirement, value, threshold):
        """
        Calculates binary requirement score for the given network requirement, value (i.e., data), and threshold (of the net requirement).
        - If the requirement is **throughput**, then the score is 1 if the given value is **larger** than the given threshold, and otherwise 0.
        - If the requirement is **latency or packet loss**, then the score is 1 if the given value is **smaller** than the given threshold, and otherwise 0.
        """
        return _calculate_binary_requirement_score(network_requirement, value, threshold)

    def calculate_iqb_score(self, data: dict | IQBData) -> float:
        """Calculates IQB score based on given data."""
        return _calculate_iqb_score(self.config, data)

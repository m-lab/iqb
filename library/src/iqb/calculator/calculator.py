"""Module that implements calculating IQB scores."""

import dataclasses
import json

from typing_extensions import deprecated

from ..cache.cache import IQBData
from ..cache.mlab import IQBMetrics
from .config import (
    IQB_DEFAULT_CONFIG,
    IQBConfig,
    IQBConfigUseCase,
    NetworkRequirement,
    iqb_config_from_legacy,
)


def _iqb_data_from_dict(data: dict[str, dict[str, float]]) -> IQBData:
    """Convert the legacy nested-dict format to IQBData."""

    def _metrics_from_dict(d: dict[str, float]) -> IQBMetrics:
        return IQBMetrics(
            download=d.get("download_throughput_mbps", 0),
            upload=d.get("upload_throughput_mbps", 0),
            latency=d.get("latency_ms", 0),
            loss=d.get("packet_loss", 0),
        )

    mlab_dict = data.get("m-lab")
    cloudflare_dict = data.get("cloudflare")
    ookla_dict = data.get("ookla")

    return IQBData(
        mlab=_metrics_from_dict(mlab_dict) if mlab_dict else None,
        cloudflare=_metrics_from_dict(cloudflare_dict) if cloudflare_dict else None,
        ookla=_metrics_from_dict(ookla_dict) if ookla_dict else None,
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
    nr_cfg: NetworkRequirement,
    data: IQBData,
) -> float:
    """Calculates requirement agreement score across all datasets for one network requirement."""
    ds_scores: list[float] = []
    ds_weights: list[float] = []

    datasets: list[tuple[IQBMetrics | None, float]] = [
        (data.mlab, nr_cfg.dataset_weights.mlab),
        (data.cloudflare, nr_cfg.dataset_weights.cloudflare),
        (data.ookla, nr_cfg.dataset_weights.ookla),
    ]

    for metrics, weight in datasets:
        if metrics is not None and weight > 0:
            value = getattr(metrics, nr_name)
            brs = int(
                value > nr_cfg.threshold_min
                if nr_cfg.higher_is_better
                else value < nr_cfg.threshold_min
            )
            ds_scores.append(brs * weight)
            ds_weights.append(weight)

    # requirement agreement score (weighted average across datasets)
    return sum(ds_scores) / sum(ds_weights)


def _calculate_use_case_score(
    *,
    uc_cfg: IQBConfigUseCase,
    data: IQBData,
) -> float:
    """Calculates use case score across all network requirements for one use case."""
    nrs = uc_cfg.network_requirements
    nr_scores: list[float] = []
    nr_weights: list[float] = []

    for field in dataclasses.fields(nrs):
        nr_cfg = getattr(nrs, field.name)
        if nr_cfg is None:
            continue
        ras = _calculate_requirement_agreement_score(
            nr_name=field.name,
            nr_cfg=nr_cfg,
            data=data,
        )
        nr_scores.append(ras * nr_cfg.weight)
        nr_weights.append(nr_cfg.weight)

    # use case score (weighted average of all requirements for this use case)
    return sum(nr_scores) / sum(nr_weights)


def _calculate_iqb_score(*, config: IQBConfig, data: dict | IQBData) -> float:
    """Calculates IQB score based on given config and data."""

    if isinstance(data, dict):
        data = _iqb_data_from_dict(data)

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

    @deprecated(
        "Use IQBConfigNetworkRequirementSpeed or IQBConfigNetworkRequirementLatency.binary_requirement_score instead"
    )
    def calculate_binary_requirement_score(
        self,
        network_requirement: str,
        value: float,
        threshold: float,
    ) -> int:
        return _calculate_binary_requirement_score(
            network_requirement=network_requirement,
            value=value,
            threshold=threshold,
        )

    def calculate_iqb_score(self, data: dict | IQBData) -> float:
        """Calculates IQB score based on given data."""
        return _calculate_iqb_score(config=self.config, data=data)

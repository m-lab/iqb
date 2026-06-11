"""Module containing the IQB configuration dataclasses and defaults."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, kw_only=True)
class IQBConfigDataset:
    """Configuration for a single dataset within a network requirement."""

    weight: float


@dataclass(frozen=True, kw_only=True)
class IQBConfigDatasetWeights:
    """Per-dataset weights for a network requirement."""

    mlab: float = 0.0
    cloudflare: float = 0.0
    ookla: float = 0.0


class NetworkRequirement(Protocol):
    weight: float
    threshold_min: float
    dataset_weights: IQBConfigDatasetWeights

    @property
    def higher_is_better(self) -> bool: ...

    def binary_requirement_score(self, value: float) -> int: ...


@dataclass(frozen=True, kw_only=True)
class IQBConfigNetworkRequirementSpeed:
    weight: float
    threshold_min: float
    dataset_weights: IQBConfigDatasetWeights

    @property
    def higher_is_better(self) -> bool:
        return True

    def binary_requirement_score(self, value: float) -> int:
        return 1 if value > self.threshold_min else 0


@dataclass(frozen=True, kw_only=True)
class IQBConfigNetworkRequirementLatency:
    weight: float
    threshold_min: float
    dataset_weights: IQBConfigDatasetWeights

    @property
    def higher_is_better(self) -> bool:
        return False

    def binary_requirement_score(self, value: float) -> int:
        return 1 if value < self.threshold_min else 0


@dataclass(frozen=True, kw_only=True)
class IQBConfigNetworkRequirementLoss:
    weight: float
    threshold_min: float
    dataset_weights: IQBConfigDatasetWeights

    @property
    def higher_is_better(self) -> bool:
        return False

    def binary_requirement_score(self, value: float) -> int:
        return 1 if value < self.threshold_min else 0


@dataclass(frozen=True, kw_only=True)
class IQBConfigNetworkRequirements:
    """All network requirements for a use case."""

    download_throughput_mbps: IQBConfigNetworkRequirementSpeed | None = None
    upload_throughput_mbps: IQBConfigNetworkRequirementSpeed | None = None
    latency_ms: IQBConfigNetworkRequirementLatency | None = None
    packet_loss: IQBConfigNetworkRequirementLoss | None = None


@dataclass(frozen=True, kw_only=True)
class IQBConfigUseCase:
    """Configuration for a single use case."""

    weight: float
    network_requirements: IQBConfigNetworkRequirements


@dataclass(frozen=True, kw_only=True)
class IQBConfig:
    """Top-level IQB configuration containing all use cases."""

    use_cases: dict[str, IQBConfigUseCase]


_NR_NAME_TO_CLASS = {
    "download_throughput_mbps": IQBConfigNetworkRequirementSpeed,
    "upload_throughput_mbps": IQBConfigNetworkRequirementSpeed,
    "latency_ms": IQBConfigNetworkRequirementLatency,
    "packet_loss": IQBConfigNetworkRequirementLoss,
}


def iqb_config_from_legacy(legacy: dict) -> IQBConfig:
    """Load an IQBConfig from the legacy nested-dict format.

    The legacy format uses keys like ``"use cases"``, ``"network requirements"``,
    and ``"threshold min"`` with spaces. This function maps them to the
    corresponding dataclass fields.
    """
    use_cases: dict[str, IQBConfigUseCase] = {}
    for uc_name, uc_dict in legacy["use cases"].items():
        nr_configs: dict[
            str,
            IQBConfigNetworkRequirementSpeed
            | IQBConfigNetworkRequirementLatency
            | IQBConfigNetworkRequirementLoss,
        ] = {}
        for nr_name, nr_dict in uc_dict["network requirements"].items():
            ds_weights: dict[str, float] = {}
            for ds_name, ds_dict in nr_dict["datasets"].items():
                ds_weights[ds_name] = ds_dict["w"]
            nr_class = _NR_NAME_TO_CLASS[nr_name]
            nr_configs[nr_name] = nr_class(
                weight=nr_dict["w"],
                threshold_min=nr_dict["threshold min"],
                dataset_weights=IQBConfigDatasetWeights(
                    mlab=ds_weights.get("m-lab", 0.0),
                    cloudflare=ds_weights.get("cloudflare", 0.0),
                    ookla=ds_weights.get("ookla", 0.0),
                ),
            )
        use_cases[uc_name] = IQBConfigUseCase(
            weight=uc_dict["w"],
            network_requirements=IQBConfigNetworkRequirements(
                **nr_configs,  # type: ignore[arg-type]
            ),
        )
    return IQBConfig(use_cases=use_cases)


IQB_CONFIG = {
    "use cases": {
        "web browsing": {
            "w": 1,
            "network requirements": {
                "download_throughput_mbps": {
                    "w": 3,
                    "threshold min": 10,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "upload_throughput_mbps": {
                    "w": 2,
                    "threshold min": 10,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "latency_ms": {
                    "w": 4,
                    "threshold min": 100,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "packet_loss": {
                    "w": 4,
                    "threshold min": 0.01,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
            },
        },
        "video streaming": {
            "w": 1,
            "network requirements": {
                "download_throughput_mbps": {
                    "w": 4,
                    "threshold min": 25,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "upload_throughput_mbps": {
                    "w": 2,
                    "threshold min": 10,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "latency_ms": {
                    "w": 4,
                    "threshold min": 100,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "packet_loss": {
                    "w": 4,
                    "threshold min": 0.01,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
            },
        },
        "audio streaming": {
            "w": 1,
            "network requirements": {
                "download_throughput_mbps": {
                    "w": 4,
                    "threshold min": 10,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "upload_throughput_mbps": {
                    "w": 1,
                    "threshold min": 5,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "latency_ms": {
                    "w": 3,
                    "threshold min": 100,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "packet_loss": {
                    "w": 4,
                    "threshold min": 0.01,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
            },
        },
        "video conferencing": {
            "w": 1,
            "network requirements": {
                "download_throughput_mbps": {
                    "w": 4,
                    "threshold min": 25,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "upload_throughput_mbps": {
                    "w": 4,
                    "threshold min": 25,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "latency_ms": {
                    "w": 4,
                    "threshold min": 50,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "packet_loss": {
                    "w": 4,
                    "threshold min": 0.005,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
            },
        },
        "online backup": {
            "w": 1,
            "network requirements": {
                "download_throughput_mbps": {
                    "w": 4,
                    "threshold min": 10,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "upload_throughput_mbps": {
                    "w": 4,
                    "threshold min": 25,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "latency_ms": {
                    "w": 2,
                    "threshold min": 100,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "packet_loss": {
                    "w": 4,
                    "threshold min": 0.01,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
            },
        },
        "gaming": {
            "w": 1,
            "network requirements": {
                "download_throughput_mbps": {
                    "w": 4,
                    "threshold min": 25,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "upload_throughput_mbps": {
                    "w": 4,
                    "threshold min": 25,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "latency_ms": {
                    "w": 5,
                    "threshold min": 10,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
                "packet_loss": {
                    "w": 4,
                    "threshold min": 0.005,
                    "datasets": {
                        "m-lab": {"w": 1},
                        "cloudflare": {"w": 0},
                        "ookla": {"w": 0},
                    },
                },
            },
        },
    },
}

IQB_DEFAULT_CONFIG: IQBConfig = iqb_config_from_legacy(IQB_CONFIG)

"""Module containing the IQB configuration dataclasses and defaults."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class IQBConfigDataset:
    """Configuration for a single dataset within a network requirement."""

    weight: float


@dataclass(frozen=True, kw_only=True)
class IQBConfigNetworkRequirement:
    """Configuration for a single network requirement within a use case."""

    weight: float
    threshold_min: float
    datasets: dict[str, IQBConfigDataset]


@dataclass(frozen=True, kw_only=True)
class IQBConfigUseCase:
    """Configuration for a single use case."""

    weight: float
    network_requirements: dict[str, IQBConfigNetworkRequirement]


@dataclass(frozen=True, kw_only=True)
class IQBConfig:
    """Top-level IQB configuration containing all use cases."""

    use_cases: dict[str, IQBConfigUseCase]


def iqb_config_from_legacy(legacy: dict) -> IQBConfig:
    """Load an IQBConfig from the legacy nested-dict format.

    The legacy format uses keys like ``"use cases"``, ``"network requirements"``,
    and ``"threshold min"`` with spaces. This function maps them to the
    corresponding dataclass fields.
    """
    use_cases: dict[str, IQBConfigUseCase] = {}
    for uc_name, uc_dict in legacy["use cases"].items():
        network_requirements: dict[str, IQBConfigNetworkRequirement] = {}
        for nr_name, nr_dict in uc_dict["network requirements"].items():
            datasets: dict[str, IQBConfigDataset] = {}
            for ds_name, ds_dict in nr_dict["datasets"].items():
                datasets[ds_name] = IQBConfigDataset(weight=ds_dict["w"])
            network_requirements[nr_name] = IQBConfigNetworkRequirement(
                weight=nr_dict["w"],
                threshold_min=nr_dict["threshold min"],
                datasets=datasets,
            )
        use_cases[uc_name] = IQBConfigUseCase(
            weight=uc_dict["w"],
            network_requirements=network_requirements,
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

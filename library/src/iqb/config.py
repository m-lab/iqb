"""Module containing the default IQB configuration."""

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
    }
}

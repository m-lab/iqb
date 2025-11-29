"""Module to convert a dataframe to an IQBSummary."""

from dataclasses import dataclass


@dataclass(frozen=True)
class IQBSummary:
    """Data frame summary for computing IQB score."""

    download_throughput_mbps: float
    upload_throughput_mbps: float
    latency_ms: float
    packet_loss: float

    def to_dict(self) -> dict[str, float]:
        """Convert to dictionary."""
        return {
            "download_throughput_mbps": self.download_throughput_mbps,
            "upload_throughput_mbps": self.upload_throughput_mbps,
            "latency_ms": self.latency_ms,
            "packet_loss": self.packet_loss,
        }

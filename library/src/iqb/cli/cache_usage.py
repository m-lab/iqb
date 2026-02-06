"""Cache usage command."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from ..pipeline.cache import (
    PIPELINE_CACHE_DATA_FILENAME,
    PIPELINE_CACHE_STATS_FILENAME,
    data_dir_or_default,
)
from .cache import cache

_TS_RE = re.compile(r"^\d{8}T\d{6}Z$")
_DATASET_RE = re.compile(r"^[a-z0-9_]+$")


@dataclass
class _DatasetStats:
    """Per-dataset raw statistics."""

    name: str
    parquet_size: int
    bq_bytes_billed: int
    query_duration_seconds: float


@dataclass
class _PeriodStats:
    """Aggregated statistics for a (start, end) time period."""

    start_ts: str
    end_ts: str
    datasets: list[_DatasetStats] = field(default_factory=list)

    @property
    def total_parquet_size(self) -> int:
        return sum(d.parquet_size for d in self.datasets)

    @property
    def total_bq_bytes_billed(self) -> int:
        return sum(d.bq_bytes_billed for d in self.datasets)

    @property
    def total_query_duration(self) -> float:
        return sum(d.query_duration_seconds for d in self.datasets)


def _read_stats_json(stats_path: Path) -> tuple[int, float]:
    """Read stats.json and return (total_bytes_billed, query_duration_seconds).

    Tolerates missing files, corrupt JSON, and null field values by
    returning zeros for any value that cannot be read.
    """
    if not stats_path.exists():
        return 0, 0.0
    try:
        data = json.loads(stats_path.read_text())
    except (json.JSONDecodeError, OSError):
        return 0, 0.0
    bq_bytes = data.get("total_bytes_billed")
    duration = data.get("query_duration_seconds")
    return (
        int(bq_bytes) if bq_bytes is not None else 0,
        float(duration) if duration is not None else 0.0,
    )


def _scan_periods(data_dir: Path) -> list[_PeriodStats]:
    """Walk cache/v1/{start}/{end}/{dataset}/ and collect statistics."""
    cache_root = data_dir / "cache" / "v1"
    if not cache_root.is_dir():
        return []

    periods: dict[tuple[str, str], _PeriodStats] = {}

    for start_dir in sorted(cache_root.iterdir()):
        if not start_dir.is_dir() or not _TS_RE.match(start_dir.name):
            continue
        for end_dir in sorted(start_dir.iterdir()):
            if not end_dir.is_dir() or not _TS_RE.match(end_dir.name):
                continue
            for dataset_dir in sorted(end_dir.iterdir()):
                if not dataset_dir.is_dir() or not _DATASET_RE.match(dataset_dir.name):
                    continue
                parquet_path = dataset_dir / PIPELINE_CACHE_DATA_FILENAME
                if not parquet_path.exists():
                    continue
                parquet_size = parquet_path.stat().st_size
                stats_path = dataset_dir / PIPELINE_CACHE_STATS_FILENAME
                bq_bytes, duration = _read_stats_json(stats_path)
                key = (start_dir.name, end_dir.name)
                if key not in periods:
                    periods[key] = _PeriodStats(start_ts=key[0], end_ts=key[1])
                periods[key].datasets.append(
                    _DatasetStats(
                        name=dataset_dir.name,
                        parquet_size=parquet_size,
                        bq_bytes_billed=bq_bytes,
                        query_duration_seconds=duration,
                    )
                )

    return [periods[k] for k in sorted(periods)]


def _format_bytes(n: int) -> str:
    """Format a byte count using SI-like suffixes."""
    if n == 0:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            if n == int(n):
                return f"{int(n)} {unit}"
            return f"{n:.1f} {unit}"
        n_f = n / 1024
        n = n_f  # type: ignore[assignment]
    return f"{n:.1f} PB"


def _format_duration(seconds: float) -> str:
    """Format a duration in seconds to a human-readable string."""
    if seconds == 0:
        return "0s"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    remaining = seconds - minutes * 60
    return f"{minutes}m {remaining:.1f}s"


def _format_period(start_ts: str, end_ts: str) -> str:
    """Format a pair of RFC3339-ish timestamps for display.

    Converts '20241001T000000Z' to '2024-10-01' style.
    """
    start = f"{start_ts[:4]}-{start_ts[4:6]}-{start_ts[6:8]}"
    end = f"{end_ts[:4]}-{end_ts[4:6]}-{end_ts[6:8]}"
    return f"{start} .. {end}"


def _build_table(periods: list[_PeriodStats]) -> Table:
    """Construct a Rich Table from the scanned period stats."""
    table = Table()
    table.add_column("Period", style="cyan")
    table.add_column("Datasets", justify="right")
    table.add_column("Parquet Size", justify="right")
    table.add_column("BQ Bytes Billed", justify="right")
    table.add_column("Query Duration", justify="right")

    total_datasets = 0
    total_parquet = 0
    total_bq = 0
    total_duration = 0.0

    for period in periods:
        count = len(period.datasets)
        total_datasets += count
        total_parquet += period.total_parquet_size
        total_bq += period.total_bq_bytes_billed
        total_duration += period.total_query_duration
        table.add_row(
            _format_period(period.start_ts, period.end_ts),
            str(count),
            _format_bytes(period.total_parquet_size),
            _format_bytes(period.total_bq_bytes_billed),
            _format_duration(period.total_query_duration),
        )

    table.add_section()
    table.add_row(
        "[bold]Total[/bold]",
        f"[bold]{total_datasets}[/bold]",
        f"[bold]{_format_bytes(total_parquet)}[/bold]",
        f"[bold]{_format_bytes(total_bq)}[/bold]",
        f"[bold]{_format_duration(total_duration)}[/bold]",
    )

    return table


@cache.command()
@click.option("-d", "--dir", "data_dir", default=None, help="Data directory (default: .iqb)")
def usage(data_dir: str | None) -> None:
    """Show cache disk and BigQuery usage statistics."""
    resolved = data_dir_or_default(data_dir)
    periods = _scan_periods(resolved)
    if not periods:
        click.echo("No cached data found.")
        return
    console = Console()
    console.print(_build_table(periods))

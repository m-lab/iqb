#!/usr/bin/env python3
"""
Orchestrate the data generation pipeline for IQB static data.

This script:
1. Runs BigQuery queries for downloads and uploads for multiple time periods
2. Saves results to v1 Parquet cache
"""

import os
import sys
from dataclasses import dataclass
from pathlib import Path

# Add library to path so we can import iqb modules
sys.path.insert(0, str(Path(__file__).parent.parent / "library" / "src"))

import click
import dacite
import yaml
from rich import get_console
from rich.panel import Panel

from iqb import __version__
from iqb.scripting import iqb_exception, iqb_logging, iqb_pipeline

default_datadir = Path(__file__).parent


@dataclass(frozen=True, kw_only=True)
class DateRange:
    start: str
    end: str


@dataclass(frozen=True, kw_only=True)
class PipelineMatrix:
    dates: list[DateRange]
    granularities: list[str]


@dataclass(frozen=True, kw_only=True)
class PipelineConfig:
    version: str
    matrix: PipelineMatrix


def load_pipeline_config(config_path):
    """Load pipeline configuration matrix from YAML script."""

    try:
        content = config_path.read_text()
    except FileNotFoundError as exc:
        raise click.ClickException(f"Pipeline config not found: {config_path}") from exc

    try:
        data = yaml.safe_load(content) or {}
    except yaml.YAMLError as exc:
        raise click.ClickException(f"Invalid YAML in {config_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise click.ClickException("Pipeline config must be a mapping.")

    try:
        config = dacite.from_dict(PipelineConfig, data)
    except dacite.DaciteError as exc:
        raise click.ClickException(f"Invalid pipeline config: {exc}") from exc

    if config.version != "v0":
        raise click.ClickException(
            f"Unsupported pipeline config version: {config.version}"
        )

    time_periods = [(entry.start, entry.end) for entry in config.matrix.dates]
    if not time_periods:
        raise click.ClickException(
            "Pipeline config matrix must include non-empty dates."
        )

    granularities = tuple(grain.strip() for grain in config.matrix.granularities)
    if not granularities or any(not grain for grain in granularities):
        raise click.ClickException(
            "Pipeline config matrix must include non-empty granularities."
        )

    return time_periods, granularities


@click.command()
@click.option(
    "-d",
    "--datadir",
    default=default_datadir.relative_to(Path(os.getcwd())),
    metavar="DIR",
    show_default=True,
    help="Set data directory.",
)
@click.option(
    "-B",
    "--enable-bigquery",
    is_flag=True,
    default=False,
    help="Enable BigQuery.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Verbose mode.",
)
@click.version_option(__version__)
def main(datadir, enable_bigquery, verbose):
    """Download IQB parquet data from remote caches and BigQuery."""

    # Grab the global rich console
    console = get_console()

    # Ensure we see debug messages
    iqb_logging.configure(verbose=verbose)

    # Create the pipeline
    pipeline = iqb_pipeline.create(datadir)

    # Read the pipeline config
    time_periods, granularities = load_pipeline_config(Path(datadir) / "pipeline.yaml")

    # Prepare for intercepting exceptions
    interceptor = iqb_exception.Interceptor()

    # Generate all data
    for grain in granularities:
        for start, end in time_periods:
            console.print(Panel(f"Sync {grain} data for {start} \u2192 {end}"))
            with interceptor:
                pipeline.sync_mlab(
                    grain,
                    enable_bigquery=enable_bigquery,
                    start_date=start,
                    end_date=end,
                )

    # Invoke exit
    sys.exit(interceptor.exitcode())


if __name__ == "__main__":
    main()

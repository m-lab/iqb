#!/usr/bin/env python3
"""
Orchestrate the data generation pipeline for IQB static data.

This script:
1. Runs BigQuery queries for downloads and uploads for multiple time periods
2. Saves results to v1 Parquet cache
"""

import sys
from pathlib import Path

# Add library to path so we can import iqb modules
sys.path.insert(0, str(Path(__file__).parent.parent / "library" / "src"))

import click
from rich import get_console
from rich.panel import Panel

from iqb import __version__
from iqb.scripting import iqb_exception, iqb_logging, iqb_pipeline


@click.command(
    "generate_data",
    context_settings={"show_default": True},
)
@click.option(
    "-d",
    "--datadir",
    default="data",
    metavar="DIR",
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

    # Define the time periods
    time_periods = [
        # ("2024-10-01", "2024-11-01"),
        ("2025-01-01", "2025-02-01"),
        ("2025-02-01", "2025-03-01"),
        ("2025-03-01", "2025-04-01"),
        ("2025-04-01", "2025-05-01"),
        ("2025-05-01", "2025-06-01"),
        ("2025-06-01", "2025-07-01"),
        ("2025-07-01", "2025-08-01"),
        ("2025-10-01", "2025-11-01"),
    ]

    # Define the granularities
    granularities = (
        "country",
        # "country_asn",
        # "city",
        # "city_asn",
        # "subdivision1",
        # "subdivision1_asn",
    )

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

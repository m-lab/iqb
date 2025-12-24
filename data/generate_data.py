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

from rich import get_console
from rich.panel import Panel

from iqb.scripting import iqb_exception, iqb_logging, iqb_pipeline


def main():
    # Ensure we're in the data directory
    data_dir = Path(__file__).parent
    console = get_console()

    # Ensure we see debug messages
    iqb_logging.configure(verbose=True)

    # Create the pipeline
    pipeline = iqb_pipeline.create(data_dir)

    # Define the time periods
    time_periods = [
        ("2024-10-01", "2024-11-01"),
        ("2025-10-01", "2025-11-01"),
    ]

    # Define the granularities
    granularities = (
        "country",
        "country_asn",
        "city",
        "city_asn",
        "subdivision1",
        "subdivision1_asn",
    )

    # Prepare for intercepting exceptions
    interceptor = iqb_exception.Interceptor()

    # Generate all data
    for granularity in granularities:
        for start_date, end_date in time_periods:
            with interceptor:
                console.print(
                    Panel(
                        f"Syncing {granularity} data for {start_date} \u2192 {end_date}"
                    )
                )
                pipeline.sync_mlab(
                    granularity,
                    start_date=start_date,
                    end_date=end_date,
                )

    # Invoke exit
    sys.exit(interceptor.exitcode())


if __name__ == "__main__":
    main()

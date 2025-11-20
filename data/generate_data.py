#!/usr/bin/env python3
"""
Orchestrate the data generation pipeline for IQB static data.

This script:
1. Runs BigQuery queries for downloads and uploads for multiple time periods
2. Merges the results into per-country JSON files
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], description: str) -> None:
    """Run a command and handle errors."""
    print(f"\n{'=' * 60}")
    print(f"{description}")
    print(f"{'=' * 60}")

    result = subprocess.run(cmd, capture_output=False)

    if result.returncode != 0:
        print(f"\n✗ Failed: {description}", file=sys.stderr)
        sys.exit(1)

    print(f"✓ Completed: {description}")


def generate_for_period(
    data_dir: Path, start_date: str, end_date: str, period_str: str
) -> None:
    """
    Generate data for a specific time period.

    Args:
        data_dir: Directory containing scripts and output files
        start_date: Start date in YYYY-MM-DD format (inclusive)
        end_date: End date in YYYY-MM-DD format (exclusive, Python slice convention)
        period_str: Period identifier for filenames (e.g., "2024_10")
    """
    print(f"\n{'#' * 60}")
    print(f"Generating data for period: {period_str}")
    print(f"Date range: [{start_date}, {end_date})")
    print(f"{'#' * 60}")

    # Stage 1a: Query downloads
    run_command(
        [
            "python3",
            str(data_dir / "run_query.py"),
            str(data_dir / "query_downloads_template.sql"),
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "-o",
            str(data_dir / "downloads.json"),
        ],
        f"Stage 1a: Querying download metrics for {period_str}",
    )

    # Stage 1b: Query uploads
    run_command(
        [
            "python3",
            str(data_dir / "run_query.py"),
            str(data_dir / "query_uploads_template.sql"),
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "-o",
            str(data_dir / "uploads.json"),
        ],
        f"Stage 1b: Querying upload metrics for {period_str}",
    )

    # Stage 2: Merge data
    # Creates: {country}_{period_str}.json (e.g., us_2024_10.json)
    run_command(
        [
            "python3",
            str(data_dir / "merge_data.py"),
            "--output-suffix",
            period_str,
        ],
        f"Stage 2: Merging data for {period_str}",
    )


def main():
    # Ensure we're in the data directory
    data_dir = Path(__file__).parent

    print("IQB Data Generation Pipeline")
    print("=" * 60)

    # Generate data for October 2024
    generate_for_period(data_dir, "2024-10-01", "2024-11-01", "2024_10")

    # Generate data for October 2025
    generate_for_period(data_dir, "2025-10-01", "2025-11-01", "2025_10")

    print("\n" + "=" * 60)
    print("✓ Pipeline completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()

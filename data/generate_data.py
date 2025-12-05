#!/usr/bin/env python3
"""
Orchestrate the data generation pipeline for IQB static data.

This script:
0. Syncs cached files from GitHub (if available)
1. Runs BigQuery queries for downloads and uploads for multiple time periods
2. Saves results to v1 Parquet cache
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

    datasets = (
        "country",
        "country_asn",
        "country_city",
        "country_city_asn",
        "country_subdivision1",
        "country_subdivision1_asn",
    )

    directions = ("downloads", "uploads")

    for dataset in datasets:
        for direction in directions:
            full_dataset = f"{direction}_by_{dataset}"
            run_command(
                [
                    "python3",
                    str(data_dir / "run_query.py"),
                    full_dataset,
                    "--start-date",
                    start_date,
                    "--end-date",
                    end_date,
                    # No -o argument: skips JSON conversion, only creates v1 Parquet cache
                ],
                f"Querying {full_dataset} metrics for {period_str}",
            )


def main():
    # Ensure we're in the data directory
    data_dir = Path(__file__).parent

    print("IQB Data Generation Pipeline")
    print("=" * 60)

    # Stage 0: Sync cached files from GitHub (if manifest exists)
    ghcache_script = data_dir / "ghcache.py"
    if ghcache_script.exists():
        run_command(
            ["python3", str(ghcache_script), "sync"],
            "Stage 0: Syncing cached files from GitHub",
        )
    else:
        print("\nNote: ghcache.py not found, skipping cache sync")

    # Generate data for October 2024
    generate_for_period(data_dir, "2024-10-01", "2024-11-01", "2024_10")

    # Generate data for October 2025
    generate_for_period(data_dir, "2025-10-01", "2025-11-01", "2025_10")

    print("\n" + "=" * 60)
    print("✓ Pipeline completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()

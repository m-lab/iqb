#!/usr/bin/env python3
"""
Orchestrate the data generation pipeline for IQB static data.

This script:
1. Runs BigQuery queries for downloads and uploads
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


def main():
    # Ensure we're in the data directory
    data_dir = Path(__file__).parent

    print("IQB Data Generation Pipeline")
    print("=" * 60)

    # Stage 1a: Query downloads
    run_command(
        [
            "python3",
            str(data_dir / "run_query.py"),
            str(data_dir / "query_downloads.sql"),
            "-o",
            str(data_dir / "downloads.json"),
        ],
        "Stage 1a: Querying download metrics (throughput, latency, packet loss)",
    )

    # Stage 1b: Query uploads
    run_command(
        [
            "python3",
            str(data_dir / "run_query.py"),
            str(data_dir / "query_uploads.sql"),
            "-o",
            str(data_dir / "uploads.json"),
        ],
        "Stage 1b: Querying upload metrics (throughput)",
    )

    # Stage 2: Merge data
    run_command(
        ["python3", str(data_dir / "merge_data.py")],
        "Stage 2: Merging download and upload data into per-country files",
    )

    print("\n" + "=" * 60)
    print("✓ Pipeline completed successfully!")
    print("=" * 60)
    print("\nGenerated files:")

    for country in ["us", "de", "br"]:
        file_path = data_dir / f"{country}_2024_10.json"
        if file_path.exists():
            size = file_path.stat().st_size
            print(f"  - {file_path.name} ({size:,} bytes)")


if __name__ == "__main__":
    main()

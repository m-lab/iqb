#!/usr/bin/env python3
"""Merge download and upload data into clean JSON files per country."""

import argparse
import json
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Merge download and upload data into per-country JSON files"
    )
    parser.add_argument(
        "--output-suffix",
        required=True,
        help="Suffix for output filenames (e.g., '2024_10' creates us_2024_10.json, de_2024_10.json, etc.)",
    )
    args = parser.parse_args()

    cache_dir = Path(__file__).parent / "cache" / "v0"

    # Load raw data (always from downloads.json and uploads.json)
    with open(cache_dir / "downloads.json") as f:
        downloads = json.load(f)

    with open(cache_dir / "uploads.json") as f:
        uploads = json.load(f)

    # Merge by country
    for dl in downloads:
        country_code = dl["country_code"]

        # Find matching upload data
        ul = next(u for u in uploads if u["country_code"] == country_code)

        # Extract percentiles into structured format
        download_percentiles = {
            f"p{p}": float(dl[f"download_p{p}"])
            for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
        }
        upload_percentiles = {
            f"p{p}": float(ul[f"upload_p{p}"])
            for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
        }
        latency_percentiles = {
            f"p{p}": float(dl[f"latency_p{p}"])
            for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
        }
        loss_percentiles = {
            f"p{p}": float(dl[f"loss_p{p}"]) for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
        }

        # Create clean output
        output = {
            "metrics": {
                "download_throughput_mbps": download_percentiles,
                "upload_throughput_mbps": upload_percentiles,
                "latency_ms": latency_percentiles,
                "packet_loss": loss_percentiles,
            },
        }

        # Write to file using output suffix
        output_file = cache_dir / f"{country_code.lower()}_{args.output_suffix}.json"
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)
        print(f"✓ Created {output_file.name}", file=sys.stderr)


if __name__ == "__main__":
    main()

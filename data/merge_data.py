#!/usr/bin/env python3
"""Merge download and upload data into clean JSON files per country."""

import json

# Load raw data
with open("downloads.json") as f:
    downloads = json.load(f)

with open("uploads.json") as f:
    uploads = json.load(f)

# Country names
COUNTRY_NAMES = {"US": "United States", "DE": "Germany", "BR": "Brazil"}

# Merge by country
for dl in downloads:
    country_code = dl["country_code"]

    # Find matching upload data
    ul = next(u for u in uploads if u["country_code"] == country_code)

    # Extract percentiles into structured format
    download_percentiles = {
        f"p{p}": float(dl[f"download_p{p}"]) for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
    }
    upload_percentiles = {
        f"p{p}": float(ul[f"upload_p{p}"]) for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
    }
    latency_percentiles = {
        f"p{p}": float(dl[f"latency_p{p}"]) for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
    }
    loss_percentiles = {
        f"p{p}": float(dl[f"loss_p{p}"]) for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]
    }

    # Create clean output
    output = {
        "metadata": {
            "country_code": country_code,
            "country_name": COUNTRY_NAMES[country_code],
            "period": "2024-10",
            "period_description": "October 2024",
            "dataset": "M-Lab NDT",
            "download_samples": int(dl["sample_count"]),
            "upload_samples": int(ul["sample_count"]),
        },
        "metrics": {
            "download_throughput_mbps": download_percentiles,
            "upload_throughput_mbps": upload_percentiles,
            "latency_ms": latency_percentiles,
            "packet_loss": loss_percentiles,
        },
    }

    # Write to file
    filename = f"{country_code.lower()}_2024_10.json"
    with open(filename, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✓ Created {filename}")

print("\nData files created successfully!")

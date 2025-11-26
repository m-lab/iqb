# IQB Static Data Files

This directory contains static measurement data used by
the IQB prototype for Phase 1 development.

## Current Dataset

**Period**: October 2024 (2024-10-01 to 2024-10-31) and October 2025

**Source**: [M-Lab NDT](https://www.measurementlab.net/tests/ndt/) unified views

**Countries**: all available countries

### Files

Generated files live inside [./cache/v0](./cache/v0).

Here are some sample files:

- `us_2024_10.json` - United States, ~31M download samples, ~24M upload samples

- `de_2024_10.json` - Germany, ~7M download samples, ~4M upload samples

- `br_2024_10.json` - Brazil, ~5M download samples, ~3M upload samples

### Data Structure

Each JSON file contains:

```JavaScript
{
  "metrics": {
    "download_throughput_mbps": {"p1": 0.38, /* ... */, "p99": 891.82},
    "upload_throughput_mbps": {"p1": 0.06, /* ... */, "p99": 813.73},
    "latency_ms": {"p1": 0.16, /* ... */, "p99": 254.34},
    "packet_loss": {"p1": 0.0, /* ... */, "p99": 0.25}
  }
}
```

**Percentiles included**: p1, p5, p10, p25, p50, p75, p90, p95, p99

## How This Data Was Generated

### BigQuery Queries

The data was extracted from M-Lab's public BigQuery tables using queries
inside the [../library/src/iqb/queries](../library/src/iqb/queries) package.

### Running the Data Generation Pipeline

**Prerequisites**:

- Google Cloud SDK (`gcloud`) installed

- BigQuery CLI (`bq`) installed

- `gcloud`-authenticated with an account subscribed to
[M-Lab Discuss mailing list](https://groups.google.com/a/measurementlab.net/g/discuss)

- Python 3.13 using `uv` as documented in the toplevel [README.md](../README.md)

**Complete Pipeline** (recommended):

```bash
cd data/
uv run python generate_data.py
```

This orchestrates the complete pipeline:

1. Queries BigQuery for download metrics (throughput, latency, packet loss)

2. Queries BigQuery for upload metrics (throughput)

3. Merges the data into per-country JSON files

Generated files `${country}_2024_10.json` and `${country}_2025_10.json`
inside the [./cache/v0](./cache/v0) directory.

**Individual Pipeline Stages** (for debugging):

```bash
cd data/

# Stage 1a: Query downloads
uv run python run_query.py query_downloads.sql -o downloads.json

# Stage 1b: Query uploads
uv run python run_query.py query_uploads.sql -o uploads.json

# Stage 2: Merge data
uv run python merge_data.py
```

**Pipeline Scripts**:

- [generate_data.py](generate_data.py) - Orchestrates the complete pipeline

- [run_query.py](run_query.py) - Executes a BigQuery query and saves results

- [merge_data.py](merge_data.py) - Merges download and upload data into
per-country files

## Notes

- **Static data**: These files contain pre-aggregated percentiles
for Phase 1 prototype. Phase 2 will add dynamic data fetching.

- **Time granularity**: Data is aggregated over the entire
months of October 2024 and October 2025. The analyst decides which
time window to use for running IQB calculations.

- **Percentile selection**: The Streamlit UI allows users
to select which percentile(s) to use for IQB score calculations.

- **File size**: Each file is ~1.4KB (uncompressed). No
compression needed.

## M-Lab NDT Data Schema

M-Lab provides two unified views:

- `measurement-lab.ndt.unified_downloads` - Download tests

- `measurement-lab.ndt.unified_uploads` - Upload tests

Key fields used:

- `a.MeanThroughputMbps` - Mean throughput in Mbps

- `a.MinRTT` - Minimum round-trip time in milliseconds

- `a.LossRate` - Packet loss rate (0.0-1.0)

- `client.Geo.CountryCode` - ISO country code

- `date` - Measurement date (YYYY-MM-DD)

See [M-Lab NDT documentation](https://www.measurementlab.net/tests/ndt/#ndt-data-in-bigquery)
for details.

## Future Improvements (Phase 2+)

- Dynamic data fetching from BigQuery

- Support for additional datasets (Ookla, Cloudflare)

- Finer time granularity (daily, weekly)

- Sub-national geographic resolution (cities, ASNs)

- Local database integration for caching aggregated data

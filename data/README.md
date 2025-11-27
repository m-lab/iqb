# IQB Static Data Files

This directory contains static reference data used by the IQB prototype.

## Current Dataset

**Period**: October 2024 and October 2025

**Source**: [M-Lab NDT](https://www.measurementlab.net/tests/ndt/) unified views

**Countries**: All available countries

## Data Formats

We maintain two data formats in `./cache/`:

### v0 - JSON Format (Golden Files)

Per-country JSON files with pre-aggregated percentiles:

- **Location**: `./cache/v0/{country}_{year}_{month}.json`
- **Example**: `us_2024_10.json` (~31M download samples, ~24M upload samples)
- **Structure**: Simple JSON with percentiles (p1, p5, p10, p25, p50, p75, p90, p95, p99)
- **Use case**: Casual data processing, backward compatibility, quick inspection

```json
{
  "metrics": {
    "download_throughput_mbps": {"p1": 0.38, "p99": 891.82},
    "upload_throughput_mbps": {"p1": 0.06, "p99": 813.73},
    "latency_ms": {"p1": 0.16, "p99": 254.34},
    "packet_loss": {"p1": 0.0, "p99": 0.25}
  }
}
```

### v1 - Parquet Format (Current)

Raw query results stored efficiently for flexible analysis:

- **Location**: `./cache/v1/{start_date}/{end_date}/{query_type}/`
- **Files**:
  - `data.parquet` - Query results (~1-60 MiB, streamable, chunked row groups)
  - `stats.json` - Query metadata (start time, duration, bytes processed/billed, template hash)
- **Use case**: Efficient filtering, large-scale analysis, direct PyArrow/Pandas processing

**Migration**: The [../library](../library) `IQBCache` uses v1. We are keeping v0 data
around as golden files, for backward compatibility, and casual use.

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

Generated files: v0 JSON files `${country}_2024_10.json` and `${country}_2025_10.json`
inside [./cache/v0](./cache/v0), plus v1 Parquet cache with query metadata.

**Individual Pipeline Stages** (for debugging):

```bash
cd data/

# Stage 1a: Query downloads
uv run python run_query.py downloads_by_country \
  --start-date 2024-10-01 --end-date 2024-11-01 \
  -o downloads.json

# Stage 1b: Query uploads
uv run python run_query.py uploads_by_country \
  --start-date 2024-10-01 --end-date 2024-11-01 \
  -o uploads.json

# Stage 2: Merge data
uv run python merge_data.py
```

**Pipeline Scripts**:

- [generate_data.py](generate_data.py) - Orchestrates the complete pipeline

- [run_query.py](run_query.py) - Executes BigQuery queries using IQBPipeline,
saves v1 cache (parquet + stats) and v0 JSON output

- [merge_data.py](merge_data.py) - Merges download and upload data into
per-country v0 files

## Notes

- **Static data**: These files contain pre-aggregated percentiles
for Phase 1 prototype. Phase 2 will add dynamic data fetching.

- **Data formats**: v0 JSON files (~1.4KB) for quick analysis;
v1 Parquet files (~1-60 MiB) with stats.json for efficient processing and cost tracking.

- **Time granularity**: Data is aggregated over the entire
months of October 2024 and October 2025. The analyst decides which
time window to use for running IQB calculations.

- **Percentile selection**: The Streamlit UI allows users
to select which percentile(s) to use for IQB score calculations.

- **File size**: Each per-country JSON file is ~1.4KB (uncompressed). No
compression needed. For more fine grained queries, the Parquet files
allow for more efficient storage and data processing.

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

- Finer geographic resolution (cities, provinces, ASNs) - IN PROGRESS
- Remote storage for `cache/v1` data (GitHub releases)
- Additional datasets (Ookla, Cloudflare)
- Finer time granularity (daily, weekly)
- Remote storage for `cache/v1` data (GCS buckets)

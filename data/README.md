# IQB Static Data Files

This directory contains static reference data used by the IQB prototype.

## Current Dataset

**Period**: October 2024 and October 2025

**Source**: [M-Lab NDT](https://www.measurementlab.net/tests/ndt/) unified views

**Countries**: All available countries

## Data Formats

We maintain two data formats in `./cache/`:

### v0 - JSON Format (Deprecated - Golden Files Only)

**IMPORTANT**: The v0 cache is no longer actively updated. Files in `./cache/v0/` are
kept as golden files for testing and backward compatibility only.

Per-country JSON files with pre-aggregated percentiles:

- **Location**: `./cache/v0/{country}_{year}_{month}.json`
- **Example**: `us_2024_10.json` (~31M download samples, ~24M upload samples)
- **Structure**: Simple JSON with percentiles (p1, p5, p10, p25, p50, p75, p90, p95, p99)
- **Use case**: Golden files for testing, backward compatibility

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

**Current format**: The [../library](../library) `IQBCache` uses v1 exclusively.

## GitHub Cache Synchronization (Interim Solution)

**IMPORTANT**: This is a throwaway interim solution that will be replaced by GCS.

Since the v1 Parquet files can be large (~1-60 MiB) and we have BigQuery quota
constraints, we use GitHub releases to distribute pre-generated cache files.

### For Data Scientists (Manual Workflow)

Sync cached files from GitHub before analysis:

```bash
cd data/
./ghcache.py sync
```

This downloads any missing cache files listed in `ghcache.json` and verifies SHA256.

### For Pipeline Users (Automatic)

The `generate_data.py` script automatically syncs cached files before running queries,
so you don't need to run `ghcache.py` manually.

### For Maintainers (Publishing New Cache)

When you generate new cache files locally:

```bash
cd data/
./ghcache.py scan
```

This:
1. Scans `cache/v1/` for git-ignored files
2. Computes SHA256 hashes
3. Copies files to `data/` with mangled names (ready for upload)
4. Updates `ghcache.json` manifest

Then manually:
1. Upload mangled files to GitHub release (e.g., v0.1.0)
2. Commit updated `ghcache.json` to repository

**Note**: This system assumes Unix paths and won't work on Windows.

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

1. Queries BigQuery for multiple datasets (country, country_asn, country_city, country_city_asn, country_subdivision1)

2. Queries both download and upload metrics for each dataset

3. Saves results to v1 Parquet cache with query metadata (skips JSON conversion for performance)

Generated files: v1 Parquet files in `./cache/v1/` with query metadata.

**Individual Pipeline Stages** (for debugging):

```bash
cd data/

# Run a single query
uv run python run_query.py downloads_by_country \
  --start-date 2024-10-01 --end-date 2024-11-01

# Inspect results with pandas
python3 << 'EOF'
import pandas as pd
df = pd.read_parquet('cache/v1/2024-10-01/2024-11-01/downloads_by_country/data.parquet')
print(df.head())
print(df.info())
EOF
```

**Pipeline Scripts**:

- [generate_data.py](generate_data.py) - Orchestrates the complete pipeline

- [run_query.py](run_query.py) - Executes BigQuery queries using IQBPipeline,
saves v1 Parquet cache only (use pandas to inspect results)

## Notes

- **Cache format**: v1 Parquet files (~1-60 MiB) with stats.json for efficient
processing and cost tracking. v0 JSON files are deprecated and kept only as golden files.

- **Time granularity**: Data is aggregated over the entire
months of October 2024 and October 2025. The analyst decides which
time window to use for running IQB calculations.

- **Percentile selection**: The Streamlit UI allows users
to select which percentile(s) to use for IQB score calculations.

- **Query results**: Raw query results are stored in Parquet format for efficient
filtering and analysis. Use the [../library](../library) `IQBCache` to access them.

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
- Replace GitHub releases with GCS buckets for cache distribution
- Additional datasets (Ookla, Cloudflare)
- Finer time granularity (daily, weekly)

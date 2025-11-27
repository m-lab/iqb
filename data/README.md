# IQB Static Data Files

This directory contains static reference data used by the IQB prototype.

## Current Dataset

**Period**: October 2024 and October 2025

**Source**: [M-Lab NDT](https://www.measurementlab.net/tests/ndt/) unified views

**Countries**: All available countries

## Data Formats

We maintain two data formats in `./cache/`:

### v0 - JSON Format

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

**Migration**: We're transitioning to v1 as the primary format. v0 remains available for
backward compatibility and casual use. If Parquet proves too heavy for some workflows,
v0 will continue to be maintained.

Raw query results stored efficiently for flexible analysis:

- **Location**: `./cache/v1/{start_date}/{end_date}/{query_type}/`
- **Files**:
  - `data.parquet` - Query results (20-60 MiB, streamable, chunked row groups)
  - `stats.json` - Query metadata (start time, duration, bytes processed/billed, template hash)
- **Use case**: Efficient filtering, large-scale analysis, direct PyArrow/Pandas processing

**Migration**: We're transitioning to v1 as the primary format. v0 remains available for
backward compatibility and casual use. If Parquet proves too heavy for some workflows,
v0 will continue to be maintained.

## Generating Data

### Prerequisites

- Google Cloud SDK authenticated with M-Lab access
- Python 3.13 with `uv` (see root [README.md](../README.md))

### Pipeline

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

# Merge into per-country v0 JSON files
uv run python merge_data.py
```

**What happens**:

1. `run_query.py` uses [IQBPipeline](../library/src/iqb/pipeline.py) to:
   - Execute BigQuery query from [templates](../library/src/iqb/queries/)
   - Save v1 cache: `cache/v1/{start}/{end}/{query_type}/data.parquet` + `stats.json`
   - Convert to v0 JSON for backward compatibility

- [run_query.py](run_query.py) - Executes BigQuery queries using IQBPipeline,
saves v1 cache (parquet + stats) and v0 JSON output

- [merge_data.py](merge_data.py) - Merges download and upload data into
per-country v0 files

## Notes

- **v0 vs v1**: v0 JSON files (~1.4KB each) are convenient for quick analysis.
  v1 Parquet files (~1-60 MiB) enable efficient filtering and large-scale processing.

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

- Direct Parquet reading in cache.py (PyArrow predicate pushdown for efficient filtering)
- Additional datasets (Ookla, Cloudflare)
- Finer geographic resolution (cities, provinces, ASNs)
- Finer time granularity (daily, weekly)
- Remote storage for v1 cache (GitHub releases, GCS buckets)

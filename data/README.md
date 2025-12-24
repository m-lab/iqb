# IQB Static Data Files

This directory contains static reference data used by the IQB prototype.

## Data Format

Raw query results stored efficiently for flexible analysis:

- **Location**: `./cache/v1/{start_date}/{end_date}/{query_type}/`
- **Files**:
  - `data.parquet` - Query results (~1-60 MiB, streamable, chunked row groups)
  - `stats.json` - Query metadata (start time, duration, bytes processed/billed, template hash)
- **Use case**: Efficient filtering, large-scale analysis, direct PyArrow/Pandas processing

## GitHub Cache Synchronization (Interim Solution)

**IMPORTANT**: This is a throwaway interim solution that will be replaced by GCS.

Since the v1 Parquet files can be large (~1-60 MiB) and we have BigQuery quota
constraints, we use GitHub releases to distribute pre-generated cache files.

The `generate_data.py` script automatically syncs cached files to avoid running
potentially expensive BigQuery queries.

The GitHub release manifest lives at `state/ghremote/manifest.json`.

### For Maintainers (Publishing New Cache)

When you generate new cache files locally:

```bash
uv run ./data/ghcache.py scan
```

This:
1. Scans `cache/v1/` for git-ignored files
2. Computes SHA256 hashes
3. Copies files to `data/` with mangled names (ready for upload)
4. Updates `state/ghremote/manifest.json` manifest

Then manually:
1. Upload mangled files to GitHub release (e.g., v0.1.0)
2. Commit updated `state/ghremote/manifest.json` to repository

### Running the Data Generation Pipeline

**Prerequisites**:

- Google Cloud SDK (`gcloud`) installed

- `gcloud`-authenticated with an account subscribed to
[M-Lab Discuss mailing list](https://groups.google.com/a/measurementlab.net/g/discuss)

- Python 3.13 using `uv` as documented in the toplevel [README.md](../README.md)

**Complete Pipeline** (recommended):

```bash
uv run python ./data/generate_data.py
```

This orchestrates the complete pipeline:

1. Queries BigQuery for multiple geographical granularities (country, country_asn, etc.)

2. Queries both download and upload metrics for each dataset

3. Saves results to v1 Parquet cache with query metadata

Generated files: v1 Parquet files in `./cache/v1/` with query metadata.

**Individual Pipeline Stages** (for debugging):

```bash
cd data/

# Run a single query
uv run python run_query.py --granularity country \
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

- [ghcache.py](ghcache.py) - Utility to manage the GitHub interim cache.

## Future Improvements (Phase 2+)

- Replace GitHub releases with GCS buckets for cache distribution
- Additional datasets (Ookla, Cloudflare)

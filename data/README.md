# IQB Data Workspace

This directory is a workspace for data curation scripts, release manifests,
and local cache artifacts produced during generation.

## What Lives Here

- `generate_data.py`: Orchestrates BigQuery extraction and writes cache files
  under `./cache/v1/` for local use.
- `pipeline.yaml`: Matrix configuration (dates and granularities) used by the
  data generation script.
- `run_query.py`: Legacy single-query helper (kept for now, but not the
  preferred workflow).
- `ghcache.py`: Helper for publishing cache files to GitHub releases.
- `state/ghremote/manifest.json`: Release manifest used by the GitHub remote
  cache implementation.

Static cache fixtures used by tests and notebooks are stored elsewhere:

- Real data fixtures: `library/tests/fixtures/real-data`
- Fake data fixtures: `library/tests/fixtures/fake-data`
- Notebook cache: `analysis/.iqb` (seeded to avoid network downloads in tests)

## Cache Format

Raw query results stored efficiently for flexible analysis:

- **Location**: `./cache/v1/{start_date}/{end_date}/{query_type}/`
- **Files**:
  - `data.parquet` - Query results (~1-60 MiB, streamable, chunked row groups)
  - `stats.json` - Query metadata (start time, duration, bytes processed/billed, template hash)
- **Use case**: Efficient filtering, large-scale analysis, direct PyArrow/Pandas processing

## GitHub Cache Synchronization (Interim Solution)

Since the v1 Parquet files can be large (~1-60 MiB) and we have BigQuery quota
constraints, we use GitHub releases to distribute pre-generated cache files.
The release manifest lives at `state/ghremote/manifest.json`.

### For Maintainers (Publishing New Cache)

When you generate new cache files locally (under `./cache/v1`):

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

1. Loads `./data/pipeline.yaml` to determine dates and granularities (edit this
   file to change the matrix)
2. Queries both download and upload metrics for each dataset
3. Saves results to v1 Parquet cache with query metadata

## Future Improvements (Phase 2+)

- Replace GitHub releases with GCS buckets for cache distribution
- Additional datasets (Ookla, Cloudflare)

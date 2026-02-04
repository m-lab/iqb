# IQB Data Workspace

This directory is a workspace for data curation scripts, release manifests,
and local cache artifacts produced during generation.

## What Lives Here

- `generate_data.py`: Orchestrates BigQuery extraction and writes cache files
  under `./cache/v1/` for local use.
- `pipeline.yaml`: Matrix configuration (dates and granularities) used by the
  data generation script.
- `ghcache.py`: Helper for publishing cache files to the remote cache.
- `state/ghremote/manifest.json`: Manifest used by the remote cache
  implementation.

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

## Remote Cache Synchronization

Since the v1 Parquet files can be large (~1-60 MiB) and we have BigQuery quota
constraints, we distribute pre-generated cache files via a GCS bucket
(`mlab-sandbox-iqb-us-central1`). The manifest lives at
`state/ghremote/manifest.json`.

### Bucket Setup

The GCS bucket was created in the `mlab-sandbox` project:

```bash
gcloud storage buckets create gs://mlab-sandbox-iqb-us-central1 \
    --project=mlab-sandbox \
    --location=us-central1 \
    --uniform-bucket-level-access
```

Public read access was granted so that the library can download cache
files without authentication:

```bash
gcloud storage buckets add-iam-policy-binding gs://mlab-sandbox-iqb-us-central1 \
    --member=allUsers \
    --role=roles/storage.objectViewer
```

### For Maintainers (Publishing New Cache)

When you generate new cache files locally (under `./cache/v1`):

```bash
uv run ./data/ghcache.py scan
```

This:
1. Scans `cache/v1/` for git-ignored files
2. Computes SHA256 hashes
3. Updates `state/ghremote/manifest.json` with correct GCS URLs
4. Prints the `gcloud storage rsync` command to upload files

Then:
1. Remove zero-length `.lock` files left over by the pipeline before uploading:
   ```bash
   find data/cache/v1 -type f -name .lock -delete
   ```
2. Run the printed `gcloud storage rsync` command to upload to GCS
3. Commit updated `state/ghremote/manifest.json` to repository

### Running the Data Generation Pipeline

**Prerequisites**:

- Google Cloud SDK (`gcloud`) installed

- `gcloud`-authenticated with an account subscribed to
[M-Lab Discuss mailing list](https://groups.google.com/a/measurementlab.net/g/discuss)

- Python 3.13 using `uv` as documented in the toplevel [README.md](../README.md)

**Complete Pipeline**:

```bash
uv run python ./data/generate_data.py -B
```

This orchestrates the complete pipeline:

1. Loads `./data/pipeline.yaml` to determine dates and granularities (edit this
   file to change the matrix)
2. Attempts to fetch from the remote cache first
3. Otherwise, if `-B` is present, queries both download and upload metrics for each dataset
4. Saves results to v1 Parquet cache with query metadata

Omit the `-B` flag to avoid querying BigQuery.

## Future Improvements (Phase 2+)

- Additional datasets (Ookla, Cloudflare)

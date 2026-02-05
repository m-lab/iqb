# IQB Data Workspace

This directory holds cache artifacts, pipeline configuration, and the
remote-cache manifest produced by the IQB data pipeline.

## What Lives Here

- `pipeline.yaml` — configuration consumed by `iqb pipeline run`.
- `cache/` — local cache written by the `iqb pipeline run`.
- `state/ghremote/manifest.json` — manifest used by `iqb cache`.

## Prerequisites

- Python 3.13 using `uv` as documented in the toplevel [README.md](../README.md).

- Google Cloud SDK (`gcloud`) installed.

- `gcloud auth login` with an account subscribed to the [M-Lab Discuss mailing
list](https://groups.google.com/a/measurementlab.net/g/discuss).

- `gcloud auth application-default login` using the same account.

## `iqb cache pull` - Getting GCS-Cached Data

The [state/ghremote/manifest.json](state/ghremote/manifest.json) file lists
all the query results already cached at GCS. Run:

```bash
uv run iqb cache pull -d ..
```

to sync files from GCS to the local copy.

Omit `-d ..` if running from the top-level directory.

Run `uv run iqb cache pull --help` for more help.

## `iqb pipeline run` - Generating Data

Run the pipeline to query BigQuery and populate the local cache:

```bash
uv run iqb pipeline run -d ..
```

This command loads `pipeline.yaml` to determine the query matrix and
executes BigQuery to generate data. If the cache already contains data, we
do not execute BigQuery to avoid burning cloud credits.

Omit `-d ..` if running from the top-level directory.

Run `uv run iqb pipeline run --help` for more help.

## `iqb cache status` - Checking Cache Status

Show which entries are local, remote, or missing:

```bash
uv run iqb cache status -d ..
```

Omit `-d ..` if running from the top-level directory.

Run `uv run iqb cache status --help` for more help.

### `iqb cache push` - Publishing Data

After generating new cache files locally using `iqb pipeline run`, push
them to GCS and update the manifest:

```bash
uv run iqb cache push -d ..
```

Then commit the updated `state/ghremote/manifest.json`.

Omit `-d ..` if running from the top-level directory.

Run `uv run iqb cache push --help` for more help.

## Bucket Setup

The GCS bucket we use was created in the `mlab-sandbox` project with:

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

## Cache Format

Raw query results stored efficiently as Parquet files for flexible analysis:

- **Location**: `./cache/v1/{start_date}/{end_date}/{query_type}/`
- **Files**:
  - `data.parquet` — query results (~1-60 MiB, streamable, chunked row groups)
  - `stats.json` — query metadata (start time, duration, bytes processed/billed, template hash)

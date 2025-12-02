# IQB Streamlit Prototype

Streamlit-based dashboard prototype for the Internet
Quality Barometer (IQB) project.

## Automatic Deployments

The dashboard is automatically published using Cloud Run:

| Trigger | Environment | URL |
|---------|-------------|-----|
| Push to `sandbox-*` branch | Sandbox | https://iqb.mlab-sandbox.measurementlab.net/ |
| Merge pull request | Staging | https://iqb-prototype-240028626237.us-east1.run.app/ |

Use sandbox deployments to see experimental changes or share them
with a colleague for feedback.

## Prerequisites

- Python 3.13 (see `.python-version` at repo root)

- [uv](https://astral.sh/uv) installed (see root README.md)

## Running Locally

```bash
# Enter into the prototype directory if you're in the repository root
cd prototype

# Sync dependencies (creates .venv if needed)
uv sync --dev

# Run the Streamlit app
uv run streamlit run Home.py
```

The app will be available at: http://localhost:8501

If you make changes to [Home.py](Home.py) or any other file in
this directory, Streamlit will reload on save.

## Project Structure

```
prototype/
├── Home.py              # Main Streamlit entry point
├── pyproject.toml       # Dependencies (streamlit, pandas, mlab-iqb)
└── README.md            # This file
```

## Testing with Docker

To test the Dockerfile locally before deploying:

```bash
# Build the image (from repo root)
cd ..
docker build -f prototype/Dockerfile -t iqb-prototype:test .

# Run the container
docker run --rm -p 8501:8501 iqb-prototype:test

# Access at http://localhost:8501
# Press Ctrl+C three or more times to stop
```

To run in background:

```bash
# Start container
docker run -d -p 8501:8501 --name iqb-test iqb-prototype:test

# Check logs
docker logs iqb-test

# Stop container
docker stop iqb-test
docker rm iqb-test
```

## Manual Deployment

For manual deployment to Cloud Run using Cloud Build:

```bash
# From the directory containing cloudbuild.yaml
gcloud builds submit --config=cloudbuild.yaml --project=mlab-sandbox
```

This will:

1. Build the Docker image from `prototype/Dockerfile`

2. Push to Artifact Registry

3. Deploy to Cloud Run in `us-east1` region

**Configuration:** See `cloudbuild.yaml` for deployment
settings (memory, CPU, region).

**Permissions required:**

- `roles/editor` - Deploy and update services

- `roles/run.admin` - Make new services public (see below)

**Making the service public:**

If deploying a new service or if you get 403 errors, an
admin needs to run this command:

```bash
gcloud run services add-iam-policy-binding iqb-prototype \
  --region=us-central1 \
  --member="allUsers" \
  --role="roles/run.invoker" \
  --project=mlab-sandbox
```

This IAM policy persists across deployments,
so it's only needed once.

## Dependencies

The prototype depends on:

- **streamlit** - Web framework

- **pandas** - Data manipulation

- **numpy** - Numerical operations

- **mlab-iqb** - IQB library (from [../library](../library),
managed via uv workspace)

Dependencies are locked in the workspace `uv.lock`
at the repository root.

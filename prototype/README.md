# IQB Streamlit Prototype

Streamlit-based dashboard for the Internet Quality Barometer (IQB) project.

## Prerequisites

- Python 3.13 (see `.python-version` at repo root)
- [uv](https://astral.sh/uv) installed (see root README.md)

## Running Locally

From the **repository root**:

```bash
# Sync dependencies (creates .venv if needed)
uv sync

# Run Streamlit app
cd prototype
uv run streamlit run Home.py
```

The app will be available at: http://localhost:8501

## Development Workflow

```bash
# Install/update dependencies after pulling changes
uv sync

# Run the app
cd prototype
uv run streamlit run Home.py

# Make changes to Home.py - Streamlit auto-reloads on save
```

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

## Dependencies

The prototype depends on:
- **streamlit** - Web framework
- **pandas** - Data manipulation
- **numpy** - Numerical operations
- **mlab-iqb** - IQB library (from `../library`, managed via uv workspace)

Dependencies are locked in the workspace `uv.lock` at the repository root.

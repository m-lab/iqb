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

## Dependencies

The prototype depends on:
- **streamlit** - Web framework
- **pandas** - Data manipulation
- **numpy** - Numerical operations
- **mlab-iqb** - IQB library (from `../library`, managed via uv workspace)

Dependencies are locked in the workspace `uv.lock` at the repository root.

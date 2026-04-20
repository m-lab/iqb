# Internet Quality Barometer (IQB)

[![Build Status](https://github.com/m-lab/iqb/actions/workflows/ci.yml/badge.svg)](https://github.com/m-lab/iqb/actions)
[![codecov](https://codecov.io/gh/m-lab/iqb/branch/main/graph/badge.svg)](https://codecov.io/gh/m-lab/iqb)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/m-lab/iqb)

IQB is an open-source framework developed by
[Measurement Lab (M-Lab)](https://www.measurementlab.net/) that computes a
composite Internet quality score across real-world use cases: web browsing,
video streaming, video conferencing, audio streaming, online backup, and
gaming. Unlike single-metric speed tests, IQB evaluates whether a connection
meets the network requirements of each use case and aggregates the results
into a single score between 0 and 1.

Read the conceptual background:

- [M-Lab blog post](https://www.measurementlab.net/blog/iqb/)
- [Detailed framework report (PDF)](https://www.measurementlab.net/publications/IQB_report_2025.pdf)
- [Executive summary (PDF)](https://www.measurementlab.net/publications/IQB_executive_summary_2025.pdf)
- [ACM IMC 2025 poster](https://arxiv.org/pdf/2509.19034)

Live staging dashboard: [iqb.mlab-staging.measurementlab.net](https://iqb.mlab-staging.measurementlab.net/)

---

## Repository Structure

| Directory | Description |
|-----------|-------------|
| `library/` | `mlab-iqb` Python package — scoring logic, cache API, data pipeline, CLI |
| `prototype/` | Streamlit web dashboard (Phase 1 prototype) |
| `analysis/` | Jupyter notebooks for research and experimentation |
| `data/` | Pipeline configuration and local Parquet cache |
| `docs/` | Documentation, design decision records, internals guide |

---

## Quick Start

### Requirements

- Python 3.13 (see `.python-version`)
- [uv](https://astral.sh/uv) — install with `brew install uv` on macOS or
  `sudo snap install astral-uv --classic` on Ubuntu

### Setup and Run

```bash
# Clone the repository
git clone git@github.com:m-lab/iqb.git
cd iqb

# Install all workspace dependencies
uv sync --dev

# Run the Streamlit prototype
cd prototype
uv run streamlit run Home.py
```

The dashboard will be available at `http://localhost:8501`.

### Using the Library

```python
from iqb import IQBCache, IQBCalculator, IQBDatasetGranularity, IQBRemoteCache

# Pull pre-computed data from GCS (requires gcloud auth)
cache = IQBCache(remote_cache=IQBRemoteCache())

# Load monthly country-level data
entry = cache.get_cache_entry(
    start_date="2025-10-01",
    end_date="2025-11-01",
    granularity=IQBDatasetGranularity.COUNTRY,
)

# Filter to a specific country and extract the median percentile
p50 = entry.mlab.read_data_frame_pair(country_code="US").to_iqb_data(percentile=50)

# Calculate the IQB score
score = IQBCalculator().calculate_iqb_score(data={"m-lab": p50.to_dict()})
print(f"IQB score: {score:.3f}")
```

See [`analysis/00-template.ipynb`](analysis/00-template.ipynb) for a complete
walkthrough.

### CLI

```bash
# Check which cache entries are available locally and remotely
uv run iqb cache status

# Pull pre-computed data from GCS to the local cache
uv run iqb cache pull -d data/

# Run the pipeline to generate new data from BigQuery
uv run iqb pipeline run -d data/
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/architecture.md](docs/architecture.md) | System overview, data flow, component responsibilities, extensibility |
| [docs/developer_guide.md](docs/developer_guide.md) | Local setup, adding metrics and pages, testing, contribution workflow |
| [docs/user_guide.md](docs/user_guide.md) | IQB for consumers, policymakers, researchers, and ISPs |
| [library/README.md](library/README.md) | Library API, testing, linting, type checking |
| [prototype/README.md](prototype/README.md) | Running locally, Docker, Cloud Run deployment |
| [data/README.md](data/README.md) | Pipeline commands, cache format, GCS configuration |
| [analysis/README.md](analysis/README.md) | Notebook usage and conventions |
| [docs/internals/](docs/internals/README.md) | Sequential guide to how the data pipeline works |
| [docs/design/](docs/design/README.md) | Architecture decision records |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development environment, VSCode setup, component workflows |

---

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) for
development environment setup and [docs/developer_guide.md](docs/developer_guide.md)
for guidance on adding metrics, use cases, and dashboard pages. All changes
require passing tests, Ruff linting, and Pyright type checks before merge.

---


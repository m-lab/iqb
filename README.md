# Internet Quality Barometer (IQB)

[![Build Status](https://github.com/m-lab/iqb/actions/workflows/ci.yml/badge.svg)](https://github.com/m-lab/iqb/actions) [![codecov](https://codecov.io/gh/m-lab/iqb/branch/main/graph/badge.svg)](https://codecov.io/gh/m-lab/iqb) [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/m-lab/iqb)

This repository contains the source for code the Internet Quality Barometer (IQB)
library, and related applications and notebooks.

## About IQB

IQB is an open-source project initiated by
[Measurement Lab (M-Lab)](https://www.measurementlab.net/).

IQB is motivated by the need to redefine how we measure and understand Internet
performance to keep pace with evolving technological demands and user
expectations. IQB is a comprehensive framework for collecting data and
calculating a composite score, the “IQB Score”, which reflects
the quality of Internet experience. IQB takes a more holistic approach
than “speed tests” and evaluates Internet performance across various
use cases (web browsing, video streaming, online gaming, etc.),
each with its own specific network requirements (latency, throughput, etc.).

Read more about the IQB framework in:

- M-Lab's [blog post](https://www.measurementlab.net/blog/iqb/).

- The IQB framework [detailed report](
https://www.measurementlab.net/publications/IQB_report_2025.pdf) and
[executive summary](
https://www.measurementlab.net/publications/IQB_executive_summary_2025.pdf).

- The IQB [poster](https://arxiv.org/pdf/2509.19034) at ACM IMC 2025.

## Repository Architecture

### **`docs/`**

Documentation, tutorials, design documents, and presentations.

See [docs/README.md](docs/README.md) for details.

### **`library/`**

The IQB library containing methods for calculating the IQB score and data collection.

See [library/README.md](library/README.md) for details.

### **`prototype/`**

A Streamlit web application for applying and parametrizing the IQB framework
in different use cases.

See [prototype/README.md](prototype/README.md) for how to run it locally.

### **`analysis/`**

Jupyter notebooks for exploratory data analysis, experimentation, and research.

See [analysis/README.md](analysis/README.md) for more information.

### **`data/`**

Workspace containing the default pipeline configuration, the default cache directory,
and instructions for generating new data using the pipeline.

See [data/README.md](data/README.md) for details.

### **`.iqb`**

Symbolic link to [data](data) that simplifies running the pipeline on Unix.

## Data Flow

The components above connect as follows:

```
BigQuery → [iqb pipeline run] → local cache/ → [IQBCache] → [IQBCalculator] → scores
                                      ↕
                              [iqb cache pull/push] ↔ GCS
```

The **pipeline** queries BigQuery for M-Lab NDT measurements and stores
percentile summaries as Parquet files in the local cache. To avoid expensive
re-queries, **`iqb cache pull`** can download pre-computed results from GCS
instead. The **`IQBCache`** API reads cached data, and **`IQBCalculator`**
applies quality thresholds and weights to produce IQB scores. The
**prototype** and **analysis notebooks** both consume scores through
these library APIs.

## Understanding the Codebase

- To learn **how the data pipeline works**, read the
[internals guide](docs/internals/README.md) — it walks through queries,
the pipeline, the remote cache, and the researcher API in sequence.

- To understand **why specific technical decisions were made**, see the
[design documents](docs/design/README.md) — architecture decision records
covering cache design, data distribution, and more.

## Quick Start

```bash
# Clone the repository
git clone git@github.com:m-lab/iqb.git
cd iqb

# Sync all dependencies (creates .venv automatically)
uv sync --dev

# Run the Streamlit prototype
cd prototype
uv run streamlit run Home.py
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for full development environment
setup, VSCode configuration, and component-specific workflows.

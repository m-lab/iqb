# Internet Quality Barometer (IQB)

[![Build Status](https://github.com/m-lab/iqb/actions/workflows/ci.yml/badge.svg)](https://github.com/m-lab/iqb/actions) [![codecov](https://codecov.io/gh/m-lab/iqb/branch/main/graph/badge.svg)](https://codecov.io/gh/m-lab/iqb) [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/m-lab/iqb)

This repository contains the source code for the Internet Quality Barometer (IQB)
[library](./library), [notebook templates](./analysis), and [prototype](./prototype).

The prototype is available online at: https://iqb.mlab-staging.measurementlab.net.

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

- [analysis](./analysis): Jupyter notebooks templates showing how to use
the IQB library for exploration and research. See
[analysis/README.md](analysis/README.md) for more information.

- [data](./data): Workspace containing the default pipeline configuration,
the default cache directory, and instructions for generating new
data using the pipeline. See [data/README.md](data/README.md) for details.

- [docs](./docs): Documentation, tutorials, design documents, and
presentations. See [docs/README.md](docs/README.md) for details.

- [library](./library): The IQB library containing methods for calculating
the IQB score and data collection. See
[library/README.md](library/README.md) for details.

- [prototype](./prototype): A [Streamlit](https://streamlit.io/) web application for applying
and parametrizing the IQB framework in different use cases. See
[prototype/README.md](prototype/README.md) for how to run it locally.

- [.iqb](./.iqb): Symbolic link to [data](data) that simplifies running
the pipeline on Unix-like systems.

- [.python-version](./.python-version): declaration of the Python version
that we use to develop the IQB framework.

- [pyproject.toml](./pyproject.toml): repository configuration declaring the
[uv](https://github.com/astral-sh/uv) workspace and its members.

- [uv.lock](./uv.lock): the [uv](https://github.com/astral-sh/uv) lockfile
declaring the dependencies used in this workspace.

## Data Flow by Use Case

The IQB framework computes IQB scores from
[Parquet](https://parquet.apache.org/) files obtained by querying
[BigQuery](https://www.measurementlab.net/data/docs/bq/quickstart/).

### IQB Users

As a user of the IQB framework, you use the IQB [library](./library) and
more specifically the `IQBCache` and `IQBRemoteCache` classes to get the
relevant Parquet files and compute the IQB score using the `IQBCalculator`.

```
scores <- [IQBCalculator] <- [IQBCache] <- localCache <- [IQBRemoteCache] <- GCS
```

More in detail:

1. the `IQBRemoteCache` downloads [Parquet](https://parquet.apache.org/) files
from Measurement Lab GCS and stores them in the local cache

2. the `IQBCache` reads [Parquet](https://parquet.apache.org/) files from
the local cache (allowing for filtering) and return data structures
compatible with the `IQBCalculator`

3. the `IQBCalculator` class computes the IQB scores

Directories of interest:

- [library](./library) for the IQB library source code

- [analysis](./analysis) for example usage via Jupyter notebooks

- [prototype](./prototype) for usage in the Streamlit prototype

### IQB Developers

As a developer of the IQB framework, you use the `IQBPipeline` class to run
[BigQuery](https://www.measurementlab.net/data/docs/bq/quickstart/) queries
and produce the related [Parquet](https://parquet.apache.org/) files. You
typically do this _indirectly_ via the `iqb` command line tool.

```
GCS <- [iqb cache push] <- parquet <- [iqb pipeline run] <- BigQuery
```

More in detail:

1. the `iqb pipeline run` command runs the IQB pipeline performing
[BigQuery](https://www.measurementlab.net/data/docs/bq/quickstart/)
queries and storing their results on the local disk as
[Parquet](https://parquet.apache.org/) files

2. the `iqb cache push` pushes the
[Parquet](https://parquet.apache.org/) files to
GCS (Google Cloud Storage)

Both commands internally use the `IQBPipeline` class.

Directories of interest:

- [data](./data): default configuration of the pipeline and temporary
storage ahead of submitting the data to GCS

- [library](./library): implementation of the CLI and of `IQBPipeline`

## Understanding the Codebase

- To learn **how the data pipeline works**, read the
[internals guide](docs/internals/README.md) — it walks through queries,
the pipeline, the remote cache, and the researcher API in sequence.

- To understand **why specific technical decisions were made**, see the
[design documents](docs/design/README.md) — architecture decision records
covering cache design, data distribution, and more.

## Quick Start

You need a supported Python version (see [.python-version](./.python-version)),
[uv](https://github.com/astral-sh/uv), and [git](https://git-scm.com/):

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

## License

```
SPDX-License-Identifier: Apache-2.0
```

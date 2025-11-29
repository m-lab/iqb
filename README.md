# Internet Quality Barometer (IQB)

[![Build Status](https://github.com/m-lab/iqb/actions/workflows/ci.yml/badge.svg)](https://github.com/m-lab/iqb/actions) [![codecov](https://codecov.io/gh/m-lab/iqb/branch/main/graph/badge.svg)](https://codecov.io/gh/m-lab/iqb)

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

Sample datasets used in the IQB app prototype and notebooks.

See [data/README.md](data/README.md) for details.

## Development Environment

We use [uv](https://astral.sh/uv) as a replacement for several Python repository
management tools such as `pip`, `poetry`, etc.

### Installing uv

On Ubuntu:

```bash
sudo snap install astral-uv --classic
```

On macOS:

```bash
brew install uv
```

On other platforms, see the [uv installation guide](https://docs.astral.sh/uv/getting-started/installation/).

### Quick Start

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

### Using VSCode

This repository is configured for VSCode with selected Python
development tools (Ruff, Pyright, pytest).

When you first open this repository with VSCode, it will prompt you
to install the required extensions for Python development.

Make sure you also read the following section to avoid `uv`
issues: there is no official `uv` extension for VSCode yet and
it seems more prudent to avoid using unofficial ones.

#### First-time uv setup

Running `uv sync --dev` creates the required `.venv` directory
that VSCode needs to find the proper python version and the proper
development tools.

If you open the repository using VSCode *before* running
`uv sync --dev`, you see the following error:

```
Unexpected error while trying to find the Ruff binary
```

To fix this, either run `uv sync --dev` from the command line or
use VSCode directly to run `uv` and reload:

1. Run the setup task:

    - Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on macOS)

    - Type "Tasks: Run Task"

    - Select **"IQB: Setup Development Environment"**

    - This runs `uv sync --dev` to install all development dependencies

2. After setup completes, reload VSCode:

    - Press `Ctrl+Shift+P` → "Developer: Reload Window"

    - The Ruff error should disappear

#### Available Tasks

Access them via `Ctrl+Shift+P` → "Tasks: Run Task":

- **IQB: Setup Development Environment** - Run `uv sync --dev` to install/update dependencies

- **IQB: Run Tests** - Run the pytest test suite

- **IQB: Run Ruff Check** - Check code style and quality

- **IQB: Run Pyright** - Run type checking

#### Extensions

VSCode will prompt to install these extensions:

- Python (`ms-python.python`)

- Pylance (`ms-python.vscode-pylance`)

- Ruff (`charliermarsh.ruff`)

# Contributing

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

## Component Workflows

Each component has its own README with specific development instructions:

- [library/README.md](library/README.md) — testing, linting, type checking, coding style

- [prototype/README.md](prototype/README.md) — running locally, Docker, deployment

- [data/README.md](data/README.md) — running the pipeline, cache management

- [analysis/README.md](analysis/README.md) — notebooks, testing notebooks

## Understanding the Codebase

- [docs/internals/](docs/internals/README.md) — sequential guide to how the data pipeline works

- [docs/design/](docs/design/README.md) — architecture decision records explaining why things were built this way

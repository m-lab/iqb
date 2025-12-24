# Research and Analyses

This directory contains template notebooks showing how
to use the IQB library for research purposes.

## Setup

The analysis directory is a workspace member with its own
dependencies. When you run

```bash
uv sync
```

from the repository root, we also install the dependencies
required for running the notebooks.

## Notebooks

### `00-template.ipynb`

Template demonstrating basic IQB usage:

- Loading cached data with `IQBCache`

- Computing scores with `IQBCalculator`

- Interpreting results

Use this as a starting point for custom analysis.

## Local Cache for Notebook Tests

The test suite seeds `analysis/.iqb` with a small cache snapshot to avoid
network downloads when executing notebooks in CI. If you need to refresh
or replace the cached month, update `analysis/.iqb` accordingly.

## Running Notebooks

### In VSCode

1. Open the notebook in VSCode

2. Select kernel: `.venv/bin/python` (from workspace root)

3. Run cells interactively

### In Jupyter

```bash
cd iqb/analysis
uv run jupyter notebook
```

## Testing

Notebooks are tested programmatically to ensure they execute
without errors:

```bash
# From repository root
uv run pytest analysis/tests/ -v
```

The test suite uses `nbconvert` to execute notebooks and
verify successful execution.

## Notebook Outputs and Version Control

**Current practice:** Clear notebook outputs before committing
to avoid merge conflicts.

In Jupyter or VSCode:

- Select "Clear All Outputs" before committing changes

**Future improvement:** Consider adding pre-commit hooks with `nbstripout`
to automatically strip outputs. This would:

- Prevent accidentally committing outputs

- Keep diffs clean and focused on code changes

- Reduce repository size

- Ensure reproducibility (forces execution on fresh checkout)

For now, manual clearing is sufficient for the small
number of notebooks in this repository.

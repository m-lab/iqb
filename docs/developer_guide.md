# IQB Developer Guide

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.13 | Specified in `.python-version` at repo root |
| uv | latest | Package and workspace manager |
| gcloud | any | Required only for BigQuery queries and GCS sync |

### Installing uv

On macOS:

```bash
brew install uv
```

On Ubuntu:

```bash
sudo snap install astral-uv --classic
```

For other platforms, see the [uv installation guide](https://docs.astral.sh/uv/getting-started/installation/).

---

## Local Setup

```bash
# Clone the repository
git clone git@github.com:m-lab/iqb.git
cd iqb

# Install all workspace dependencies, including dev tools
uv sync --dev
```

`uv sync --dev` creates a `.venv` directory at the repo root and installs all
dependencies defined across workspace members (`library/`, `prototype/`,
`analysis/`, `docs/internals/`). The `mlab-iqb` library package is installed
in editable mode from `library/`.

### VSCode Setup

Open the repo root in VSCode after running `uv sync --dev`. VSCode will
prompt you to install the recommended extensions:

- `ms-python.python`
- `ms-python.vscode-pylance`
- `charliermarsh.ruff`

If you open VSCode before running `uv sync --dev`, Ruff will report a binary
not found error. Fix it by running the **IQB: Setup Development Environment**
task from the Command Palette (`Cmd+Shift+P` → "Tasks: Run Task").

After setup, reload the window (`Cmd+Shift+P` → "Developer: Reload Window").

---

## Running the Prototype

```bash
cd prototype
uv run streamlit run Home.py
```

The app is available at `http://localhost:8501`. Streamlit reloads on file
save. To run with automatic reloading disabled (e.g., in a headless
environment):

```bash
uv run streamlit run Home.py --server.headless true
```

### Running with Docker

```bash
# Build from the repo root
docker build -f prototype/Dockerfile -t iqb-prototype:local .

# Run and access at http://localhost:8501
docker run --rm -p 8501:8501 iqb-prototype:local
```

---

## Code Structure Conventions

### Library (`library/src/iqb/`)

- Public constructors and top-level functions use **keyword-only arguments**.
  This makes call sites explicit and allows adding parameters without
  breaking callers. Prefer `IQBCache(remote_cache=...)` over positional args.
- Module-level constants (e.g., `IQB_CONFIG`) are the single source of truth
  for configuration. Avoid duplicating threshold values in multiple places.
- Type annotations are enforced by Pyright. All new public functions must be
  fully annotated.

### Prototype (`prototype/`)

- Scoring logic belongs in the library. The prototype only calls library APIs.
- `utils/calculation_utils.py` — functions that translate Streamlit session
  state into library-compatible data structures.
- `utils/data_utils.py` — data loading and filtering helpers.
- `utils/constants.py` — UI constants (colors, input ranges, weight bounds).
  Do not hardcode these values in page files.
- `visualizations/` — self-contained chart components. Pages import from
  here; they do not construct Plotly figures directly.
- `session_state.py` — defines and initialises all Streamlit session state
  keys. Add new state keys here, not inline in page files.
- `app_state.py` — the `IQBAppState` dataclass passed to utility functions.

---

## Adding a New Dashboard Page

Streamlit automatically discovers Python files in `prototype/pages/`.

1. Create `prototype/pages/My_Page.py`. The filename becomes the sidebar
   label (underscores are converted to spaces).
2. Import shared utilities:

```python
import streamlit as st
from app_state import IQBAppState
from session_state import get_app_state
from utils.data_utils import load_cache_data
```

3. Retrieve session state and render:

```python
state: IQBAppState = get_app_state()
st.title("My Page")
# Use state.manual_entry, state.thresholds, etc.
```

4. For charts, add a function in `visualizations/` and call it from the page.

---

## Adding a New Metric (Network Requirement)

Network requirements are the measurable properties evaluated per use case:
`download_throughput_mbps`, `upload_throughput_mbps`, `latency_ms`,
`packet_loss`.

To add a new metric, e.g. `jitter_ms`:

1. **Extend `IQB_CONFIG`** in `library/src/iqb/config.py`:

```python
"jitter_ms": {
    "w": 3,
    "threshold min": 30,
    "datasets": {
        "m-lab": {"w": 1},
        "cloudflare": {"w": 0},
        "ookla": {"w": 0},
    },
},
```

Add this block under each use case's `"network requirements"` dict where
the metric is relevant.

2. **Implement binary scoring** in `library/src/iqb/calculator.py`:

```python
elif network_requirement == "jitter_ms":
    return 1 if value < threshold else 0
```

3. **Update constants** in `prototype/utils/constants.py` if the metric
   needs UI input controls (input ranges, step values, display colours).

4. **Update `session_state.py`** to initialise a default value for the
   new metric in `manual_entry` and in any threshold override structures.

5. **Add tests** in `library/tests/` covering the new scoring branch and
   any edge cases (zero, boundary, above/below threshold).

---

## Adding a New Use Case

1. Add a new key to the `"use cases"` dict in `library/src/iqb/config.py`:

```python
"cloud gaming": {
    "w": 1,
    "network requirements": {
        "download_throughput_mbps": {"w": 4, "threshold min": 50, "datasets": {...}},
        "upload_throughput_mbps":   {"w": 4, "threshold min": 50, "datasets": {...}},
        "latency_ms":               {"w": 5, "threshold min": 10, "datasets": {...}},
        "packet_loss":              {"w": 4, "threshold min": 0.005, "datasets": {...}},
    },
},
```

2. `IQBCalculator` automatically includes the new use case in score
   computation without further code changes.

3. Update `prototype/utils/constants.py` to assign a display colour for
   the new use case in `USE_CASE_COLORS` if it will appear in charts.

---

## Extending IQB Scoring Logic Safely

- `IQB_CONFIG` is the only place weights and thresholds are defined.
  Do not hard-code numeric thresholds elsewhere.
- `IQBCalculator.calculate_iqb_score()` accepts a `data` dict keyed by
  dataset name, then by requirement name. Use this signature for all
  new consumers; do not modify the function signature.
- Use `copy.deepcopy(IQB_CONFIG)` when creating per-session overrides
  (as done in `prototype/utils/calculation_utils.py`). Never mutate the
  module-level `IQB_CONFIG` at runtime.
- The `config` parameter of `IQBCalculator` currently only supports `None`
  (uses the default). File-based configuration loading is stubbed and
  raises `NotImplementedError`. Extend this path when adding YAML/JSON
  configuration support.

---

## Where Caching Should Be Applied

| Layer | Mechanism | Location |
|-------|-----------|----------|
| BigQuery results | Parquet files | `data/cache/v1/` |
| Remote sharing | GCS via `iqb cache push/pull` | `gs://mlab-sandbox-iqb-us-central1` |
| Dashboard runtime | Static JSON per country/ASN | `prototype/cache/` |
| Streamlit session | `st.cache_data` / session state | `prototype/session_state.py` |

Use `@st.cache_data` for functions that load or transform data files. Avoid
caching at the page level; cache at the data-loading function level to allow
pages to share results across re-renders.

Use `IQBCache` (not direct Parquet reads) as the access layer in notebooks
and scripts to benefit from its filtering and error handling.

---

## Testing Strategy

Tests live in `library/tests/` and follow the `*_test.py` naming convention.

```bash
# Run all tests from the repo root
uv sync --dev
cd library
uv run pytest

# Run with coverage
uv run pytest --cov=src/iqb

# Run a specific test file
uv run pytest tests/iqb_score_test.py

# Run a specific test class or function
uv run pytest tests/iqb_score_test.py::TestIQBInitialization::test_init_with_name
```

### What to Test

- **Scoring correctness** — verify `calculate_iqb_score()` returns known
  values for known inputs. Cover boundary conditions (value exactly at
  threshold, zero weight, all-zero data).
- **Config integrity** — assert that all use cases and requirements in
  `IQB_CONFIG` have required keys (`w`, `threshold min`, `datasets`).
- **Cache reads** — use fixture Parquet files in `library/tests/fixtures/`
  to test `IQBCache` without a live GCS connection.
- **Pipeline queries** — unit-test SQL template rendering independently of
  BigQuery execution.

### Test File Structure

```python
"""tests/my_feature_test.py"""
from iqb import IQBCalculator

class TestMyFeature:
    def test_something(self):
        calculator = IQBCalculator()
        result = calculator.calculate_iqb_score(data={
            "m-lab": {
                "download_throughput_mbps": 50,
                "upload_throughput_mbps": 20,
                "latency_ms": 30,
                "packet_loss": 0.001,
            }
        })
        assert 0.0 <= result <= 1.0
```

---

## Code Quality

```bash
# Lint and auto-fix (from library/)
cd library
uv run ruff check --fix .
uv run ruff format .

# Type checking
uv run pyright
```

Ruff and Pyright configurations are in `library/pyproject.toml`. CI runs
both checks on all pushes and pull requests to `main`.

---

## Contribution Workflow

1. **Fork and branch** — create a feature branch from `main`:
   `git checkout -b feature/my-change`

2. **Sync dependencies** — run `uv sync --dev` after pulling changes to
   keep the lockfile consistent.

3. **Write tests first** for library changes. Aim for full coverage of new
   code paths in `library/tests/`.

4. **Run quality checks locally** before pushing:

   ```bash
   cd library
   uv run pytest
   uv run ruff check .
   uv run pyright
   ```

5. **Commit style** — write concise commit messages in the imperative mood
   (`Add jitter metric to config`, not `Added jitter metric`).

6. **Pull Request** — open a PR against `main`. CI will run tests, linting,
   and type checks. Address all failures before requesting review.

7. **Review and merge** — at least one maintainer review is required.
   Squash merge is preferred to keep the commit history linear.

See [CONTRIBUTING.md](../CONTRIBUTING.md) for component-specific workflows
and VSCode task configurations.

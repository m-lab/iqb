# IQB library

The IQB library contains methods for:

- Calculating the IQB score/formula (based on input measurements
data, weight matrices, and quality thresholds).

- Data collection, aggregation, and unification of raw measurements data.

## Installation

From the repository root:

```bash
# Install dependencies
uv sync

# For development (includes test dependencies)
uv sync --dev
```

## Usage

```python
from iqb import IQBCache, IQBCalculator, IQBDatasetGranularity, IQBRemoteCache

# Initialize cache (downloads data from GCS if not available locally)
cache = IQBCache(remote_cache=IQBRemoteCache())

# Initialize calculator with default IQB configuration
calculator = IQBCalculator()

# Load cached measurement data for US, October 2025
entry = cache.get_cache_entry(
    start_date="2025-10-01",
    end_date="2025-11-01",
    granularity=IQBDatasetGranularity.COUNTRY,
)

# Read M-Lab data filtered to the US
df_pair = entry.mlab.read_data_frame_pair(country_code="US")

# Extract the 50th percentile and convert for the calculator
p50 = df_pair.to_iqb_data(percentile=50)
data = {"m-lab": p50.to_dict()}

# Calculate IQB score
score = calculator.calculate_iqb_score(data=data)
print(f"IQB score: {score:.3f}")
```

See [analysis/00-template.ipynb](../analysis/00-template.ipynb) for a
complete walkthrough with step-by-step explanations.

## Command-Line Interface

The library provides an `iqb` command-line tool. Run `uv run iqb --help`
from the sources or `iqb --help` when installed for usage details.

## GCloud Configuration

To run BigQuery queries, you need to be logged in using the
user account subscribed to the [Measurement Lab mailing list](
https://www.measurementlab.net/data/docs/bq/quickstart/).

To install `gcloud` (on Ubuntu):

```bash
sudo snap install --classic google-cloud-sdk
```

Then login with:

```bash
gcloud auth application-default login
```

The billing project name should be set to `measurement-lab`
as illustrated in the example below:

```python
from iqb import IQBPipeline
pipeline = IQBPipeline(
  project="measurement-lab",
  data_dir=None,
)
```

You can then use `pipeline` to run queries up to a daily quota.

## Coding Style

We strongly prefer keyword-only arguments for public constructors (e.g.,
`IQBPipeline`, `IQBCache`) and functions, because they are harder to misuse
and they enable incremental refactoring.

## Running Tests

The library uses `pytest` for testing. Tests are located in the `tests/`
directory and follow the `*_test.py` naming convention.

```bash
# From the repository root, sync dev dependencies
uv sync --dev

# Run all tests
cd library
uv run pytest

# Run tests with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/iqb_score_test.py

# Run specific test class or function
uv run pytest tests/iqb_score_test.py::TestIQBInitialization
uv run pytest tests/iqb_score_test.py::TestIQBInitialization::test_init_with_name

# Get coverage
uv run pytest --cov=.
```

## Code Quality Tools

The library uses `ruff` for linting/formatting and
`pyright` for type checking.

### Linting with Ruff

Ruff is a fast Python linter that checks code style, potential
bugs, and code quality issues:

```bash
# From the library directory
cd library

# Check for linting issues
uv run ruff check .

# Check and auto-fix issues
uv run ruff check --fix .

# Format code (like Black)
uv run ruff format .

# Check specific files
uv run ruff check src/iqb/iqb_score.py
```

Ruff configuration is in `pyproject.toml` under `[tool.ruff]`.

### Type Checking with Pyright

Pyright performs static type analysis to catch type-related bugs:

```bash
# From the library directory
cd library

# Run type checking
uv run pyright

# Run with verbose output to verify it's checking the right files
uv run pyright --verbose
```

**Verifying Pyright is Working:**

Pyright can be silent if misconfigured. To verify it's actually
checking your code:

```bash
# This should show which files are being analyzed
uv run pyright --verbose

# Expected output should include:
# - "Loading pyproject.toml file at ..."
# - "Found X source files" (should be ~5 files)
# - Python version and search paths
# - "X errors, Y warnings, Z informations"
```

If you see `"Found 0 source files"`, the configuration is wrong.

To test that Pyright catches errors, temporarily introduce a type error:

```python
# In src/iqb/iqb_score.py, add:
x: int = "this should fail"  # Pyright should catch this!
```

If Pyright reports an error, it's working correctly. Remove the
test line afterwards.

Pyright configuration is in `pyproject.toml` under `[tool.pyright]`.

## Development

### Adding New Tests

Create new test files in the `tests/` directory following the
naming pattern `*_test.py`:

```python
"""tests/my_feature_test.py"""
from iqb import IQBCalculator

class TestMyFeature:
    def test_something(self):
        calculator = IQBCalculator()
        # Your test code here
        assert True
```

### Running Tests in CI

Tests run automatically on all pushes and pull requests to the
`main` branch via GitHub Actions. See `.github/workflows/ci.yml`
for the CI configuration.

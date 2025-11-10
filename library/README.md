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
from iqb import IQB

# Create an IQB instance
iqb = IQB(name='my_analysis')

# Calculate IQB score with default data
score = iqb.calculate_iqb_score()
print(f'IQB score: {score}')

# Calculate with detailed output
score = iqb.calculate_iqb_score(print_details=True)

# Print configuration
iqb.print_config()
```

## Running Tests

The library uses pytest for testing. Tests are located in the `tests/` directory and follow the `*_test.py` naming convention.

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
```

## Development

### Adding New Tests

Create new test files in the `tests/` directory following the naming pattern `*_test.py`:

```python
"""tests/my_feature_test.py"""
import pytest
from iqb import IQB

class TestMyFeature:
    def test_something(self):
        iqb = IQB()
        # Your test code here
        assert True
```

### Running Tests in CI

Tests run automatically on all pushes and pull requests to the main branch via GitHub Actions. See `.github/workflows/ci.yml` for the CI configuration.

"""Shared pytest fixtures for IQB tests."""

from pathlib import Path

import pytest


@pytest.fixture
def data_dir():
    """Return path to the repository's data/ directory."""
    # tests/conftest.py -> tests/ -> library/ -> repo root -> data
    return Path(__file__).parent.parent.parent / "data"

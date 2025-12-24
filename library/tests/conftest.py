"""Shared pytest fixtures for IQB tests."""

from pathlib import Path

import pytest


@pytest.fixture
def real_data_dir() -> Path:
    """Return path to the real-data cache fixtures directory."""
    return Path(__file__).parent / "fixtures" / "real-data"


@pytest.fixture
def fake_data_dir() -> Path:
    """Return path to the fake-data cache fixtures directory."""
    return Path(__file__).parent / "fixtures" / "fake-data"

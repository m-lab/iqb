"""Tests for the iqb.pipeline.cache module."""

from datetime import datetime
from pathlib import Path

import pytest

from iqb.pipeline.cache import (
    PipelineCacheEntry,
    PipelineCacheManager,
    _parse_both_dates,
    _parse_date,
    data_dir_or_default,
)


class TestDataDirOrDefault:
    """Test for data_dir_or_default function."""

    def test_data_dir_or_default_with_none(self):
        """Test default behavior when data_dir is None."""
        result = data_dir_or_default(None)
        expected = Path.cwd() / ".iqb"
        assert result == expected

    def test_data_dir_or_default_with_string(self, tmp_path):
        """Test conversion of string path."""
        test_path = str(tmp_path / "test")
        result = data_dir_or_default(test_path)
        assert result == Path(test_path)

    def test_data_dir_or_default_with_path(self, tmp_path):
        """Test pass-through of Path object."""
        input_path = tmp_path / "test"
        result = data_dir_or_default(input_path)
        assert result == input_path


class TestParseDate:
    """Test for _parse_date function."""

    def test_parse_date_valid(self):
        """Test parsing valid date string."""
        result = _parse_date("2024-10-01")
        assert result == datetime(2024, 10, 1)

    def test_parse_date_invalid_format(self):
        """Test error on invalid date format."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _parse_date("2024/10/01")

    def test_parse_date_invalid_date(self):
        """Test error on invalid date values."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _parse_date("2024-13-01")


class TestParseBothDates:
    """Test for _parse_both_dates function."""

    def test_parse_valid_date_range(self):
        """Test parsing valid date range."""
        start, end = _parse_both_dates("2024-10-01", "2024-11-01")
        assert start == datetime(2024, 10, 1)
        assert end == datetime(2024, 11, 1)

    def test_parse_equal_dates(self):
        """Test parsing when start equals end (valid for zero-duration queries)."""
        start, end = _parse_both_dates("2024-10-01", "2024-10-01")
        assert start == datetime(2024, 10, 1)
        assert end == datetime(2024, 10, 1)

    def test_parse_reversed_dates_error(self):
        """Test error when start_date > end_date."""
        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            _parse_both_dates("2024-11-01", "2024-10-01")

    def test_parse_invalid_start_date(self):
        """Test error on invalid start_date format."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _parse_both_dates("2024/10/01", "2024-11-01")

    def test_parse_invalid_end_date(self):
        """Test error on invalid end_date format."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _parse_both_dates("2024-10-01", "2024/11/01")

    def test_parse_leap_year(self):
        """Test parsing leap year date."""
        start, end = _parse_both_dates("2024-02-29", "2024-03-01")
        assert start == datetime(2024, 2, 29)
        assert end == datetime(2024, 3, 1)

    def test_parse_invalid_leap_year(self):
        """Test error on invalid leap year date."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _parse_both_dates("2023-02-29", "2023-03-01")

    def test_parse_year_boundary(self):
        """Test parsing dates across year boundary."""
        start, end = _parse_both_dates("2024-12-15", "2025-01-15")
        assert start == datetime(2024, 12, 15)
        assert end == datetime(2025, 1, 15)


class TestPipelineCacheManager:
    """Test for PipelineCacheManager class."""

    def test_init_default_data_dir(self):
        """Test manager initialization with default data directory."""
        manager = PipelineCacheManager()
        assert manager.data_dir == Path.cwd() / ".iqb"

    def test_init_custom_data_dir(self, tmp_path):
        """Test manager initialization with custom data directory."""
        custom_dir = tmp_path / "custom"
        manager = PipelineCacheManager(data_dir=custom_dir)
        assert manager.data_dir == custom_dir

    def test_get_cache_entry_returns_entry(self, tmp_path):
        """Test that get_cache_entry returns PipelineCacheEntry."""
        manager = PipelineCacheManager(data_dir=tmp_path)
        entry = manager.get_cache_entry(
            dataset_name="downloads_by_country",
            start_date="2024-10-01",
            end_date="2024-11-01",
        )

        assert isinstance(entry, PipelineCacheEntry)
        assert entry.data_dir == tmp_path
        assert entry.dataset_name == "downloads_by_country"
        assert entry.start_time == datetime(2024, 10, 1)
        assert entry.end_time == datetime(2024, 11, 1)

    def test_get_cache_entry_validates_dataset_name_rejects_invalid(self, tmp_path):
        """Test that get_cache_entry rejects invalid dataset names."""
        manager = PipelineCacheManager(data_dir=tmp_path)

        # Should reject hyphens
        with pytest.raises(ValueError, match="Invalid dataset name"):
            manager.get_cache_entry(
                dataset_name="invalid_dataset-name",
                start_date="2024-10-01",
                end_date="2024-11-01",
            )

        # Should reject uppercase letters
        with pytest.raises(ValueError, match="Invalid dataset name"):
            manager.get_cache_entry(
                dataset_name="InvalidDataset",
                start_date="2024-10-01",
                end_date="2024-11-01",
            )

        # Should reject special characters
        with pytest.raises(ValueError, match="Invalid dataset name"):
            manager.get_cache_entry(
                dataset_name="dataset@name",
                start_date="2024-10-01",
                end_date="2024-11-01",
            )

    def test_get_cache_entry_accepts_valid_dataset_names(self, tmp_path):
        """Test that get_cache_entry accepts valid dataset names including numbers."""
        manager = PipelineCacheManager(data_dir=tmp_path)

        # Should accept lowercase letters and underscores
        entry1 = manager.get_cache_entry(
            dataset_name="downloads_by_country",
            start_date="2024-10-01",
            end_date="2024-11-01",
        )
        assert entry1.dataset_name == "downloads_by_country"

        # Should accept numbers (new requirement)
        entry2 = manager.get_cache_entry(
            dataset_name="downloads_by_country_subdivision1",
            start_date="2024-10-01",
            end_date="2024-11-01",
        )
        assert entry2.dataset_name == "downloads_by_country_subdivision1"

        # Should accept names starting with numbers
        entry3 = manager.get_cache_entry(
            dataset_name="2024_dataset",
            start_date="2024-10-01",
            end_date="2024-11-01",
        )
        assert entry3.dataset_name == "2024_dataset"

        # Should accept all lowercase alphanumeric with underscores
        entry4 = manager.get_cache_entry(
            dataset_name="test_123_data_v2",
            start_date="2024-10-01",
            end_date="2024-11-01",
        )
        assert entry4.dataset_name == "test_123_data_v2"

    def test_get_cache_entry_validates_dates(self, tmp_path):
        """Test that get_cache_entry validates date range."""
        manager = PipelineCacheManager(data_dir=tmp_path)

        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            manager.get_cache_entry(
                dataset_name="downloads_by_country",
                start_date="2024-11-01",
                end_date="2024-10-01",
            )


class TestPipelineCacheEntry:
    """Test for PipelineCacheEntry class."""

    def test_dir_path_construction(self, tmp_path):
        """Test that dir_path constructs correct cache directory path."""
        entry = PipelineCacheEntry(
            data_dir=tmp_path,
            dataset_name="downloads_by_country",
            start_time=datetime(2024, 10, 1),
            end_time=datetime(2024, 11, 1),
        )

        expected = (
            tmp_path
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )
        assert entry.dir_path() == expected

    def test_data_path_when_file_exists(self, tmp_path):
        """Test data_path returns path when file exists."""
        entry = PipelineCacheEntry(
            data_dir=tmp_path,
            dataset_name="downloads_by_country",
            start_time=datetime(2024, 10, 1),
            end_time=datetime(2024, 11, 1),
        )

        # Create the file
        cache_dir = entry.dir_path()
        cache_dir.mkdir(parents=True, exist_ok=True)
        data_file = cache_dir / "data.parquet"
        data_file.write_text("fake data")

        assert entry.data_parquet_file_path() == data_file
        assert entry.data_parquet_file_path().exists()

    def test_data_path_when_file_missing(self, tmp_path):
        """Test data_path returns None when file doesn't exist."""
        entry = PipelineCacheEntry(
            data_dir=tmp_path,
            dataset_name="downloads_by_country",
            start_time=datetime(2024, 10, 1),
            end_time=datetime(2024, 11, 1),
        )

        data_file = entry.dir_path() / "data.parquet"
        assert entry.data_parquet_file_path() == data_file
        assert not entry.data_parquet_file_path().exists()

    def test_stats_path_when_file_exists(self, tmp_path):
        """Test stats_path returns path when file exists."""
        entry = PipelineCacheEntry(
            data_dir=tmp_path,
            dataset_name="downloads_by_country",
            start_time=datetime(2024, 10, 1),
            end_time=datetime(2024, 11, 1),
        )

        # Create the file
        cache_dir = entry.dir_path()
        cache_dir.mkdir(parents=True, exist_ok=True)
        stats_file = cache_dir / "stats.json"
        stats_file.write_text("{}")

        assert entry.stats_json_file_path() == stats_file
        assert entry.stats_json_file_path().exists()

    def test_stats_path_when_file_missing(self, tmp_path):
        """Test stats_path returns None when file doesn't exist."""
        entry = PipelineCacheEntry(
            data_dir=tmp_path,
            dataset_name="downloads_by_country",
            start_time=datetime(2024, 10, 1),
            end_time=datetime(2024, 11, 1),
        )

        stats_file = entry.dir_path() / "stats.json"
        assert entry.stats_json_file_path() == stats_file
        assert not entry.stats_json_file_path().exists()

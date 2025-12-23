"""Tests for the iqb.pipeline.pqread module."""

from pathlib import Path

import pytest

from iqb.pipeline.pqread import iqb_parquet_read


@pytest.fixture
def test_data_path() -> Path:
    """Fixture that returns the path to the test cache parquet file."""
    return (
        Path(__file__).parent.parent.parent
        / "fixtures"
        / "cache"
        / "v1"
        / "20241001T000000Z"
        / "20241101T000000Z"
        / "downloads_by_country_city"
        / "data.parquet"
    )


class TestIQBParquetRead:
    """Test for iqb_parquet_read function."""

    def test_simple_read(self, test_data_path: Path):
        df = iqb_parquet_read(test_data_path)
        assert len(df) == 5
        assert len(df.columns) == 7

    def test_with_country_filter(self, test_data_path: Path):
        df = iqb_parquet_read(test_data_path, country_code="US")
        assert len(df) == 3
        assert len(df.columns) == 7
        assert all(df["country_code"] == "US")

    def test_with_asn_filter(self, test_data_path: Path):
        df = iqb_parquet_read(test_data_path, asn=101)
        assert len(df) == 2
        assert len(df.columns) == 7
        assert all(df["asn"] == 101)

    def test_with_subdivision1_filter(self, test_data_path: Path):
        df = iqb_parquet_read(test_data_path, subdivision1="California")
        assert len(df) == 2
        assert len(df.columns) == 7
        assert all(df["subdivision1_name"] == "California")

    def test_with_city_filter(self, test_data_path: Path):
        df = iqb_parquet_read(test_data_path, city="New York City")
        assert len(df) == 1
        assert len(df.columns) == 7
        assert all(df["city"] == "New York City")

    def test_with_multiple_filters(self, test_data_path: Path):
        df = iqb_parquet_read(test_data_path, country_code="US", subdivision1="California")
        assert len(df) == 2
        assert len(df.columns) == 7
        assert all(df["country_code"] == "US")
        assert all(df["subdivision1_name"] == "California")

    def test_with_column_filter(self, test_data_path: Path):
        df = iqb_parquet_read(
            test_data_path,
            columns=[
                "country_code",
                "sample_count",
                "download_p95",
            ],
        )
        assert len(df) == 5
        assert len(df.columns) == 3
        assert list(df.columns) == ["country_code", "sample_count", "download_p95"]

    def test_nonexisting_file_throws(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _ = iqb_parquet_read(tmp_path / "data.parquet")

    def test_nonexisting_column_throws(self, test_data_path: Path):
        with pytest.raises(ValueError):
            _ = iqb_parquet_read(test_data_path, columns=["antani", "mascetti"])

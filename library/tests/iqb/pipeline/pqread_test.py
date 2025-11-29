"""Tests for the iqb.pipeline.pqread module."""

from pathlib import Path

import pytest

from iqb.pipeline.pqread import iqb_parquet_read


def _basepath() -> Path:
    return Path(__file__).parent.parent.parent.parent.parent


def _country_dataset() -> Path:
    return (
        _basepath()
        / "data"
        / "cache"
        / "v1"
        / "20241001T000000Z"
        / "20241101T000000Z"
        / "downloads_by_country"
        / "data.parquet"
    )


class TestIQBParquetRead:
    """Test for iqb_parquet_read function."""

    def test_country_dataset_simple(self):
        fullpath = _country_dataset()
        df = iqb_parquet_read(fullpath)
        assert len(df) == 236
        assert len(df.columns) == 29

    def test_country_dataset_with_country_filter(self):
        fullpath = _country_dataset()
        df = iqb_parquet_read(fullpath, country_code="IT")
        assert len(df) == 1
        assert len(df.columns) == 29

    def test_country_dataset_with_column_filter(self):
        fullpath = _country_dataset()
        df = iqb_parquet_read(
            fullpath,
            columns=[
                "country_code",
                "sample_count",
                "download_p95",
                "loss_p95",
            ],
        )
        assert len(df) == 236
        assert len(df.columns) == 4

    def test_nonexisting_file_throws(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _ = iqb_parquet_read(tmp_path / "data.parquet")

    def test_nonexisting_column_throws(self):
        with pytest.raises(ValueError):
            _ = iqb_parquet_read(_country_dataset(), columns=["antani", "mascetti"])

    def test_asn_filter_with_country_dataset_throws(self):
        with pytest.raises(ValueError):
            _ = iqb_parquet_read(_country_dataset(), asn=137)

    def test_city_filter_with_country_dataset_throws(self):
        with pytest.raises(ValueError):
            _ = iqb_parquet_read(_country_dataset(), city="Boston")

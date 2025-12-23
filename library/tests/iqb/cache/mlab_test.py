"""Tests for the iqb.cache.mlab module."""

from pathlib import Path

import pandas as pd
import pytest

from iqb import IQBDatasetGranularity
from iqb.cache.mlab import (
    MLabCacheEntry,
    MLabCacheManager,
    MLabDataFramePair,
)
from iqb.pipeline.cache import PipelineCacheManager


def _create_manager(data_dir: str) -> MLabCacheManager:
    manager = PipelineCacheManager(data_dir=data_dir)
    return MLabCacheManager(manager)


def _get_country_cache_entry_2024_10(manager: MLabCacheManager) -> MLabCacheEntry:
    return manager.get_cache_entry(
        start_date="2024-10-01",
        end_date="2024-11-01",
        granularity=IQBDatasetGranularity.COUNTRY,
    )


def _get_country_city_cache_entry_2024_10(manager: MLabCacheManager) -> MLabCacheEntry:
    return manager.get_cache_entry(
        start_date="2024-10-01",
        end_date="2024-11-01",
        granularity=IQBDatasetGranularity.COUNTRY_CITY,
    )


@pytest.fixture
def cache_fixture_dir() -> Path:
    """Return path to the test cache fixture directory."""
    return Path(__file__).parent.parent.parent / "fixtures"


class TestMLabCacheManagerIntegration:
    """Integration tests for MLabCacheManager class."""

    def test_read_download_dataframe(self, data_dir):
        """Test that IQBCache uses .iqb/ directory by default."""
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        download_df = entry.read_download_data_frame()

        assert not download_df.empty
        assert "country_code" in download_df.columns
        assert "sample_count" in download_df.columns
        assert "download_p95" in download_df.columns
        assert "latency_p95" in download_df.columns
        assert "loss_p95" in download_df.columns
        assert len(download_df) == 236

    def test_read_upload_dataframe(self, data_dir):
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        upload_df = entry.read_upload_data_frame()

        assert not upload_df.empty
        assert "country_code" in upload_df.columns
        assert "sample_count" in upload_df.columns
        assert "upload_p95" in upload_df.columns
        assert len(upload_df) == 237

    def test_filter_by_country_code(self, data_dir):
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        us_download_df = entry.read_download_data_frame(country_code="US")

        assert len(us_download_df) == 1
        assert us_download_df.iloc[0]["country_code"] == "US"
        assert us_download_df.iloc[0]["sample_count"] == 31443312
        assert us_download_df.iloc[0]["download_p95"] == 625.6932041848493
        assert us_download_df.iloc[0]["loss_p95"] == 0.0
        assert us_download_df.iloc[0]["latency_p95"] == 0.806

        us_upload_df = entry.read_upload_data_frame(country_code="US")
        assert len(us_upload_df) == 1
        assert us_upload_df.iloc[0]["country_code"] == "US"
        assert us_upload_df.iloc[0]["sample_count"] == 24288961
        assert us_upload_df.iloc[0]["upload_p95"] == 370.487725107692

    def test_filter_by_subdivision1(self, cache_fixture_dir):
        manager = _create_manager(cache_fixture_dir)
        entry = _get_country_city_cache_entry_2024_10(manager)

        ak_download_df = entry.read_download_data_frame(
            country_code="US",
            subdivision1="California",
            city="Los Angeles",
        )

        assert len(ak_download_df) == 1
        assert ak_download_df.iloc[0]["country_code"] == "US"
        assert ak_download_df.iloc[0]["subdivision1_name"] == "California"
        assert ak_download_df.iloc[0]["city"] == "Los Angeles"
        assert ak_download_df.iloc[0]["sample_count"] == 100
        assert ak_download_df.iloc[0]["download_p95"] == 120.5
        assert ak_download_df.iloc[0]["loss_p95"] == 0.10

        ak_upload_df = entry.read_upload_data_frame(
            country_code="US",
            subdivision1="California",
            city="Los Angeles",
        )
        assert len(ak_upload_df) == 1
        assert ak_upload_df.iloc[0]["country_code"] == "US"
        assert ak_upload_df.iloc[0]["subdivision1_name"] == "California"
        assert ak_upload_df.iloc[0]["city"] == "Los Angeles"
        assert ak_upload_df.iloc[0]["sample_count"] == 100
        assert ak_upload_df.iloc[0]["upload_p95"] == 60.25

    def test_column_projection(self, data_dir):
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        limited_download_df = entry.read_download_data_frame(
            country_code="US",
            columns=[
                "country_code",
                "sample_count",
                "download_p50",
                "latency_p50",
            ],
        )
        assert len(limited_download_df.columns) == 4
        assert "sample_count" in limited_download_df.columns
        assert "download_p50" in limited_download_df.columns
        assert "latency_p50" in limited_download_df.columns
        assert "loss_p95" not in limited_download_df.columns  # Not requested

    def test_read_multiple_countries(self, data_dir):
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        countries = ("US", "DE", "BR", "IT", "FR", "IN", "DE")
        for country_code in countries:
            ddf = entry.read_download_data_frame(country_code=country_code)
            assert len(ddf) == 1
            assert ddf.iloc[0]["country_code"] == country_code
            assert ddf.iloc[0]["sample_count"] > 0
            assert ddf.iloc[0]["download_p95"] > 0

            udf = entry.read_upload_data_frame(country_code=country_code)
            assert len(udf) == 1
            assert udf.iloc[0]["country_code"] == country_code
            assert udf.iloc[0]["sample_count"] > 0
            assert udf.iloc[0]["upload_p95"] > 0

    def test_read_data_frame_pair(self, data_dir):
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        pair = entry.read_data_frame_pair(country_code="US")

        assert pair is not None
        assert len(pair.download) == 1
        assert len(pair.upload) == 1

        assert "download_p95" in pair.download.columns
        assert "download_p50" in pair.download.columns
        assert "upload_p95" in pair.upload.columns
        assert "upload_p50" in pair.upload.columns

    def test_read_data_frame_pair_with_subdivision1(self, cache_fixture_dir):
        manager = _create_manager(cache_fixture_dir)
        entry = _get_country_city_cache_entry_2024_10(manager)

        pair = entry.read_data_frame_pair(
            country_code="US",
            city="Los Angeles",
            subdivision1="California",
        )

        assert len(pair.download) == 1
        assert len(pair.upload) == 1
        assert pair.download.iloc[0]["subdivision1_name"] == "California"
        assert pair.upload.iloc[0]["subdivision1_name"] == "California"
        assert pair.download.iloc[0]["download_p95"] == 120.5
        assert pair.upload.iloc[0]["upload_p95"] == 60.25

    def test_convert_to_dict_p95(self, data_dir):
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        pair = entry.read_data_frame_pair(country_code="US")

        data_p95 = pair.to_iqb_data(percentile=95).to_dict()

        assert "download_throughput_mbps" in data_p95
        assert "upload_throughput_mbps" in data_p95
        assert "latency_ms" in data_p95
        assert "packet_loss" in data_p95

        assert isinstance(data_p95["download_throughput_mbps"], float)
        assert isinstance(data_p95["upload_throughput_mbps"], float)
        assert isinstance(data_p95["latency_ms"], float)
        assert isinstance(data_p95["packet_loss"], float)

        assert data_p95["download_throughput_mbps"] == 625.6932041848493
        assert data_p95["upload_throughput_mbps"] == 370.487725107692

        assert data_p95["latency_ms"] == 0.806
        assert data_p95["packet_loss"] == 0.0

    def test_convert_to_dict_default_percentile(self, data_dir):
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        pair = entry.read_data_frame_pair(country_code="US")

        data_p95 = pair.to_iqb_data(percentile=95).to_dict()

        data_default = pair.to_iqb_data().to_dict()
        assert data_default == data_p95

    def test_convert_to_dict_multiple_percentiles(self, data_dir):
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        pair = entry.read_data_frame_pair(country_code="US")

        data_p95 = pair.to_iqb_data(percentile=95).to_dict()

        data_p50 = pair.to_iqb_data(percentile=50).to_dict()

        # Median values should generally be different from p95
        assert (
            data_p95["download_throughput_mbps"] != data_p50["download_throughput_mbps"]
            or data_p95["upload_throughput_mbps"] != data_p50["upload_throughput_mbps"]
            or data_p95["latency_ms"] != data_p50["latency_ms"]
            or data_p95["packet_loss"] != data_p50["packet_loss"]
        )

    def test_read_data_frame_pair_granularity_error_city(self, data_dir):
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        with pytest.raises(ValueError):
            _ = entry.read_data_frame_pair(country_code="US", city="Boston")

    def test_read_data_frame_pair_granularity_error_asn(self, data_dir):
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        with pytest.raises(ValueError):
            _ = entry.read_data_frame_pair(country_code="US", asn=137)

    def test_get_cache_entry_with_missing_on_disk_data(self, tmp_path):
        manager = _create_manager(tmp_path)
        with pytest.raises(FileNotFoundError):
            _ = _get_country_cache_entry_2024_10(manager)

    def test_get_data_successful(self, data_dir):
        manager = _create_manager(data_dir)
        data = manager.get_data(
            granularity=IQBDatasetGranularity.COUNTRY,
            country_code="US",
            start_date="2024-10-01",
            end_date="2024-11-01",
            percentile=95,
        )

        assert data["download_throughput_mbps"] == 625.6932041848493
        assert data["upload_throughput_mbps"] == 370.487725107692

        assert data["latency_ms"] == 0.806
        assert data["packet_loss"] == 0.0

    def test_download_stats_property(self, data_dir):
        """Test backward compatibility property download_stats."""
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        # Use the backward compatibility property
        stats_path = entry.download_stats

        # Verify it returns the correct path
        assert stats_path == entry.download.stats_json_file_path()
        assert stats_path.exists()
        assert stats_path.name == "stats.json"

    def test_upload_stats_property(self, data_dir):
        """Test backward compatibility property upload_stats."""
        manager = _create_manager(data_dir)
        entry = _get_country_cache_entry_2024_10(manager)

        # Use the backward compatibility property
        stats_path = entry.upload_stats

        # Verify it returns the correct path
        assert stats_path == entry.upload.stats_json_file_path()
        assert stats_path.exists()
        assert stats_path.name == "stats.json"


class TestMLabDataFramePairExceptions:
    def test_download_multiple_rows_raises(self):
        df_download = pd.DataFrame({"download_p95": [100, 200]})
        df_upload = pd.DataFrame({"upload_p95": [50]})
        pair = MLabDataFramePair(download=df_download, upload=df_upload)

        with pytest.raises(ValueError, match="Expected exactly 1 row in download DataFrame"):
            pair.to_iqb_data().to_dict()

    def test_upload_multiple_rows_raises(self):
        df_download = pd.DataFrame({"download_p95": [100]})
        df_upload = pd.DataFrame({"upload_p95": [50, 75]})
        pair = MLabDataFramePair(download=df_download, upload=df_upload)

        with pytest.raises(ValueError, match="Expected exactly 1 row in upload DataFrame"):
            pair.to_iqb_data().to_dict()

    @pytest.mark.parametrize(
        "missing_column",
        [
            "download_p95",
            "latency_p95",
            "loss_p95",
        ],
    )
    def test_missing_download_columns_raises(self, missing_column):
        columns = {"download_p95": [100], "latency_p95": [10], "loss_p95": [0.1]}
        columns.pop(missing_column)
        df_download = pd.DataFrame(columns)
        df_upload = pd.DataFrame({"upload_p95": [50]})

        pair = MLabDataFramePair(download=df_download, upload=df_upload)

        with pytest.raises(ValueError, match=f"Percentile column '{missing_column}'"):
            pair.to_iqb_data().to_dict()

    def test_missing_upload_column_raises(self):
        df_download = pd.DataFrame(
            {
                "download_p95": [100],
                "latency_p95": [10],
                "loss_p95": [0.1],
            }
        )
        df_upload = pd.DataFrame({"upload_p90": [50]})  # Wrong percentile

        pair = MLabDataFramePair(download=df_download, upload=df_upload)

        with pytest.raises(ValueError, match="Percentile column 'upload_p95'"):
            pair.to_iqb_data().to_dict()

    def test_custom_percentile_missing_columns_raises(self):
        df_download = pd.DataFrame({"download_p50": [100], "latency_p50": [10], "loss_p50": [0.1]})
        df_upload = pd.DataFrame({"upload_p50": [50]})

        pair = MLabDataFramePair(download=df_download, upload=df_upload)

        # percentile=95 will be missing
        with pytest.raises(ValueError, match="Percentile column 'download_p95'"):
            pair.to_iqb_data(percentile=95).to_dict()

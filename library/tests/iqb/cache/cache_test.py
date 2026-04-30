"""Tests for the iqb.cache.cache module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from iqb import IQBCache, IQBDatasetGranularity
from iqb.pipeline.pipeline import PipelineRemoteCache


class TestIQBCacheInitialization:
    """Tests for IQBCache class initialization."""

    def test_init_with_default_data_dir(self):
        """Test that IQBCache uses .iqb/ directory by default."""
        cache = IQBCache()
        assert cache.data_dir.name == ".iqb"
        # Should be in current working directory
        assert cache.data_dir.parent.samefile(".")

    def test_init_with_custom_data_dir(self):
        """Test that IQBCache can be instantiated with custom data_dir."""
        cache = IQBCache(data_dir="/custom/path")
        assert cache.data_dir == Path("/custom/path")

    def test_init_with_remote_cache(self):
        """Test that we properly configure a remote cache."""
        mock_remote_cache = Mock(spec=PipelineRemoteCache)
        cache = IQBCache(remote_cache=mock_remote_cache)
        assert cache.manager.remote_cache == mock_remote_cache


class TestIQBCacheGetData:
    """Tests for IQBCache get_data method."""

    def test_get_data_us_october_2024(self, real_data_dir):
        """Test fetching US data for October 2024."""
        cache = IQBCache(data_dir=real_data_dir)
        data = cache.get_data(
            country="US",
            start_date=datetime(2024, 10, 1),
        )

        # Unwrap the `m-lab` part
        assert "m-lab" in data
        data = data["m-lab"]

        # Check structure
        assert "download_throughput_mbps" in data
        assert "upload_throughput_mbps" in data
        assert "latency_ms" in data
        assert "packet_loss" in data

        # Values should be numeric
        assert isinstance(data["download_throughput_mbps"], (int, float))
        assert isinstance(data["upload_throughput_mbps"], (int, float))
        assert isinstance(data["latency_ms"], (int, float))
        assert isinstance(data["packet_loss"], (int, float))

    def test_get_data_de_october_2024(self, real_data_dir):
        """Test fetching Germany data for October 2024."""
        cache = IQBCache(data_dir=real_data_dir)
        data = cache.get_data(
            country="DE",
            start_date=datetime(2024, 10, 1),
        )

        # Unwrap the `m-lab` part
        assert "m-lab" in data
        data = data["m-lab"]

        assert "download_throughput_mbps" in data
        assert isinstance(data["download_throughput_mbps"], (int, float))

    def test_get_data_br_october_2024(self, real_data_dir):
        """Test fetching Brazil data for October 2024."""
        cache = IQBCache(data_dir=real_data_dir)
        data = cache.get_data(
            country="BR",
            start_date=datetime(2024, 10, 1),
        )

        # Unwrap the `m-lab` part
        assert "m-lab" in data
        data = data["m-lab"]

        assert "download_throughput_mbps" in data
        assert isinstance(data["download_throughput_mbps"], (int, float))

    def test_get_data_case_insensitive_country(self, real_data_dir):
        """Test that country code is case-insensitive."""
        cache = IQBCache(data_dir=real_data_dir)

        data_upper = cache.get_data(country="US", start_date=datetime(2024, 10, 1))
        data_lower = cache.get_data(country="us", start_date=datetime(2024, 10, 1))

        # Should return same data regardless of case
        assert data_upper == data_lower

    def test_get_data_with_different_percentile(self, real_data_dir):
        """Test extracting different percentile values."""
        cache = IQBCache(data_dir=real_data_dir)

        data_p95 = cache.get_data(country="US", start_date=datetime(2024, 10, 1), percentile=95)
        data_p50 = cache.get_data(country="US", start_date=datetime(2024, 10, 1), percentile=50)

        # Unwrap the `m-lab` part
        assert "m-lab" in data_p95
        data_p95 = data_p95["m-lab"]

        assert "m-lab" in data_p50
        data_p50 = data_p50["m-lab"]

        # p95 should be higher than p50 for throughput metrics (higher percentile = higher speed)
        assert data_p95["download_throughput_mbps"] > data_p50["download_throughput_mbps"]
        assert data_p95["upload_throughput_mbps"] > data_p50["upload_throughput_mbps"]

        # p95 should be lower than p50 for latency (inverted: p95 label = p5 raw = best latency)
        assert data_p95["latency_ms"] < data_p50["latency_ms"]

    def test_get_data_france_october_2024(self, real_data_dir):
        """Test fetching France data for October 2024."""
        cache = IQBCache(data_dir=real_data_dir)
        data = cache.get_data(
            country="FR",
            start_date=datetime(2024, 10, 1),
        )

        # Unwrap the `m-lab` part
        assert "m-lab" in data
        data = data["m-lab"]

        assert "download_throughput_mbps" in data
        assert isinstance(data["download_throughput_mbps"], (int, float))

    def test_get_data_canada_october_2024(self, real_data_dir):
        """Test fetching Canada data for October 2024."""
        cache = IQBCache(data_dir=real_data_dir)
        data = cache.get_data(
            country="CA",
            start_date=datetime(2024, 10, 1),
        )

        # Unwrap the `m-lab` part
        assert "m-lab" in data
        data = data["m-lab"]

        assert "download_throughput_mbps" in data
        assert isinstance(data["download_throughput_mbps"], (int, float))

    def test_get_data_unavailable_country_raises_error(self, real_data_dir):
        """Test that requesting data for unavailable country raises FileNotFoundError."""
        cache = IQBCache(data_dir=real_data_dir)

        # Use a fictional country code that won't exist
        with pytest.raises(
            ValueError,
            match="Expected exactly 1 row in download DataFrame, but got 0 rows",
        ):
            cache.get_data(country="ZZ", start_date=datetime(2024, 10, 1))

    def test_get_data_unavailable_date_raises_error(self, real_data_dir):
        """Test that requesting data for unavailable date raises FileNotFoundError."""
        cache = IQBCache(data_dir=real_data_dir)

        with pytest.raises(FileNotFoundError, match=r"Cache entry not found"):
            cache.get_data(country="US", start_date=datetime(2024, 11, 1))


class TestIQBCacheGetCacheEntry:
    """Tests for IQBCache.get_cache_entry method."""

    def test_get_data_us_october_2024(self, real_data_dir):
        """Test fetching US data for October 2024."""
        # Create the cache
        cache = IQBCache(data_dir=real_data_dir)

        # Get the cache entry
        entry = cache.get_cache_entry(
            start_date="2024-10-01",
            end_date="2024-11-01",
            granularity=IQBDatasetGranularity.COUNTRY,
        )

        # Read the data frame pair filtering for US
        pair = entry.mlab.read_data_frame_pair(
            country_code="US",
        )

        # Obtain the p50
        p50 = pair.to_iqb_data(percentile=50)

        # Assert some properties
        assert p50.download == 96.35647029849147
        assert p50.upload == 20.958955220895582
        assert p50.latency == 16.13
        assert p50.loss == 0.0005169056182326177

    def test_get_data_de_october_2024(self, real_data_dir):
        """Test fetching Germany data for October 2024."""
        # Create the cache
        cache = IQBCache(data_dir=real_data_dir)

        # Get the cache entry
        entry = cache.get_cache_entry(
            start_date="2024-10-01",
            end_date="2024-11-01",
            granularity=IQBDatasetGranularity.COUNTRY,
        )

        # Read the data frame pair filtering for DE
        pair = entry.mlab.read_data_frame_pair(
            country_code="DE",
        )

        # Obtain the p50
        p50 = pair.to_iqb_data(percentile=50)

        # Assert some properties
        assert p50.download == 45.26447184187283
        assert p50.upload == 17.168405411624306
        assert p50.latency == 17.716
        assert p50.loss == 0.00034476733492527604


class TestIQBCacheAllFiles:
    """Test that all expected cache files are accessible."""

    def test_all_countries_october_2024(self, real_data_dir):
        """Test that all countries can be accessed for October 2024."""
        cache = IQBCache(data_dir=real_data_dir)
        countries = ["US", "DE", "BR"]

        for country in countries:
            data = cache.get_iqb_data(
                granularity=IQBDatasetGranularity.COUNTRY,
                country_code=country,
                start_date="2024-10-01",
                end_date="2024-11-01",
            )
            assert data.mlab is not None

    def test_all_percentiles_available(self, real_data_dir):
        """Test that all expected percentiles are available in cached data."""
        cache = IQBCache(data_dir=real_data_dir)

        # Standard percentiles we generate
        percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]

        # Test on one sample file (US, October 2024)
        for p in percentiles:
            data = cache.get_iqb_data(
                granularity=IQBDatasetGranularity.COUNTRY,
                country_code="US",
                start_date="2024-10-01",
                end_date="2024-11-01",
                percentile=p,
            )
            assert data.mlab is not None

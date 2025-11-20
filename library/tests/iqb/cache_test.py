"""Tests for the IQBCache data fetching module."""

from datetime import datetime

import pytest

from iqb import IQBCache


class TestIQBCacheInitialization:
    """Tests for IQBCache class initialization."""

    def test_init_with_default_cache_dir(self):
        """Test that IQBCache uses .iqb/ directory by default."""
        cache = IQBCache()
        assert cache.cache_dir.name == ".iqb"
        # Should be in current working directory
        assert cache.cache_dir.parent.samefile(".")

    def test_init_with_custom_cache_dir(self):
        """Test that IQBCache can be instantiated with custom cache_dir."""
        cache = IQBCache(cache_dir="/custom/path")
        assert str(cache.cache_dir) == "/custom/path"


class TestIQBCacheGetData:
    """Tests for IQBCache get_data method."""

    def test_get_data_us_october_2024(self, data_dir):
        """Test fetching US data for October 2024."""
        cache = IQBCache(cache_dir=data_dir)
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

    def test_get_data_de_october_2024(self, data_dir):
        """Test fetching Germany data for October 2024."""
        cache = IQBCache(cache_dir=data_dir)
        data = cache.get_data(
            country="DE",
            start_date=datetime(2024, 10, 1),
        )

        # Unwrap the `m-lab` part
        assert "m-lab" in data
        data = data["m-lab"]

        assert "download_throughput_mbps" in data
        assert isinstance(data["download_throughput_mbps"], (int, float))

    def test_get_data_br_october_2024(self, data_dir):
        """Test fetching Brazil data for October 2024."""
        cache = IQBCache(cache_dir=data_dir)
        data = cache.get_data(
            country="BR",
            start_date=datetime(2024, 10, 1),
        )

        # Unwrap the `m-lab` part
        assert "m-lab" in data
        data = data["m-lab"]

        assert "download_throughput_mbps" in data
        assert isinstance(data["download_throughput_mbps"], (int, float))

    def test_get_data_case_insensitive_country(self, data_dir):
        """Test that country code is case-insensitive."""
        cache = IQBCache(cache_dir=data_dir)

        data_upper = cache.get_data(country="US", start_date=datetime(2024, 10, 1))
        data_lower = cache.get_data(country="us", start_date=datetime(2024, 10, 1))

        # Should return same data regardless of case
        assert data_upper == data_lower

    def test_get_data_with_different_percentile(self, data_dir):
        """Test extracting different percentile values."""
        cache = IQBCache(cache_dir=data_dir)

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

    def test_get_data_france_october_2024(self, data_dir):
        """Test fetching France data for October 2024."""
        cache = IQBCache(cache_dir=data_dir)
        data = cache.get_data(
            country="FR",
            start_date=datetime(2024, 10, 1),
        )

        # Unwrap the `m-lab` part
        assert "m-lab" in data
        data = data["m-lab"]

        assert "download_throughput_mbps" in data
        assert isinstance(data["download_throughput_mbps"], (int, float))

    def test_get_data_canada_october_2024(self, data_dir):
        """Test fetching Canada data for October 2024."""
        cache = IQBCache(cache_dir=data_dir)
        data = cache.get_data(
            country="CA",
            start_date=datetime(2024, 10, 1),
        )

        # Unwrap the `m-lab` part
        assert "m-lab" in data
        data = data["m-lab"]

        assert "download_throughput_mbps" in data
        assert isinstance(data["download_throughput_mbps"], (int, float))

    def test_get_data_australia_october_2025(self, data_dir):
        """Test fetching Australia data for October 2025."""
        cache = IQBCache(cache_dir=data_dir)
        data = cache.get_data(
            country="AU",
            start_date=datetime(2025, 10, 1),
        )

        # Unwrap the `m-lab` part
        assert "m-lab" in data
        data = data["m-lab"]

        assert "download_throughput_mbps" in data
        assert isinstance(data["download_throughput_mbps"], (int, float))

    def test_get_data_unavailable_country_raises_error(self, data_dir):
        """Test that requesting data for unavailable country raises FileNotFoundError."""
        cache = IQBCache(cache_dir=data_dir)

        # Use a fictional country code that won't exist
        with pytest.raises(FileNotFoundError, match="No cached data file found"):
            cache.get_data(country="ZZ", start_date=datetime(2024, 10, 1))

    def test_get_data_unavailable_date_raises_error(self, data_dir):
        """Test that requesting data for unavailable date raises FileNotFoundError."""
        cache = IQBCache(cache_dir=data_dir)

        with pytest.raises(FileNotFoundError, match="No cached data"):
            cache.get_data(country="US", start_date=datetime(2024, 11, 1))

    def test_get_data_with_explicit_end_date_raises_error(self, data_dir):
        """Test that specifying end_date raises error (not yet supported)."""
        cache = IQBCache(cache_dir=data_dir)

        with pytest.raises(FileNotFoundError, match="No cached data"):
            cache.get_data(
                country="US",
                start_date=datetime(2024, 10, 1),
                end_date=datetime(2024, 11, 1),
            )


class TestIQBCacheExtractPercentile:
    """Tests for IQBCache _extract_percentile method."""

    def test_extract_percentile_returns_correct_structure(self, data_dir):
        """Test that _extract_percentile returns the expected dict structure."""
        cache = IQBCache(cache_dir=data_dir)

        # Create sample data matching JSON structure
        sample_data = {
            "metrics": {
                "download_throughput_mbps": {"p95": 100.0, "p50": 50.0},
                "upload_throughput_mbps": {"p95": 80.0, "p50": 40.0},
                "latency_ms": {"p95": 100.0, "p50": 50.0},
                "packet_loss": {"p95": 0.01, "p50": 0.005},
            }
        }

        result = cache._extract_percentile(sample_data, 95)

        assert result == {
            "download_throughput_mbps": 100.0,
            "upload_throughput_mbps": 80.0,
            "latency_ms": 100.0,
            "packet_loss": 0.01,
        }

    def test_extract_percentile_invalid_raises_helpful_error(self, data_dir):
        """Test that requesting invalid percentile raises ValueError with available options."""
        cache = IQBCache(cache_dir=data_dir)

        # Create sample data with only p50 and p95
        sample_data = {
            "metrics": {
                "download_throughput_mbps": {"p50": 50.0, "p95": 100.0},
                "upload_throughput_mbps": {"p50": 40.0, "p95": 80.0},
                "latency_ms": {"p50": 50.0, "p95": 100.0},
                "packet_loss": {"p50": 0.005, "p95": 0.01},
            }
        }

        # Request p99 which doesn't exist
        with pytest.raises(ValueError) as exc_info:
            cache._extract_percentile(sample_data, 99)

        # Error message should mention p99 and show available options
        error_msg = str(exc_info.value)
        assert "99" in error_msg
        assert "50" in error_msg
        assert "95" in error_msg
        assert "Available percentiles" in error_msg

    def test_get_data_invalid_percentile_from_real_file(self, data_dir):
        """Test that requesting unavailable percentile from real data file raises ValueError."""
        cache = IQBCache(cache_dir=data_dir)

        # Request p37 which doesn't exist in the actual data files
        with pytest.raises(ValueError) as exc_info:
            cache.get_data(country="US", start_date=datetime(2024, 10, 1), percentile=37)

        # Error should list available percentiles (1, 5, 10, 25, 50, 75, 90, 95, 99)
        error_msg = str(exc_info.value)
        assert "37" in error_msg
        assert "Available percentiles" in error_msg


class TestIQBCacheAllFiles:
    """Test that all expected cache files are accessible."""

    def test_all_countries_october_2024(self, data_dir):
        """Test that all countries can be accessed for October 2024."""
        cache = IQBCache(cache_dir=data_dir)
        countries = ["US", "DE", "BR"]

        for country in countries:
            data = cache.get_data(
                country=country,
                start_date=datetime(2024, 10, 1),
            )

            # Verify structure
            assert "m-lab" in data
            mlab_data = data["m-lab"]

            assert "download_throughput_mbps" in mlab_data
            assert "upload_throughput_mbps" in mlab_data
            assert "latency_ms" in mlab_data
            assert "packet_loss" in mlab_data

            # Verify all values are numeric
            assert isinstance(mlab_data["download_throughput_mbps"], (int, float))
            assert isinstance(mlab_data["upload_throughput_mbps"], (int, float))
            assert isinstance(mlab_data["latency_ms"], (int, float))
            assert isinstance(mlab_data["packet_loss"], (int, float))

    def test_all_countries_october_2025(self, data_dir):
        """Test that all countries can be accessed for October 2025."""
        cache = IQBCache(cache_dir=data_dir)
        countries = ["US", "DE", "BR"]

        for country in countries:
            data = cache.get_data(
                country=country,
                start_date=datetime(2025, 10, 1),
            )

            # Verify structure
            assert "m-lab" in data
            mlab_data = data["m-lab"]

            assert "download_throughput_mbps" in mlab_data
            assert "upload_throughput_mbps" in mlab_data
            assert "latency_ms" in mlab_data
            assert "packet_loss" in mlab_data

            # Verify all values are numeric
            assert isinstance(mlab_data["download_throughput_mbps"], (int, float))
            assert isinstance(mlab_data["upload_throughput_mbps"], (int, float))
            assert isinstance(mlab_data["latency_ms"], (int, float))
            assert isinstance(mlab_data["packet_loss"], (int, float))

    def test_all_supported_combinations(self, data_dir):
        """Test all combinations of countries and periods that should be available."""
        cache = IQBCache(cache_dir=data_dir)

        # All combinations we expect to have cached
        combinations = [
            ("US", datetime(2024, 10, 1)),
            ("DE", datetime(2024, 10, 1)),
            ("BR", datetime(2024, 10, 1)),
            ("US", datetime(2025, 10, 1)),
            ("DE", datetime(2025, 10, 1)),
            ("BR", datetime(2025, 10, 1)),
        ]

        for country, start_date in combinations:
            # Should not raise any errors
            data = cache.get_data(country=country, start_date=start_date)

            # Basic sanity check
            assert "m-lab" in data
            assert "download_throughput_mbps" in data["m-lab"]

    def test_all_percentiles_available(self, data_dir):
        """Test that all expected percentiles are available in cached data."""
        cache = IQBCache(cache_dir=data_dir)

        # Standard percentiles we generate
        percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]

        # Test on one sample file (US, October 2024)
        for p in percentiles:
            data = cache.get_data(
                country="US",
                start_date=datetime(2024, 10, 1),
                percentile=p,
            )

            # Should not raise any errors
            assert "m-lab" in data
            assert "download_throughput_mbps" in data["m-lab"]

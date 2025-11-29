"""Integration tests for the IQB library."""

from datetime import datetime

from iqb import IQBCache, IQBCalculator, IQBDatasetGranularity


class TestIntegration:
    """Integration test for the IQB library."""

    def test_with_us_data_october_2024(self, data_dir):
        """Test that IQBCache uses .iqb/ directory by default."""
        # Instantiate the cache with the global cache dir
        cache = IQBCache(data_dir=data_dir)

        # Read the data from the cache
        data = cache.get_data("US", start_date=datetime.strptime("2024-10-01", "%Y-%m-%d"))

        # Create the calculator
        calculator = IQBCalculator()

        # Compute the IQB score
        score = calculator.calculate_iqb_score(data)

        # Ensure the score is reasonable (0.0 to 1.0 inclusive)
        # NOTE: With current percentile interpretation (p95 for all metrics),
        # scores may reach 1.0 as we're checking "top ~5% performance"
        assert score >= 0 and score <= 1

    def test_cache_entry_read_dataframes(self, data_dir):
        """Test reading parquet files from v1 cache using get_cache_entry()."""
        # Instantiate the cache with the data directory
        cache = IQBCache(data_dir=data_dir)

        # Get cache entry for October 2024 country-level data
        entry = cache.get_mlab_cache_entry(
            start_date="2024-10-01",
            end_date="2024-11-01",
            granularity=IQBDatasetGranularity.COUNTRY,
        )

        # Read download DataFrame for all countries
        download_df = entry.read_download_data_frame()
        assert not download_df.empty
        assert "country_code" in download_df.columns
        assert "sample_count" in download_df.columns
        assert "download_p95" in download_df.columns
        assert "latency_p95" in download_df.columns
        assert "loss_p95" in download_df.columns
        assert len(download_df) > 200  # We have 236 countries

        # Read upload DataFrame for all countries
        upload_df = entry.read_upload_data_frame()
        assert not upload_df.empty
        assert "country_code" in upload_df.columns
        assert "sample_count" in upload_df.columns
        assert "upload_p95" in upload_df.columns
        assert len(upload_df) > 200  # We have 237 countries

        # Filter by country_code (US)
        us_download_df = entry.read_download_data_frame(country_code="US")
        assert len(us_download_df) == 1
        assert us_download_df.iloc[0]["country_code"] == "US"
        assert us_download_df.iloc[0]["sample_count"] > 0
        assert us_download_df.iloc[0]["download_p95"] > 0  # US has good throughput

        us_upload_df = entry.read_upload_data_frame(country_code="US")
        assert len(us_upload_df) == 1
        assert us_upload_df.iloc[0]["country_code"] == "US"
        assert us_upload_df.iloc[0]["sample_count"] > 0
        assert us_upload_df.iloc[0]["upload_p95"] > 0

        # Test column projection (only load specific columns)
        limited_download_df = entry.read_download_data_frame(
            country_code="US",
            columns=["country_code", "sample_count", "download_p95", "latency_p95"],
        )
        assert len(limited_download_df.columns) == 4
        assert "sample_count" in limited_download_df.columns
        assert "download_p95" in limited_download_df.columns
        assert "latency_p95" in limited_download_df.columns
        assert "loss_p95" not in limited_download_df.columns  # Not requested

        # Verify we can read data for multiple countries
        countries = ["US", "DE", "BR"]
        for country_code in countries:
            df = entry.read_download_data_frame(country_code=country_code)
            assert len(df) == 1
            assert df.iloc[0]["country_code"] == country_code
            assert df.iloc[0]["sample_count"] > 0
            assert df.iloc[0]["download_p95"] > 0

    def test_data_frame_pair_to_dict(self, data_dir):
        """Test the high-level API: get_data_frame_pair() and to_dict()."""
        # Instantiate the cache with the data directory
        cache = IQBCache(data_dir=data_dir)

        # Get cache entry for October 2024 country-level data
        entry = cache.get_mlab_cache_entry(
            start_date="2024-10-01",
            end_date="2024-11-01",
            granularity=IQBDatasetGranularity.COUNTRY,
        )

        # Use the high-level API to get a DataFramePair (no percentile binding)
        pair = entry.read_data_frame_pair(country_code="US")

        # Verify the pair has the expected structure
        assert pair is not None
        assert len(pair.download) == 1
        assert len(pair.upload) == 1

        # Verify all percentile columns are present (for inspection)
        assert "download_p95" in pair.download.columns
        assert "download_p50" in pair.download.columns
        assert "upload_p95" in pair.upload.columns
        assert "upload_p50" in pair.upload.columns

        # Convert to dict format for IQBCalculator (percentile specified here)
        data_p95 = pair.to_dict(percentile=95)

        # Verify the dict has the expected structure
        assert "download_throughput_mbps" in data_p95
        assert "upload_throughput_mbps" in data_p95
        assert "latency_ms" in data_p95
        assert "packet_loss" in data_p95

        # Verify all values are floats and reasonable
        assert isinstance(data_p95["download_throughput_mbps"], float)
        assert isinstance(data_p95["upload_throughput_mbps"], float)
        assert isinstance(data_p95["latency_ms"], float)
        assert isinstance(data_p95["packet_loss"], float)

        # US should have decent throughput (> 0)
        assert data_p95["download_throughput_mbps"] > 0
        assert data_p95["upload_throughput_mbps"] > 0

        # Latency and packet loss should be non-negative
        assert data_p95["latency_ms"] >= 0
        assert data_p95["packet_loss"] >= 0

        # Test extracting different percentile from SAME pair (no re-read!)
        data_p50 = pair.to_dict(percentile=50)

        # Median values should generally be different from p95
        # (though not guaranteed, it's extremely unlikely they're identical)
        assert (
            data_p95["download_throughput_mbps"] != data_p50["download_throughput_mbps"]
            or data_p95["upload_throughput_mbps"] != data_p50["upload_throughput_mbps"]
            or data_p95["latency_ms"] != data_p50["latency_ms"]
            or data_p95["packet_loss"] != data_p50["packet_loss"]
        )

        # Test default percentile (should be 95)
        data_default = pair.to_dict()
        assert data_default == data_p95

        # Test granularity validation: should reject city filter with country granularity
        error_raised = False
        try:
            entry.read_data_frame_pair(country_code="US", city="Boston")
        except ValueError as e:
            error_raised = True
            assert "city" in str(e).lower()
            assert "granularity" in str(e).lower()

        assert error_raised, "Expected ValueError for city filter with country granularity"

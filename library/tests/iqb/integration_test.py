"""Integration tests for the IQB library."""

from datetime import datetime

from iqb import IQBCache, IQBCalculator


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
        entry = cache.get_cache_entry(
            start_date="2024-10-01",
            end_date="2024-11-01",
            granularity="country",
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

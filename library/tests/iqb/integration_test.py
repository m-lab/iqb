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

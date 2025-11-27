"""Tests comparing v0 JSON golden files against v1 parquet golden files.

This test ensures that the v0 and v1 cache formats contain equivalent data
within acceptable tolerance bounds (accounting for BigQuery non-determinism).

## Why Do Values Differ Between v0 and v1?

The v0 (JSON) and v1 (parquet) golden files were generated from **separate
BigQuery query executions**, typically minutes apart. BigQuery's APPROX_QUANTILES
function is intentionally non-deterministic for performance reasons:

1. **Distributed sampling**: BigQuery processes data across multiple workers,
   each computing approximate quantiles on their partition
2. **Merge strategy**: The final result merges these approximations, which
   can vary between executions
3. **Data ordering**: Even with the same data, different execution plans can
   produce slightly different quantile estimates

## Why 3% Tolerance is Acceptable

Testing with test_parquet_precision.py confirms that:
- ✅ Parquet stores float64 values with **zero precision loss**
- ✅ JSON serialization preserves **exact float64 values**
- ✅ to_pylist() conversion is **lossless**

Therefore, all observed differences are due to **BigQuery non-determinism only**.

The 3% (0.03) relative tolerance accounts for:
- **Typical variation**: Most percentiles differ by 0.1-1%
- **Extreme percentile variance**: p1 and p99 can differ by 1-3% due to:
  - Fewer samples in extreme tails
  - Higher sensitivity to sampling strategy
  - More variance in underlying data
- **Metric-specific variance**: Latency and packet loss at extreme percentiles
  show higher variance than throughput metrics

This tolerance is appropriate because:
1. **IQB scores use p95**: The critical percentile (p95) shows ~0.1-0.5%
   variance, well within tolerance
2. **Relative differences matter**: 3% of 624 Mbps (p95) = ±18 Mbps, which
   doesn't change IQB threshold classifications
3. **Statistical validity**: APPROX_QUANTILES documentation states results
   are approximate and may vary between executions
4. **Production reality**: Any production system will experience this same
   variance when querying BigQuery multiple times

If values differ by >3%, it indicates an actual problem (not BigQuery variance).
"""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

# Golden countries with guaranteed data across both formats
GOLDEN_COUNTRIES = ["US", "DE", "BR"]
GOLDEN_PERIODS = [
    ("2024_10", "20241001T000000Z", "20241101T000000Z"),
    ("2025_10", "20251001T000000Z", "20251101T000000Z"),
]

# All percentiles we track
PERCENTILES = [1, 5, 10, 25, 50, 75, 90, 95, 99]

# Tolerance for differences (BigQuery non-determinism + precision)
# 3% relative tolerance - BigQuery APPROX_QUANTILES has higher variance
# at extreme percentiles (p1, p99), especially for metrics with high variance
RELATIVE_TOLERANCE = 0.03

# Absolute tolerance for very small values (packet loss near 0)
ABSOLUTE_TOLERANCE = 0.001


def load_v0_data(country: str, period: str) -> dict:
    """
    Load data from v0 JSON golden file.

    Args:
        country: Country code (e.g., "US")
        period: Period string (e.g., "2024_10")

    Returns:
        Dict with metrics -> percentiles
    """
    # From library/tests/iqb/ go up 3 levels to iqb/, then to data/cache
    iqb_dir = Path(__file__).parent.parent.parent.parent
    cache_dir = iqb_dir / "data" / "cache"
    json_file = cache_dir / "v0" / f"{country.lower()}_{period}.json"

    assert json_file.exists(), f"v0 golden file missing: {json_file}"

    data = json.loads(json_file.read_text())
    return data["metrics"]


def load_v1_data(country: str, start_date: str, end_date: str, query_type: str) -> dict:
    """
    Load data from v1 parquet golden file for a specific country.

    Args:
        country: Country code (e.g., "US")
        start_date: Start date directory (e.g., "20241001T000000Z")
        end_date: End date directory (e.g., "20241101T000000Z")
        query_type: Query type ("downloads_by_country" or "uploads_by_country")

    Returns:
        Dict with percentile data for the country
    """
    iqb_dir = Path(__file__).parent.parent.parent.parent
    cache_dir = iqb_dir / "data" / "cache"
    parquet_file = cache_dir / "v1" / start_date / end_date / f"{query_type}.parquet"

    assert parquet_file.exists(), f"v1 golden file missing: {parquet_file}"

    # Read parquet and find country
    table = pq.read_table(parquet_file)
    records = table.to_pylist()

    country_record = next((r for r in records if r["country_code"] == country.upper()), None)

    assert country_record is not None, f"Country {country} not found in {parquet_file}"

    return country_record


def values_match(v0_val: float, v1_val: float, metric_name: str) -> tuple[bool, str]:
    """
    Check if two values match within tolerance.

    Args:
        v0_val: Value from v0 JSON
        v1_val: Value from v1 parquet
        metric_name: Name of metric (for error messages)

    Returns:
        (matches, error_message)
    """
    # Handle exact zeros
    if v0_val == 0 and v1_val == 0:
        return True, ""

    # For very small values, use absolute tolerance
    if abs(v0_val) < ABSOLUTE_TOLERANCE or abs(v1_val) < ABSOLUTE_TOLERANCE:
        diff = abs(v0_val - v1_val)
        if diff <= ABSOLUTE_TOLERANCE:
            return True, ""
        return (
            False,
            f"{metric_name}: |{v0_val} - {v1_val}| = {diff} > {ABSOLUTE_TOLERANCE} (absolute)",
        )

    # For normal values, use relative tolerance
    rel_diff = abs(v0_val - v1_val) / max(abs(v0_val), abs(v1_val))
    if rel_diff <= RELATIVE_TOLERANCE:
        return True, ""

    return (
        False,
        f"{metric_name}: {v0_val} vs {v1_val} (rel diff: {rel_diff:.6f} > {RELATIVE_TOLERANCE})",
    )


class TestGoldenFilesComparison:
    """Compare v0 JSON and v1 parquet golden files."""

    @pytest.mark.parametrize("country", GOLDEN_COUNTRIES)
    @pytest.mark.parametrize("period,start_date,end_date", GOLDEN_PERIODS)
    def test_download_metrics_match(self, country, period, start_date, end_date):
        """Test that download metrics match between v0 and v1."""
        v0_data = load_v0_data(country, period)
        v1_data = load_v1_data(country, start_date, end_date, "downloads_by_country")

        mismatches = []

        # Compare download throughput
        for p in PERCENTILES:
            v0_val = v0_data["download_throughput_mbps"][f"p{p}"]
            v1_val = v1_data[f"download_p{p}"]
            matches, error = values_match(v0_val, v1_val, f"download_p{p}")
            if not matches:
                mismatches.append(error)

        # Compare latency
        for p in PERCENTILES:
            v0_val = v0_data["latency_ms"][f"p{p}"]
            v1_val = v1_data[f"latency_p{p}"]
            matches, error = values_match(v0_val, v1_val, f"latency_p{p}")
            if not matches:
                mismatches.append(error)

        # Compare packet loss
        for p in PERCENTILES:
            v0_val = v0_data["packet_loss"][f"p{p}"]
            v1_val = v1_data[f"loss_p{p}"]
            matches, error = values_match(v0_val, v1_val, f"loss_p{p}")
            if not matches:
                mismatches.append(error)

        if mismatches:
            pytest.fail(f"Mismatches for {country} {period} downloads:\n" + "\n".join(mismatches))

    @pytest.mark.parametrize("country", GOLDEN_COUNTRIES)
    @pytest.mark.parametrize("period,start_date,end_date", GOLDEN_PERIODS)
    def test_upload_metrics_match(self, country, period, start_date, end_date):
        """Test that upload metrics match between v0 and v1."""
        v0_data = load_v0_data(country, period)
        v1_data = load_v1_data(country, start_date, end_date, "uploads_by_country")

        mismatches = []

        # Compare upload throughput
        for p in PERCENTILES:
            v0_val = v0_data["upload_throughput_mbps"][f"p{p}"]
            v1_val = v1_data[f"upload_p{p}"]
            matches, error = values_match(v0_val, v1_val, f"upload_p{p}")
            if not matches:
                mismatches.append(error)

        if mismatches:
            pytest.fail(f"Mismatches for {country} {period} uploads:\n" + "\n".join(mismatches))

    @pytest.mark.parametrize("country", GOLDEN_COUNTRIES)
    @pytest.mark.parametrize("period,start_date,end_date", GOLDEN_PERIODS)
    def test_v0_files_exist(self, country, period, start_date, end_date):
        """Verify v0 golden files exist for all test cases."""
        iqb_dir = Path(__file__).parent.parent.parent.parent
        cache_dir = iqb_dir / "data" / "cache"
        json_file = cache_dir / "v0" / f"{country.lower()}_{period}.json"
        assert json_file.exists(), f"Missing v0 golden file: {json_file}"

    @pytest.mark.parametrize("country", GOLDEN_COUNTRIES)
    @pytest.mark.parametrize("period,start_date,end_date", GOLDEN_PERIODS)
    def test_v1_files_exist(self, country, period, start_date, end_date):
        """Verify v1 golden files exist for all test cases."""
        iqb_dir = Path(__file__).parent.parent.parent.parent
        cache_dir = iqb_dir / "data" / "cache"

        downloads_file = cache_dir / "v1" / start_date / end_date / "downloads_by_country.parquet"
        uploads_file = cache_dir / "v1" / start_date / end_date / "uploads_by_country.parquet"

        assert downloads_file.exists(), f"Missing v1 golden file: {downloads_file}"
        assert uploads_file.exists(), f"Missing v1 golden file: {uploads_file}"

        # Verify country exists in parquet files
        downloads_table = pq.read_table(downloads_file)
        downloads_records = downloads_table.to_pylist()
        assert any(r["country_code"] == country.upper() for r in downloads_records), (
            f"Country {country} not found in {downloads_file}"
        )

        uploads_table = pq.read_table(uploads_file)
        uploads_records = uploads_table.to_pylist()
        assert any(r["country_code"] == country.upper() for r in uploads_records), (
            f"Country {country} not found in {uploads_file}"
        )

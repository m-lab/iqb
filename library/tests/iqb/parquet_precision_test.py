"""Tests to verify parquet storage preserves float64 precision.

This test verifies that the parquet format itself is not introducing precision
loss when storing and retrieving floating point values.
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


class TestParquetPrecision:
    """Verify parquet preserves float64 precision."""

    def test_float64_exact_roundtrip(self, tmp_path):
        """Test that float64 values survive parquet roundtrip exactly."""
        # Use actual values from US October 2024 data
        test_values = [
            624.9716396384902,  # download_p95 from v0
            370.12750203424736,  # upload_p95 from v0
            0.806,  # latency_p95 from v0
            0.0,  # loss_p95 from v0
            0.371868756143722,  # download_p1
            0.00034311166153415786,  # loss_p50 (very small)
        ]

        # Create table with float64 values
        table = pa.table(
            {
                "country_code": ["TEST"] * len(test_values),
                "metric_value": pa.array(test_values, type=pa.float64()),
            }
        )

        # Write to parquet
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, parquet_file)

        # Read back
        table2 = pq.read_table(parquet_file)
        records = table2.to_pylist()

        # Verify exact equality (no precision loss)
        for i, record in enumerate(records):
            assert (
                record["metric_value"] == test_values[i]
            ), f"Precision lost at index {i}: {test_values[i]} != {record['metric_value']}"

    def test_parquet_schema_is_float64(self, tmp_path):
        """Verify parquet stores values as float64, not float32."""
        test_value = 624.9716396384902

        table = pa.table({"value": pa.array([test_value], type=pa.float64())})

        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, parquet_file)

        # Check schema
        table2 = pq.read_table(parquet_file)
        assert table2.schema.field("value").type == pa.float64(), (
            "Parquet stored as float32 instead of float64"
        )

    def test_json_roundtrip_precision(self):
        """Test that JSON serialization preserves precision."""
        import json

        test_value = 624.9716396384902

        # Serialize to JSON
        json_str = json.dumps({"value": test_value})

        # Deserialize
        parsed = json.loads(json_str)

        assert parsed["value"] == test_value, (
            f"JSON roundtrip lost precision: {test_value} != {parsed['value']}"
        )

    def test_to_pylist_preserves_precision(self, tmp_path):
        """Test that to_pylist() preserves float64 precision."""
        test_values = [624.9716396384902, 0.00034311166153415786]

        table = pa.table({"values": pa.array(test_values, type=pa.float64())})

        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, parquet_file)

        # Read and convert to Python list
        table2 = pq.read_table(parquet_file)
        py_list = table2.to_pylist()

        for i, record in enumerate(py_list):
            assert record["values"] == test_values[i], (
                f"to_pylist() lost precision: {test_values[i]} != {record['values']}"
            )

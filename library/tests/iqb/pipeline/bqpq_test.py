"""Tests for the iqb.pipeline.bqpq module."""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa

from iqb.pipeline.bqpq import (
    PipelineBQPQClient,
    PipelineBQPQQueryResult,
)


class FakePathsProvider:
    """Implement PipelineBQPQPathsProvider for testing."""

    def __init__(self, basedir) -> None:
        self.basedir = Path(basedir)

    def data_parquet_file_path(self) -> Path:
        return self.basedir / "data.parquet"

    def stats_json_file_path(self) -> Path:
        return self.basedir / "stats.json"


class TestPipelineBQPQClient:
    """Test for PipelineBQPQClient class."""

    @patch("iqb.pipeline.bqpq.bigquery.Client")
    @patch("iqb.pipeline.bqpq.bigquery_storage_v1.BigQueryReadClient")
    def test_init_calls_bigquery_initializers(self, mock_storage, mock_client):
        """Test that BigQuery clients are lazy-loaded on first access."""
        client = PipelineBQPQClient(project="test-project")

        # Clients should NOT be initialized during __init__ (lazy loading)
        mock_client.assert_not_called()
        mock_storage.assert_not_called()

        # Accessing client property should trigger initialization
        _ = client.client
        mock_client.assert_called_once_with(project="test-project")

        # Accessing bq_read_clnt property should trigger initialization
        _ = client.bq_read_clnt
        mock_storage.assert_called_once()

        # Subsequent access should not re-initialize (cached)
        _ = client.client
        _ = client.bq_read_clnt
        mock_client.assert_called_once()  # Still only called once
        mock_storage.assert_called_once()  # Still only called once

    @patch("iqb.pipeline.bqpq.bigquery.Client")
    @patch("iqb.pipeline.bqpq.bigquery_storage_v1.BigQueryReadClient")
    def test_execute_query_waits_for_job(self, mock_storage, mock_client, tmp_path):
        """Test that execute_query polls job.state and calls job.reload."""
        # ARRANGE: Set up a mock job that will report "RUNNING" twice
        # before reporting "DONE".
        mock_client_instance = Mock()
        mock_job = Mock()
        mock_client_instance.query.return_value = mock_job
        mock_job.total_bytes_processed = 123456789
        mock_job.state = "RUNNING"
        mock_rows = Mock()
        mock_job.result.return_value = mock_rows

        # This side effect is called by mock_job.reload().
        # It changes the state to "DONE" on the second call.
        def reload_side_effect():
            if mock_job.reload.call_count == 2:
                mock_job.state = "DONE"

        mock_job.reload.side_effect = reload_side_effect
        mock_client.return_value = mock_client_instance
        mock_storage_client = Mock()
        mock_storage.return_value = mock_storage_client

        # Create the client
        client = PipelineBQPQClient(project="test-project")
        paths_provider = FakePathsProvider(tmp_path)

        # ACT: Execute the query. The _sleep_secs avoids long waits.
        result = client.execute_query(
            paths_provider=paths_provider,
            template_hash="hash123",
            query="SELECT * FROM TABLE COUNT 1;",
            _sleep_secs=0.001,
        )

        # ASSERT: Verify the polling logic worked as expected.
        mock_client_instance.query.assert_called_once()

        # The loop should run until state is "DONE", which we configured
        # to happen after the second reload call.
        assert mock_job.reload.call_count == 2
        mock_job.result.assert_called_once()

        # Verify the result structure
        assert result.bq_read_client == mock_storage_client
        assert result.job == mock_job
        assert result.rows == mock_rows
        assert result.paths_provider == paths_provider
        assert result.template_hash == "hash123"


class TestPipelineBQPQQueryResultSaveDataParquet:
    """Test for PipelineBQPQQueryResult.save_data_parquet method."""

    def test_save_data_parquet_with_data(self, tmp_path):
        """Test saving parquet file with mock data."""
        cache_dir = tmp_path / "cache"

        # Mock Arrow batches
        mock_batch = MagicMock()
        mock_batch.schema = MagicMock()
        mock_batch.num_rows = 1

        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([mock_batch])
        mock_rows.total_rows = 1

        # Mock ParquetWriter
        with patch("iqb.pipeline.bqpq.pq.ParquetWriter") as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            # Create a side effect to actually create the temp parquet file
            def create_parquet_file(*args, **kwargs):
                _, _ = args, kwargs
                # Get the path that was passed to ParquetWriter
                temp_path = Path(mock_writer.call_args[0][0])
                temp_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path.write_text("fake data")

            mock_writer_instance.write_batch.side_effect = create_parquet_file

            # Create result and save
            result = PipelineBQPQQueryResult(
                bq_read_client=Mock(),
                job=Mock(),
                rows=mock_rows,
                paths_provider=FakePathsProvider(cache_dir),
                template_hash="abc123",
            )

            file_path = result.save_data_parquet()

            # Verify file path and directory creation
            expected_path = cache_dir / "data.parquet"
            assert file_path == expected_path
            assert cache_dir.exists()
            assert file_path.exists()

            mock_writer_instance.write_batch.assert_called_once_with(mock_batch)

    def test_save_data_parquet_with_empty_results(self, tmp_path):
        """Test handling of empty query results - writes empty parquet file."""
        cache_dir = tmp_path / "cache"

        # Mock empty iterator
        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([])
        mock_rows.total_rows = 0

        # Mock ParquetWriter to verify it's called with empty schema
        with patch("iqb.pipeline.bqpq.pq.ParquetWriter") as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            # Create a side effect to actually create the temp parquet file
            def create_parquet_file(*args, **kwargs):
                _, _ = args, kwargs
                # Get the path that was passed to ParquetWriter
                temp_path = Path(mock_writer.call_args[0][0])
                temp_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path.write_text("")

            mock_writer.return_value.__exit__.side_effect = create_parquet_file

            result = PipelineBQPQQueryResult(
                bq_read_client=Mock(),
                job=Mock(),
                rows=mock_rows,
                paths_provider=FakePathsProvider(cache_dir),
                template_hash="abc123",
            )

            file_path = result.save_data_parquet()

            # Verify empty parquet file would be created
            expected_path = cache_dir / "data.parquet"
            assert file_path == expected_path
            assert cache_dir.exists()
            assert file_path.exists()

            # Verify ParquetWriter was called with temp file path
            mock_writer.assert_called_once()
            call_args = mock_writer.call_args
            # The first arg should be a temp path, not the final path
            temp_file_arg = Path(call_args[0][0])
            assert temp_file_arg.name == "data.parquet"

            # Verify schema is empty (no fields)
            schema_arg = call_args[0][1]
            assert isinstance(schema_arg, pa.Schema)
            assert len(schema_arg) == 0

            # Verify no batches were written (first_batch is None, for loop has nothing)
            mock_writer_instance.write_batch.assert_not_called()

    def test_save_data_parquet_with_multiple_batches(self, tmp_path):
        """Test saving multiple Arrow batches."""
        cache_dir = tmp_path / "cache"

        # Mock multiple batches
        mock_batch1 = MagicMock()
        mock_batch1.schema = MagicMock()
        mock_batch1.num_rows = 1
        mock_batch2 = MagicMock()
        mock_batch2.num_rows = 1
        mock_batch3 = MagicMock()
        mock_batch3.num_rows = 1

        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([mock_batch1, mock_batch2, mock_batch3])
        mock_rows.total_rows = 3

        with patch("iqb.pipeline.bqpq.pq.ParquetWriter") as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            # Create a side effect to actually create the temp parquet file
            def create_parquet_file(*args, **kwargs):
                _, _ = args, kwargs
                # Get the path that was passed to ParquetWriter
                temp_path = Path(mock_writer.call_args[0][0])
                temp_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path.write_text("fake data")

            mock_writer_instance.write_batch.side_effect = create_parquet_file

            result = PipelineBQPQQueryResult(
                bq_read_client=Mock(),
                job=Mock(),
                rows=mock_rows,
                paths_provider=FakePathsProvider(cache_dir),
                template_hash="abc123",
            )

            file_path = result.save_data_parquet()

            # Verify all batches written
            assert mock_writer_instance.write_batch.call_count == 3
            expected_path = cache_dir / "data.parquet"
            assert file_path == expected_path
            assert file_path.exists()

    def test_save_data_parquet_creates_nested_directories(self, tmp_path):
        """Test that save_parquet creates nested cache directory."""
        # Deep nested path
        cache_dir = tmp_path / "a" / "b" / "c"

        mock_batch = MagicMock()
        mock_batch.schema = MagicMock()
        mock_batch.num_rows = 1

        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([mock_batch])
        mock_rows.total_rows = 1

        with patch("iqb.pipeline.bqpq.pq.ParquetWriter") as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            # Create a side effect to actually create the temp parquet file
            def create_parquet_file(*args, **kwargs):
                _, _ = args, kwargs
                # Get the path that was passed to ParquetWriter
                temp_path = Path(mock_writer.call_args[0][0])
                temp_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path.write_text("fake data")

            mock_writer_instance.write_batch.side_effect = create_parquet_file

            result = PipelineBQPQQueryResult(
                bq_read_client=Mock(),
                job=Mock(),
                rows=mock_rows,
                paths_provider=FakePathsProvider(cache_dir),
                template_hash="abc123",
            )

            file_path = result.save_data_parquet()

            # Verify cache directory created
            assert cache_dir.exists()
            expected_path = cache_dir / "data.parquet"
            assert file_path == expected_path
            assert file_path.exists()


class TestPipelineBQPQQueryResultSaveStatsJSON:
    """Test for PipelineBQPQQueryResult.save_stats_json method."""

    def test_save_stats_json_with_complete_job_info(self, tmp_path):
        """Test saving stats.json with complete BigQuery job information."""
        cache_dir = tmp_path / "cache"

        # Mock BigQuery job with complete info
        mock_job = Mock()
        mock_job.started = datetime(2024, 11, 27, 10, 0, 0, tzinfo=UTC)
        mock_job.ended = datetime(2024, 11, 27, 10, 5, 30, tzinfo=UTC)
        mock_job.total_bytes_processed = 1000000000  # 1 GB
        mock_job.total_bytes_billed = 1073741824  # 1 GiB

        result = PipelineBQPQQueryResult(
            bq_read_client=Mock(),
            job=mock_job,
            rows=Mock(),
            paths_provider=FakePathsProvider(cache_dir),
            template_hash="abc123def456",
        )

        stats_path = result.save_stats_json()

        # Verify file created
        assert stats_path == cache_dir / "stats.json"
        assert stats_path.exists()

        # Verify content
        with stats_path.open() as filep:
            stats = json.load(filep)

        assert stats["query_start_time"] == "2024-11-27T10:00:00.000000Z"
        assert stats["query_duration_seconds"] == 330.0  # 5 min 30 sec
        assert stats["template_hash"] == "abc123def456"
        assert stats["total_bytes_processed"] == 1000000000
        assert stats["total_bytes_billed"] == 1073741824

    def test_save_stats_json_with_incomplete_job_info(self, tmp_path):
        """Test saving stats when job timing info is incomplete."""
        # Mock job without timing info
        mock_job = Mock()
        mock_job.started = None
        mock_job.ended = None
        mock_job.total_bytes_processed = 500000
        mock_job.total_bytes_billed = 524288

        result = PipelineBQPQQueryResult(
            bq_read_client=Mock(),
            job=mock_job,
            rows=Mock(),
            paths_provider=FakePathsProvider(tmp_path),
            template_hash="xyz789",
        )

        stats_path = result.save_stats_json()

        # Verify content
        with stats_path.open() as filep:
            stats = json.load(filep)

        assert stats["query_start_time"] is None
        assert stats["query_duration_seconds"] is None
        assert stats["template_hash"] == "xyz789"
        assert stats["total_bytes_processed"] == 500000
        assert stats["total_bytes_billed"] == 524288

    def test_save_stats_json_creates_directory(self, tmp_path):
        """Test that save_stats creates cache directory if needed."""
        cache_dir = tmp_path / "a" / "b" / "c"

        mock_job = Mock()
        mock_job.started = None
        mock_job.ended = None
        mock_job.total_bytes_processed = 0
        mock_job.total_bytes_billed = 0

        result = PipelineBQPQQueryResult(
            bq_read_client=Mock(),
            job=mock_job,
            rows=Mock(),
            paths_provider=FakePathsProvider(cache_dir),
            template_hash="test123",
        )

        stats_path = result.save_stats_json()

        assert cache_dir.exists()
        assert stats_path.exists()

"""Tests for the iqb.pipeline module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from iqb.pipeline import (
    IQBPipeline,
    ParquetFileInfo,
    QueryResult,
    _load_query_template,
    _parse_date,
)


class TestHelperFunctions:
    """Test pure functions without external dependencies."""

    def test_parse_date_valid(self):
        """Test parsing valid date string."""
        result = _parse_date("2024-10-01")
        assert result == datetime(2024, 10, 1)

    def test_parse_date_invalid_format(self):
        """Test error on invalid date format."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _parse_date("2024/10/01")

    def test_parse_date_invalid_date(self):
        """Test error on invalid date values."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _parse_date("2024-13-01")

    def test_load_query_template_substitution(self):
        """Test that query template placeholders are substituted."""
        query, template_hash = _load_query_template("downloads_by_country", "2024-10-01", "2024-11-01")

        # Verify placeholders replaced
        assert "{START_DATE}" not in query
        assert "{END_DATE}" not in query
        assert "2024-10-01" in query
        assert "2024-11-01" in query

        # Verify hash is present and is a SHA256 hex string (64 chars)
        assert template_hash is not None
        assert len(template_hash) == 64
        assert all(c in "0123456789abcdef" for c in template_hash)


class TestIQBPipelineInit:
    """Test IQBPipeline initialization."""

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_init_default_data_dir(self, mock_storage, mock_client):
        """Test initialization with default data directory."""
        pipeline = IQBPipeline(project_id="test-project")

        mock_client.assert_called_once_with(project="test-project")
        mock_storage.assert_called_once()
        assert pipeline.data_dir == Path.cwd() / ".iqb"

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_init_custom_data_dir(self, mock_storage, mock_client, tmp_path):
        """Test initialization with custom data directory."""
        custom_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project_id="test-project", data_dir=custom_dir)

        assert pipeline.data_dir == custom_dir


class TestIQBPipelineExecuteQuery:
    """Test query execution without hitting BigQuery."""

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_execute_query_template_path_construction(self, mock_storage, mock_client, tmp_path):
        """Test that execute_query_template constructs correct cache directory."""
        # Setup mocks
        mock_job = Mock()
        mock_rows = Mock()
        mock_client_instance = Mock()
        mock_client_instance.query.return_value = mock_job
        mock_job.result.return_value = mock_rows
        mock_client.return_value = mock_client_instance

        # Create pipeline
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project_id="test-project", data_dir=data_dir)

        # Execute query
        result = pipeline.execute_query_template(
            template="downloads_by_country",
            start_date="2024-10-01",
            end_date="2024-11-01",
        )

        # Verify query was called
        mock_client_instance.query.assert_called_once()
        query_arg = mock_client_instance.query.call_args[0][0]
        assert "2024-10-01" in query_arg
        assert "2024-11-01" in query_arg

        # Verify cache directory construction
        expected_dir = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )
        assert result.cache_dir == expected_dir
        assert result.rows == mock_rows
        assert result.job == mock_job

        # Verify metadata fields
        assert result.query_start_time is not None
        assert result.template_hash is not None
        assert len(result.template_hash) == 64

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_execute_query_invalid_dates(self, mock_storage, mock_client):
        """Test error when start_date > end_date."""
        pipeline = IQBPipeline(project_id="test-project")

        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            pipeline.execute_query_template(
                template="downloads_by_country",
                start_date="2024-11-01",
                end_date="2024-10-01",
            )

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_execute_query_invalid_template(self, mock_storage, mock_client):
        """Test error on unknown template name."""
        pipeline = IQBPipeline(project_id="test-project")

        with pytest.raises(ValueError, match="Unknown template"):
            pipeline.execute_query_template(
                template="invalid_template",
                start_date="2024-10-01",
                end_date="2024-11-01",
            )

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_execute_query_uploads_template(self, mock_storage, mock_client, tmp_path):
        """Test executing uploads query template."""
        # Setup mocks
        mock_job = Mock()
        mock_rows = Mock()
        mock_client_instance = Mock()
        mock_client_instance.query.return_value = mock_job
        mock_job.result.return_value = mock_rows
        mock_client.return_value = mock_client_instance

        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project_id="test-project", data_dir=data_dir)

        result = pipeline.execute_query_template(
            template="uploads_by_country",
            start_date="2024-10-01",
            end_date="2024-11-01",
        )

        # Verify correct cache directory
        expected_dir = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "uploads_by_country"
        )
        assert result.cache_dir == expected_dir


class TestQueryResultSaveParquet:
    """Test parquet file saving with mocked Arrow data."""

    def test_save_parquet_with_data(self, tmp_path):
        """Test saving parquet file with mock data."""
        cache_dir = tmp_path / "cache"

        # Mock Arrow batches
        mock_batch = MagicMock()
        mock_batch.schema = MagicMock()

        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([mock_batch])

        # Mock ParquetWriter
        with patch("iqb.pipeline.pq.ParquetWriter") as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            # Create QueryResult and save
            result = QueryResult(
                bq_read_client=Mock(),
                job=Mock(),
                rows=mock_rows,
                cache_dir=cache_dir,
                query_start_time="2024-11-27T10:00:00.000000Z",
                template_hash="abc123",
            )

            info = result.save_parquet()

            # Verify
            assert info.no_content is False
            expected_path = cache_dir / "data.parquet"
            assert info.file_path == expected_path
            assert cache_dir.exists()
            mock_writer_instance.write_batch.assert_called_once_with(mock_batch)

    def test_save_parquet_empty_results(self, tmp_path):
        """Test handling of empty query results."""
        cache_dir = tmp_path / "cache"

        # Mock empty iterator
        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([])

        result = QueryResult(
            bq_read_client=Mock(),
            job=Mock(),
            rows=mock_rows,
            cache_dir=cache_dir,
            query_start_time="2024-11-27T10:00:00+00:00",
            template_hash="abc123",
        )

        info = result.save_parquet()

        # Verify no file created, but directory exists
        assert info.no_content is True
        expected_path = cache_dir / "data.parquet"
        assert info.file_path == expected_path
        assert cache_dir.exists()
        assert not expected_path.exists()

    def test_save_parquet_multiple_batches(self, tmp_path):
        """Test saving multiple Arrow batches."""
        cache_dir = tmp_path / "cache"

        # Mock multiple batches
        mock_batch1 = MagicMock()
        mock_batch1.schema = MagicMock()
        mock_batch2 = MagicMock()
        mock_batch3 = MagicMock()

        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([mock_batch1, mock_batch2, mock_batch3])

        with patch("iqb.pipeline.pq.ParquetWriter") as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            result = QueryResult(
                bq_read_client=Mock(),
                job=Mock(),
                rows=mock_rows,
                cache_dir=cache_dir,
                query_start_time="2024-11-27T10:00:00.000000Z",
                template_hash="abc123",
            )

            info = result.save_parquet()

            # Verify all batches written
            assert mock_writer_instance.write_batch.call_count == 3
            assert info.no_content is False

    def test_save_parquet_creates_nested_directories(self, tmp_path):
        """Test that save_parquet creates nested cache directory."""
        # Deep nested path
        cache_dir = tmp_path / "a" / "b" / "c"

        mock_batch = MagicMock()
        mock_batch.schema = MagicMock()

        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([mock_batch])

        with patch("iqb.pipeline.pq.ParquetWriter") as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            result = QueryResult(
                bq_read_client=Mock(),
                job=Mock(),
                rows=mock_rows,
                cache_dir=cache_dir,
                query_start_time="2024-11-27T10:00:00.000000Z",
                template_hash="abc123",
            )

            info = result.save_parquet()

            # Verify cache directory created
            assert cache_dir.exists()
            assert info.no_content is False


class TestQueryResultSaveStats:
    """Test stats.json file writing."""

    def test_save_stats_with_complete_job_info(self, tmp_path):
        """Test saving stats with complete BigQuery job information."""
        import json
        from datetime import datetime, timedelta, timezone

        cache_dir = tmp_path / "cache"

        # Mock BigQuery job with complete info
        mock_job = Mock()
        mock_job.started = datetime(2024, 11, 27, 10, 0, 0, tzinfo=timezone.utc)
        mock_job.ended = datetime(2024, 11, 27, 10, 5, 30, tzinfo=timezone.utc)
        mock_job.total_bytes_processed = 1000000000  # 1 GB
        mock_job.total_bytes_billed = 1073741824  # 1 GiB

        result = QueryResult(
            bq_read_client=Mock(),
            job=mock_job,
            rows=Mock(),
            cache_dir=cache_dir,
            query_start_time="2024-11-27T10:00:00.000000Z",
            template_hash="abc123def456",
        )

        stats_path = result.save_stats()

        # Verify file created
        assert stats_path == cache_dir / "stats.json"
        assert stats_path.exists()

        # Verify content
        with stats_path.open() as f:
            stats = json.load(f)

        assert stats["query_start_time"] == "2024-11-27T10:00:00.000000Z"
        assert stats["query_duration_seconds"] == 330.0  # 5 min 30 sec
        assert stats["template_hash"] == "abc123def456"
        assert stats["total_bytes_processed"] == 1000000000
        assert stats["total_bytes_billed"] == 1073741824

    def test_save_stats_with_incomplete_job_info(self, tmp_path):
        """Test saving stats when job timing info is incomplete."""
        import json

        cache_dir = tmp_path / "cache"

        # Mock job without timing info
        mock_job = Mock()
        mock_job.started = None
        mock_job.ended = None
        mock_job.total_bytes_processed = 500000
        mock_job.total_bytes_billed = 524288

        result = QueryResult(
            bq_read_client=Mock(),
            job=mock_job,
            rows=Mock(),
            cache_dir=cache_dir,
            query_start_time="2024-11-27T10:00:00.000000Z",
            template_hash="xyz789",
        )

        stats_path = result.save_stats()

        # Verify content
        with stats_path.open() as f:
            stats = json.load(f)

        assert stats["query_start_time"] == "2024-11-27T10:00:00.000000Z"
        assert stats["query_duration_seconds"] is None
        assert stats["template_hash"] == "xyz789"
        assert stats["total_bytes_processed"] == 500000
        assert stats["total_bytes_billed"] == 524288

    def test_save_stats_creates_directory(self, tmp_path):
        """Test that save_stats creates cache directory if needed."""
        cache_dir = tmp_path / "a" / "b" / "c"

        mock_job = Mock()
        mock_job.started = None
        mock_job.ended = None
        mock_job.total_bytes_processed = 0
        mock_job.total_bytes_billed = 0

        result = QueryResult(
            bq_read_client=Mock(),
            job=mock_job,
            rows=Mock(),
            cache_dir=cache_dir,
            query_start_time="2024-11-27T10:00:00.000000Z",
            template_hash="test123",
        )

        stats_path = result.save_stats()

        assert cache_dir.exists()
        assert stats_path.exists()


class TestParquetFileInfo:
    """Test ParquetFileInfo dataclass."""

    def test_parquet_file_info_with_content(self, tmp_path):
        """Test ParquetFileInfo creation with content."""
        test_file = tmp_path / "test.parquet"
        info = ParquetFileInfo(no_content=False, file_path=test_file)

        assert info.no_content is False
        assert info.file_path == test_file

    def test_parquet_file_info_no_content(self, tmp_path):
        """Test ParquetFileInfo creation without content."""
        test_file = tmp_path / "test.parquet"
        info = ParquetFileInfo(no_content=True, file_path=test_file)

        assert info.no_content is True
        assert info.file_path == test_file

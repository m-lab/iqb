"""Tests for the iqb.pipeline module."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
import pytest

from iqb.pipeline import (
    IQBPipeline,
    ParquetFileInfo,
    ParsedTemplateName,
    PipelineCacheEntry,
    PipelineCacheManager,
    QueryResult,
    _load_query_template,
)


class TestLoadQueryTemplate:
    """Test for _load_query_template function."""

    def test_load_query_template_substitution(self):
        """Test that query template placeholders are substituted."""
        # Parse inputs first (as the public interface does)
        tname = ParsedTemplateName(value="downloads_by_country")
        start_time = datetime(2024, 10, 1)
        end_time = datetime(2024, 11, 1)

        query, template_hash = _load_query_template(tname, start_time, end_time)

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
    """Test for IQBPipeline.__init__ method."""

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_init_default_data_dir(self, mock_storage, mock_client):
        """Test initialization with default data directory."""
        pipeline = IQBPipeline(project_id="test-project")

        # Verify that we correctly call BigQuery initializers
        mock_client.assert_called_once_with(project="test-project")
        mock_storage.assert_called_once()

        # Verify that the manager is initialized
        assert pipeline.manager is not None
        assert isinstance(pipeline.manager, PipelineCacheManager)

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_init_custom_data_dir(self, mock_storage, mock_client, tmp_path):
        """Test initialization with custom data directory."""
        custom_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project_id="test-project", data_dir=custom_dir)

        # Verify that we correctly call BigQuery initializers
        mock_client.assert_called_once_with(project="test-project")
        mock_storage.assert_called_once()

        # Verify that the manager is initialized
        assert pipeline.manager is not None
        assert isinstance(pipeline.manager, PipelineCacheManager)
        assert pipeline.manager.data_dir == custom_dir


class TestIQBPipelineExecuteQuery:
    """Test for IQBPipeline.execute_query method."""

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
    """Test for QueryResult.save_parquet method."""

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

            # Verify file path and directory creation
            expected_path = cache_dir / "data.parquet"
            assert info.file_path == expected_path
            assert cache_dir.exists()
            mock_writer_instance.write_batch.assert_called_once_with(mock_batch)

    def test_save_parquet_empty_results(self, tmp_path):
        """Test handling of empty query results - writes empty parquet file."""
        cache_dir = tmp_path / "cache"

        # Mock empty iterator
        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([])

        # Mock ParquetWriter to verify it's called with empty schema
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

            # Verify empty parquet file would be created
            expected_path = cache_dir / "data.parquet"
            assert info.file_path == expected_path
            assert cache_dir.exists()

            # Verify ParquetWriter was called with empty schema
            mock_writer.assert_called_once()
            call_args = mock_writer.call_args
            assert call_args[0][0] == expected_path.as_posix()

            # Verify schema is empty (no fields)
            schema_arg = call_args[0][1]
            assert isinstance(schema_arg, pa.Schema)
            assert len(schema_arg) == 0

            # Verify no batches were written (first_batch is None, for loop has nothing)
            mock_writer_instance.write_batch.assert_not_called()

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
            expected_path = cache_dir / "data.parquet"
            assert info.file_path == expected_path

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
            expected_path = cache_dir / "data.parquet"
            assert info.file_path == expected_path


class TestQueryResultSaveStats:
    """Test for QueryResult.save_stats method."""

    def test_save_stats_with_complete_job_info(self, tmp_path):
        """Test saving stats with complete BigQuery job information."""
        import json
        from datetime import datetime

        cache_dir = tmp_path / "cache"

        # Mock BigQuery job with complete info
        mock_job = Mock()
        mock_job.started = datetime(2024, 11, 27, 10, 0, 0, tzinfo=UTC)
        mock_job.ended = datetime(2024, 11, 27, 10, 5, 30, tzinfo=UTC)
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


class TestIQBPipelineGetCacheEntry:
    """Test IQBPipeline.get_cache_entry method."""

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_get_cache_entry_when_exists(self, mock_storage, mock_client, tmp_path):
        """Test get_cache_entry returns existing cache."""
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project_id="test-project", data_dir=data_dir)

        # Create cache directory and files
        cache_dir = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "data.parquet").write_text("fake parquet data")
        (cache_dir / "stats.json").write_text("{}")

        # Get cache entry (should not execute query)
        entry = pipeline.get_cache_entry("downloads_by_country", "2024-10-01", "2024-11-01")

        assert isinstance(entry, PipelineCacheEntry)
        data_path = entry.data_parquet_file_path()
        assert data_path == cache_dir / "data.parquet"
        assert data_path.exists()

        stats_path = entry.stats_json_file_path()
        assert stats_path == cache_dir / "stats.json"
        assert stats_path.exists()

        # Query should NOT have been called
        mock_client.return_value.query.assert_not_called()

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_get_cache_entry_missing_without_fetch(self, mock_storage, mock_client, tmp_path):
        """Test get_cache_entry raises FileNotFoundError when cache missing and fetch_if_missing=False."""
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project_id="test-project", data_dir=data_dir)

        # Don't create cache
        with pytest.raises(FileNotFoundError, match="Cache entry not found"):
            pipeline.get_cache_entry(
                "downloads_by_country",
                "2024-10-01",
                "2024-11-01",
                fetch_if_missing=False,
            )

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_get_cache_entry_fetch_if_missing(self, mock_storage, mock_client, tmp_path):
        """Test get_cache_entry executes query when cache missing and fetch_if_missing=True."""
        data_dir = tmp_path / "iqb"

        # Setup mocks for query execution
        mock_job = Mock()
        mock_job.started = None
        mock_job.ended = None
        mock_job.total_bytes_processed = 0
        mock_job.total_bytes_billed = 0

        mock_batch = MagicMock()
        mock_batch.schema = MagicMock()

        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([mock_batch])

        mock_client_instance = Mock()
        mock_client_instance.query.return_value = mock_job
        mock_job.result.return_value = mock_rows
        mock_client.return_value = mock_client_instance

        pipeline = IQBPipeline(project_id="test-project", data_dir=data_dir)

        # Expected cache directory
        expected_cache_dir = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )

        # Mock ParquetWriter and ensure the file is created
        with patch("iqb.pipeline.pq.ParquetWriter") as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            # Create a side effect to actually create the data.parquet file
            def create_parquet_file(*args, **kwargs):
                expected_cache_dir.mkdir(parents=True, exist_ok=True)
                (expected_cache_dir / "data.parquet").write_text("fake data")
                return MagicMock()

            mock_writer.side_effect = create_parquet_file

            # Get cache entry with fetch_if_missing=True
            entry = pipeline.get_cache_entry(
                "downloads_by_country",
                "2024-10-01",
                "2024-11-01",
                fetch_if_missing=True,
            )

            # Query should have been called
            mock_client_instance.query.assert_called_once()

            # Entry should be returned with correct paths
            assert isinstance(entry, PipelineCacheEntry)
            data_path = entry.data_parquet_file_path()
            stats_path = entry.stats_json_file_path()
            assert data_path == expected_cache_dir / "data.parquet"
            assert stats_path == expected_cache_dir / "stats.json"

            # Both files should exist
            assert data_path.exists()
            assert stats_path.exists()

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_get_cache_entry_partial_cache_data_only(self, mock_storage, mock_client, tmp_path):
        """Test get_cache_entry when only data.parquet exists (missing stats.json)."""
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project_id="test-project", data_dir=data_dir)

        # Create only data.parquet, not stats.json
        cache_dir = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "data.parquet").write_text("fake parquet data")
        # Intentionally NOT creating stats.json

        # Should raise FileNotFoundError (cache incomplete)
        with pytest.raises(FileNotFoundError, match="Cache entry not found"):
            pipeline.get_cache_entry("downloads_by_country", "2024-10-01", "2024-11-01")

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_get_cache_entry_partial_cache_stats_only(self, mock_storage, mock_client, tmp_path):
        """Test get_cache_entry when only stats.json exists (missing data.parquet)."""
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project_id="test-project", data_dir=data_dir)

        # Create only stats.json, not data.parquet
        cache_dir = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "stats.json").write_text("{}")
        # Intentionally NOT creating data.parquet

        # Should raise FileNotFoundError (cache incomplete)
        with pytest.raises(FileNotFoundError, match="Cache entry not found"):
            pipeline.get_cache_entry("downloads_by_country", "2024-10-01", "2024-11-01")

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_get_cache_entry_validation_before_fs_check(self, mock_storage, mock_client, tmp_path):
        """Test that input validation happens before filesystem check."""
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project_id="test-project", data_dir=data_dir)

        # Invalid template should fail immediately, without touching filesystem
        with pytest.raises(ValueError, match="Unknown template"):
            pipeline.get_cache_entry("invalid_template", "2024-10-01", "2024-11-01")

        # Invalid date range should fail immediately
        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            pipeline.get_cache_entry("downloads_by_country", "2024-11-01", "2024-10-01")


class TestParquetFileInfo:
    """Test ParquetFileInfo class."""

    def test_parquet_file_info_creation(self, tmp_path):
        """Test ParquetFileInfo creation."""
        test_file = tmp_path / "test.parquet"
        info = ParquetFileInfo(file_path=test_file)

        assert info.file_path == test_file

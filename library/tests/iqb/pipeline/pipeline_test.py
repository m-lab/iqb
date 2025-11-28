"""Tests for the iqb.pipeline module."""

from datetime import UTC, datetime
from pathlib import Path
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
    _parse_both_dates,
    _parse_date,
    _parse_template_name,
    data_dir_or_default,
)


class TestDataDirOrDefault:
    """Test for data_dir_or_default function."""

    def test_data_dir_or_default_with_none(self):
        """Test default behavior when data_dir is None."""
        result = data_dir_or_default(None)
        expected = Path.cwd() / ".iqb"
        assert result == expected

    def test_data_dir_or_default_with_string(self, tmp_path):
        """Test conversion of string path."""
        test_path = str(tmp_path / "test")
        result = data_dir_or_default(test_path)
        assert result == Path(test_path)

    def test_data_dir_or_default_with_path(self, tmp_path):
        """Test pass-through of Path object."""
        input_path = tmp_path / "test"
        result = data_dir_or_default(input_path)
        assert result == input_path


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


class TestParseTemplateName:
    """Test _parse_template_name() validation."""

    def test_parse_valid_downloads_by_country(self):
        """Test parsing valid downloads_by_country template."""
        result = _parse_template_name("downloads_by_country")
        assert isinstance(result, ParsedTemplateName)
        assert result.value == "downloads_by_country"

    def test_parse_valid_uploads_by_country(self):
        """Test parsing valid uploads_by_country template."""
        result = _parse_template_name("uploads_by_country")
        assert result.value == "uploads_by_country"

    def test_parse_invalid_template_name(self):
        """Test error on unknown template name."""
        with pytest.raises(ValueError, match="Unknown template 'invalid_query'"):
            _parse_template_name("invalid_query")

    def test_parse_invalid_template_lists_valid_options(self):
        """Test error message includes valid template names."""
        with pytest.raises(ValueError, match="valid templates:"):
            _parse_template_name("bad_template")

    def test_parse_empty_string(self):
        """Test error on empty template name."""
        with pytest.raises(ValueError, match="Unknown template ''"):
            _parse_template_name("")


class TestParseBothDates:
    """Test _parse_both_dates() validation."""

    def test_parse_valid_date_range(self):
        """Test parsing valid date range."""
        start, end = _parse_both_dates("2024-10-01", "2024-11-01")
        assert start == datetime(2024, 10, 1)
        assert end == datetime(2024, 11, 1)

    def test_parse_equal_dates(self):
        """Test parsing when start equals end (valid for zero-duration queries)."""
        start, end = _parse_both_dates("2024-10-01", "2024-10-01")
        assert start == datetime(2024, 10, 1)
        assert end == datetime(2024, 10, 1)

    def test_parse_reversed_dates_error(self):
        """Test error when start_date > end_date."""
        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            _parse_both_dates("2024-11-01", "2024-10-01")

    def test_parse_invalid_start_date(self):
        """Test error on invalid start_date format."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _parse_both_dates("2024/10/01", "2024-11-01")

    def test_parse_invalid_end_date(self):
        """Test error on invalid end_date format."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _parse_both_dates("2024-10-01", "2024/11/01")

    def test_parse_leap_year(self):
        """Test parsing leap year date."""
        start, end = _parse_both_dates("2024-02-29", "2024-03-01")
        assert start == datetime(2024, 2, 29)
        assert end == datetime(2024, 3, 1)

    def test_parse_invalid_leap_year(self):
        """Test error on invalid leap year date."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _parse_both_dates("2023-02-29", "2023-03-01")

    def test_parse_year_boundary(self):
        """Test parsing dates across year boundary."""
        start, end = _parse_both_dates("2024-12-15", "2025-01-15")
        assert start == datetime(2024, 12, 15)
        assert end == datetime(2025, 1, 15)


class TestIQBPipelineInit:
    """Test IQBPipeline initialization."""

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_init_default_data_dir(self, mock_storage, mock_client):
        """Test initialization with default data directory."""
        pipeline = IQBPipeline(project_id="test-project")

        mock_client.assert_called_once_with(project="test-project")
        mock_storage.assert_called_once()
        # Verify manager is initialized (internal implementation)
        assert pipeline.manager is not None
        assert isinstance(pipeline.manager, PipelineCacheManager)

    @patch("iqb.pipeline.bigquery.Client")
    @patch("iqb.pipeline.bigquery_storage_v1.BigQueryReadClient")
    def test_init_custom_data_dir(self, mock_storage, mock_client, tmp_path):
        """Test initialization with custom data directory."""
        custom_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project_id="test-project", data_dir=custom_dir)

        # Verify manager is initialized with custom data_dir
        assert isinstance(pipeline.manager, PipelineCacheManager)
        assert pipeline.manager.data_dir == custom_dir


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
    """Test stats.json file writing."""

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


class TestIQBPipelineGetPipelineCacheEntry:
    """Test get_cache_entry method."""

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
        data_path = entry.data_path()
        assert data_path is not None
        assert data_path == cache_dir / "data.parquet"
        assert data_path.exists()

        stats_path = entry.stats_path()
        assert stats_path is not None
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
            data_path = entry.data_path()
            assert data_path is not None
            stats_path = entry.stats_path()
            assert stats_path is not None
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


class TestPipelineCacheManager:
    """Test PipelineCacheManager (internal component)."""

    def test_init_default_data_dir(self):
        """Test manager initialization with default data directory."""
        manager = PipelineCacheManager()
        assert manager.data_dir == Path.cwd() / ".iqb"

    def test_init_custom_data_dir(self, tmp_path):
        """Test manager initialization with custom data directory."""
        custom_dir = tmp_path / "custom"
        manager = PipelineCacheManager(data_dir=custom_dir)
        assert manager.data_dir == custom_dir

    def test_get_cache_entry_returns_entry(self, tmp_path):
        """Test that get_cache_entry returns PipelineCacheEntry."""
        manager = PipelineCacheManager(data_dir=tmp_path)
        entry = manager.get_cache_entry(
            template="downloads_by_country",
            start_date="2024-10-01",
            end_date="2024-11-01",
        )

        assert isinstance(entry, PipelineCacheEntry)
        assert entry.data_dir == tmp_path
        assert entry.tname.value == "downloads_by_country"
        assert entry.start_time == datetime(2024, 10, 1)
        assert entry.end_time == datetime(2024, 11, 1)

    def test_get_cache_entry_validates_template(self, tmp_path):
        """Test that get_cache_entry validates template name."""
        manager = PipelineCacheManager(data_dir=tmp_path)

        with pytest.raises(ValueError, match="Unknown template"):
            manager.get_cache_entry(
                template="invalid_template",
                start_date="2024-10-01",
                end_date="2024-11-01",
            )

    def test_get_cache_entry_validates_dates(self, tmp_path):
        """Test that get_cache_entry validates date range."""
        manager = PipelineCacheManager(data_dir=tmp_path)

        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            manager.get_cache_entry(
                template="downloads_by_country",
                start_date="2024-11-01",
                end_date="2024-10-01",
            )


class TestPipelineCacheEntry:
    """Test PipelineCacheEntry value object."""

    def test_dir_path_construction(self, tmp_path):
        """Test that dir_path constructs correct cache directory path."""
        entry = PipelineCacheEntry(
            data_dir=tmp_path,
            tname=ParsedTemplateName(value="downloads_by_country"),
            start_time=datetime(2024, 10, 1),
            end_time=datetime(2024, 11, 1),
        )

        expected = (
            tmp_path
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )
        assert entry.dir_path() == expected

    def test_data_path_when_file_exists(self, tmp_path):
        """Test data_path returns path when file exists."""
        entry = PipelineCacheEntry(
            data_dir=tmp_path,
            tname=ParsedTemplateName(value="downloads_by_country"),
            start_time=datetime(2024, 10, 1),
            end_time=datetime(2024, 11, 1),
        )

        # Create the file
        cache_dir = entry.dir_path()
        cache_dir.mkdir(parents=True, exist_ok=True)
        data_file = cache_dir / "data.parquet"
        data_file.write_text("fake data")

        assert entry.data_path() == data_file

    def test_data_path_when_file_missing(self, tmp_path):
        """Test data_path returns None when file doesn't exist."""
        entry = PipelineCacheEntry(
            data_dir=tmp_path,
            tname=ParsedTemplateName(value="downloads_by_country"),
            start_time=datetime(2024, 10, 1),
            end_time=datetime(2024, 11, 1),
        )

        assert entry.data_path() is None

    def test_stats_path_when_file_exists(self, tmp_path):
        """Test stats_path returns path when file exists."""
        entry = PipelineCacheEntry(
            data_dir=tmp_path,
            tname=ParsedTemplateName(value="downloads_by_country"),
            start_time=datetime(2024, 10, 1),
            end_time=datetime(2024, 11, 1),
        )

        # Create the file
        cache_dir = entry.dir_path()
        cache_dir.mkdir(parents=True, exist_ok=True)
        stats_file = cache_dir / "stats.json"
        stats_file.write_text("{}")

        assert entry.stats_path() == stats_file

    def test_stats_path_when_file_missing(self, tmp_path):
        """Test stats_path returns None when file doesn't exist."""
        entry = PipelineCacheEntry(
            data_dir=tmp_path,
            tname=ParsedTemplateName(value="downloads_by_country"),
            start_time=datetime(2024, 10, 1),
            end_time=datetime(2024, 11, 1),
        )

        assert entry.stats_path() is None


class TestParquetFileInfo:
    """Test ParquetFileInfo dataclass."""

    def test_parquet_file_info_creation(self, tmp_path):
        """Test ParquetFileInfo creation."""
        test_file = tmp_path / "test.parquet"
        info = ParquetFileInfo(file_path=test_file)

        assert info.file_path == test_file

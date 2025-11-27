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
        query = _load_query_template("downloads_by_country", "2024-10-01", "2024-11-01")

        # Verify placeholders replaced
        assert "{START_DATE}" not in query
        assert "{END_DATE}" not in query
        assert "2024-10-01" in query
        assert "2024-11-01" in query


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
    def test_execute_query_template_path_construction(
        self, mock_storage, mock_client, tmp_path
    ):
        """Test that execute_query_template constructs correct parquet path."""
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

        # Verify path construction
        expected_path = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country.parquet"
        )
        assert result.parquet_path == expected_path
        assert result.rows == mock_rows
        assert result.job == mock_job

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

        # Verify correct filename
        expected_path = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "uploads_by_country.parquet"
        )
        assert result.parquet_path == expected_path


class TestQueryResultSaveParquet:
    """Test parquet file saving with mocked Arrow data."""

    def test_save_parquet_with_data(self, tmp_path):
        """Test saving parquet file with mock data."""
        parquet_path = tmp_path / "test.parquet"

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
                parquet_path=parquet_path,
            )

            info = result.save_parquet()

            # Verify
            assert info.no_content is False
            assert info.file_path == parquet_path
            assert parquet_path.parent.exists()
            mock_writer_instance.write_batch.assert_called_once_with(mock_batch)

    def test_save_parquet_empty_results(self, tmp_path):
        """Test handling of empty query results."""
        parquet_path = tmp_path / "test.parquet"

        # Mock empty iterator
        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter([])

        result = QueryResult(
            bq_read_client=Mock(),
            job=Mock(),
            rows=mock_rows,
            parquet_path=parquet_path,
        )

        info = result.save_parquet()

        # Verify no file created, but directory exists
        assert info.no_content is True
        assert info.file_path == parquet_path
        assert parquet_path.parent.exists()
        assert not parquet_path.exists()

    def test_save_parquet_multiple_batches(self, tmp_path):
        """Test saving multiple Arrow batches."""
        parquet_path = tmp_path / "test.parquet"

        # Mock multiple batches
        mock_batch1 = MagicMock()
        mock_batch1.schema = MagicMock()
        mock_batch2 = MagicMock()
        mock_batch3 = MagicMock()

        mock_rows = Mock()
        mock_rows.to_arrow_iterable.return_value = iter(
            [mock_batch1, mock_batch2, mock_batch3]
        )

        with patch("iqb.pipeline.pq.ParquetWriter") as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            result = QueryResult(
                bq_read_client=Mock(),
                job=Mock(),
                rows=mock_rows,
                parquet_path=parquet_path,
            )

            info = result.save_parquet()

            # Verify all batches written
            assert mock_writer_instance.write_batch.call_count == 3
            assert info.no_content is False

    def test_save_parquet_creates_nested_directories(self, tmp_path):
        """Test that save_parquet creates nested parent directories."""
        # Deep nested path
        parquet_path = tmp_path / "a" / "b" / "c" / "test.parquet"

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
                parquet_path=parquet_path,
            )

            info = result.save_parquet()

            # Verify all parent directories created
            assert parquet_path.parent.exists()
            assert info.no_content is False


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

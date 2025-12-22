"""Tests for the iqb.pipeline.pipeline module."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from iqb.pipeline.bqpq import PipelineBQPQQueryResult
from iqb.pipeline.pipeline import (
    IQBPipeline,
    PipelineCacheEntry,
    _load_query_template,
)


class TestLoadQueryTemplate:
    """Test for _load_query_template function."""

    def test_load_query_template_substitution(self):
        """Test that query template placeholders are substituted."""
        # Parse inputs first (as the public interface does)
        dataset_name = "downloads_by_country"
        start_time = datetime(2024, 10, 1)
        end_time = datetime(2024, 11, 1)

        query, template_hash = _load_query_template(dataset_name, start_time, end_time)

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

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    @patch("iqb.pipeline.pipeline.PipelineCacheManager")
    def test_init_default_data_dir(self, mock_manager, mock_client):
        """Test initialization with default data directory."""
        pipeline = IQBPipeline(project="test-project")

        # Verify that we correctly instantiated dependencies
        mock_client.assert_called_once_with(project="test-project")
        mock_manager.assert_called_once_with(data_dir=None, remote_cache=None)

        # Verify that the client is initialized
        assert pipeline.client is not None

        # Verify that the manager is initialized
        assert pipeline.manager is not None

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    @patch("iqb.pipeline.pipeline.PipelineCacheManager")
    def test_init_custom_data_dir(self, mock_manager, mock_client, tmp_path):
        """Test initialization with custom data directory."""
        custom_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project="test-project", data_dir=custom_dir)

        # Verify that we correctly instantiated dependencies
        mock_client.assert_called_once_with(project="test-project")
        mock_manager.assert_called_once_with(data_dir=custom_dir, remote_cache=None)

        # Verify that the client is initialized
        assert pipeline.client is not None

        # Verify that the manager is initialized
        assert pipeline.manager is not None


class TestIQBPipelineExecuteQuery:
    """Test for IQBPipeline.execute_query method."""

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_execute_query_template_path_construction(self, mock_client, tmp_path):
        """Test that execute_query_template constructs correct cache directory."""
        # Create the pipeline
        pipeline = IQBPipeline(project="test-project", data_dir=tmp_path)

        # Set up what IQBPipeline.execute_query should return
        mock_query_result = MagicMock(spec=PipelineBQPQQueryResult)
        mock_client.return_value.execute_query.return_value = mock_query_result

        # Execute query
        result = pipeline.execute_query_template(
            dataset_name="downloads_by_country",
            start_date="2024-10-01",
            end_date="2024-11-01",
        )

        # Verify execute_query was called
        mock_client.return_value.execute_query.assert_called_once()
        call_args = mock_client.return_value.execute_query.call_args
        assert call_args.kwargs["template_hash"] is not None  # SHA256 hash
        assert "SELECT" in call_args.kwargs["query"]  # Contains SQL
        assert call_args.kwargs["paths_provider"] is not None  # PipelineCacheEntry

        # Verify the result is what we expect
        assert result is mock_query_result
        paths_provider = call_args.kwargs["paths_provider"]
        expected_dir = (
            tmp_path
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )
        assert paths_provider.data_parquet_file_path() == expected_dir / "data.parquet"
        assert paths_provider.stats_json_file_path() == expected_dir / "stats.json"

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_execute_query_invalid_dates(self, mock_client, tmp_path):
        """Test error when start_date > end_date."""
        # Create the pipeline
        pipeline = IQBPipeline(project="test-project", data_dir=tmp_path)

        # Verify that we correctly instantiated dependencies
        mock_client.assert_called_once_with(project="test-project")

        # Ensure we get the expected exception
        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            pipeline.execute_query_template(
                dataset_name="downloads_by_country",
                start_date="2024-11-01",
                end_date="2024-10-01",
            )

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_execute_query_invalid_dataset_name(self, mock_client, tmp_path):
        """Test error on unknown template name."""
        # Create the pipeline
        pipeline = IQBPipeline(project="test-project", data_dir=tmp_path)

        # Verify that we correctly instantiated dependencies
        mock_client.assert_called_once_with(project="test-project")

        # Ensure we get the expected exception
        with pytest.raises(ValueError, match="Invalid dataset name"):
            pipeline.execute_query_template(
                dataset_name="invalid_dataset-name",
                start_date="2024-10-01",
                end_date="2024-11-01",
            )


class TestIQBPipelineGetCacheEntry:
    """Test IQBPipeline.get_cache_entry method."""

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_get_cache_entry_when_exists(self, mock_client, tmp_path):
        """Test get_cache_entry returns existing cache."""
        # Create the pipeline
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project="test-project", data_dir=data_dir)

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
        entry = pipeline.get_cache_entry(
            dataset_name="downloads_by_country",
            start_date="2024-10-01",
            end_date="2024-11-01",
        )

        # Ensure that we have both files
        assert isinstance(entry, PipelineCacheEntry)
        data_path = entry.data_parquet_file_path()
        assert data_path == cache_dir / "data.parquet"
        assert data_path.exists()

        stats_path = entry.stats_json_file_path()
        assert stats_path == cache_dir / "stats.json"
        assert stats_path.exists()

        # Query should NOT have been called
        mock_client.return_value.query.assert_not_called()

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_get_cache_entry_missing_without_fetch(self, mock_client, tmp_path):
        """Test get_cache_entry raises FileNotFoundError when cache missing and fetch_if_missing=False."""
        # Create the pipeline
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project="test-project", data_dir=data_dir)

        # Verify that we correctly instantiated dependencies
        mock_client.assert_called_once_with(project="test-project")

        # Don't create cache
        with pytest.raises(FileNotFoundError, match="Cache entry not found"):
            pipeline.get_cache_entry(
                dataset_name="downloads_by_country",
                start_date="2024-10-01",
                end_date="2024-11-01",
                fetch_if_missing=False,
            )

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_get_cache_entry_fetch_if_missing(self, mock_client, tmp_path):
        """Test get_cache_entry executes query when cache missing and fetch_if_missing=True."""
        # Create the pipeline
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project="test-project", data_dir=data_dir)

        # Expected cache directory
        expected_cache_dir = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )

        # Mock the query result to create files when save methods are called
        mock_result = MagicMock(spec=PipelineBQPQQueryResult)

        def mock_save_parquet():
            expected_cache_dir.mkdir(parents=True, exist_ok=True)
            (expected_cache_dir / "data.parquet").write_text("fake")
            return expected_cache_dir / "data.parquet"

        def mock_save_stats():
            expected_cache_dir.mkdir(parents=True, exist_ok=True)
            (expected_cache_dir / "stats.json").write_text("{}")
            return expected_cache_dir / "stats.json"

        mock_result.save_data_parquet = mock_save_parquet
        mock_result.save_stats_json = mock_save_stats

        # Tell execute_query to return our mock result
        mock_client.return_value.execute_query.return_value = mock_result

        # Get cache entry with fetch_if_missing=True
        entry = pipeline.get_cache_entry(
            dataset_name="downloads_by_country",
            start_date="2024-10-01",
            end_date="2024-11-01",
            fetch_if_missing=True,
        )

        # Verify execute_query was called
        mock_client.return_value.execute_query.assert_called_once()

        # Verify the returned entry has correct paths
        assert isinstance(entry, PipelineCacheEntry)
        assert entry.data_parquet_file_path() == expected_cache_dir / "data.parquet"
        assert entry.stats_json_file_path() == expected_cache_dir / "stats.json"

        # Verify files were created by the mock save methods
        assert entry.data_parquet_file_path().exists()
        assert entry.stats_json_file_path().exists()

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_get_cache_entry_partial_cache_data_only(self, mock_client, tmp_path):
        """Test get_cache_entry when only data.parquet exists (missing stats.json)."""
        # Create the pipeline
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project="test-project", data_dir=data_dir)

        # Verify that we correctly instantiated dependencies
        mock_client.assert_called_once_with(project="test-project")

        # Create only data.parquet, NOT stats.json
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

        # Should raise FileNotFoundError (cache incomplete)
        with pytest.raises(FileNotFoundError, match="Cache entry not found"):
            pipeline.get_cache_entry(
                dataset_name="downloads_by_country",
                start_date="2024-10-01",
                end_date="2024-11-01",
            )

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_get_cache_entry_partial_cache_stats_only(self, mock_client, tmp_path):
        """Test get_cache_entry when only stats.json exists (missing data.parquet)."""
        # Create the pipeline
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project="test-project", data_dir=data_dir)

        # Verify that we correctly instantiated dependencies
        mock_client.assert_called_once_with(project="test-project")

        # Create only stats.json, NOT data.parquet
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

        # Should raise FileNotFoundError (cache incomplete)
        with pytest.raises(FileNotFoundError, match="Cache entry not found"):
            pipeline.get_cache_entry(
                dataset_name="downloads_by_country",
                start_date="2024-10-01",
                end_date="2024-11-01",
            )

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_get_cache_entry_validation_checks(self, mock_client, tmp_path):
        """Test that there are exceptions if input is wrong."""
        # Create the pipeline
        data_dir = tmp_path / "iqb"
        pipeline = IQBPipeline(project="test-project", data_dir=data_dir)

        # Verify that we correctly instantiated dependencies
        mock_client.assert_called_once_with(project="test-project")

        # Invalid dataset name should fail immediately
        with pytest.raises(ValueError, match="Invalid dataset name"):
            pipeline.get_cache_entry(
                dataset_name="invalid_dataset-name",
                start_date="2024-10-01",
                end_date="2024-11-01",
            )

        # Invalid date range should fail immediately
        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            pipeline.get_cache_entry(
                dataset_name="downloads_by_country",
                start_date="2024-11-01",
                end_date="2024-10-01",
            )

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_get_cache_entry_with_remote_cache_success(self, mock_client, tmp_path):
        """Test get_cache_entry uses remote cache when local cache missing."""
        # Expected cache directory
        data_dir = tmp_path / "iqb"
        expected_cache_dir = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )

        # Create a mock remote cache that succeeds
        class MockRemoteCache:
            def __init__(self):
                self.sync_called = False

            def sync(self, entry: PipelineCacheEntry) -> bool:
                """Simulate successful remote cache sync."""
                _ = entry
                self.sync_called = True
                # Create the files that remote cache would download
                expected_cache_dir.mkdir(parents=True, exist_ok=True)
                (expected_cache_dir / "data.parquet").write_text("remote data")
                (expected_cache_dir / "stats.json").write_text("{}")
                return True

        mock_remote = MockRemoteCache()

        # Create the pipeline with remote_cache
        pipeline = IQBPipeline(project="test-project", data_dir=data_dir, remote_cache=mock_remote)

        # Get cache entry (should use remote, not BigQuery)
        entry = pipeline.get_cache_entry(
            dataset_name="downloads_by_country",
            start_date="2024-10-01",
            end_date="2024-11-01",
            fetch_if_missing=True,
        )

        # Verify remote cache sync was called
        assert mock_remote.sync_called

        # Verify BigQuery was NOT called (remote cache succeeded)
        mock_client.return_value.execute_query.assert_not_called()

        # Verify the returned entry has correct paths and files exist
        assert isinstance(entry, PipelineCacheEntry)
        assert entry.data_parquet_file_path().exists()
        assert entry.stats_json_file_path().exists()

    @patch("iqb.pipeline.pipeline.PipelineBQPQClient")
    def test_get_cache_entry_with_remote_cache_failure(self, mock_client, tmp_path):
        """Test get_cache_entry falls back to BigQuery when remote cache fails."""
        # Expected cache directory
        data_dir = tmp_path / "iqb"
        expected_cache_dir = (
            data_dir
            / "cache"
            / "v1"
            / "20241001T000000Z"
            / "20241101T000000Z"
            / "downloads_by_country"
        )

        # Create a mock remote cache that fails
        class MockRemoteCache:
            def __init__(self):
                self.sync_called = False

            def sync(self, entry: PipelineCacheEntry) -> bool:
                """Simulate remote cache sync failure."""
                _ = entry
                self.sync_called = True
                return False  # Sync failed

        mock_remote = MockRemoteCache()

        # Mock the BigQuery query result
        mock_result = MagicMock(spec=PipelineBQPQQueryResult)

        def mock_save_parquet():
            expected_cache_dir.mkdir(parents=True, exist_ok=True)
            (expected_cache_dir / "data.parquet").write_text("bq data")
            return expected_cache_dir / "data.parquet"

        def mock_save_stats():
            expected_cache_dir.mkdir(parents=True, exist_ok=True)
            (expected_cache_dir / "stats.json").write_text("{}")
            return expected_cache_dir / "stats.json"

        mock_result.save_data_parquet = mock_save_parquet
        mock_result.save_stats_json = mock_save_stats
        mock_client.return_value.execute_query.return_value = mock_result

        # Create the pipeline with remote_cache
        pipeline = IQBPipeline(project="test-project", data_dir=data_dir, remote_cache=mock_remote)

        # Get cache entry (should fallback to BigQuery)
        entry = pipeline.get_cache_entry(
            dataset_name="downloads_by_country",
            start_date="2024-10-01",
            end_date="2024-11-01",
            fetch_if_missing=True,
        )

        # Verify remote cache sync was attempted
        assert mock_remote.sync_called

        # Verify BigQuery WAS called (remote cache failed)
        mock_client.return_value.execute_query.assert_called_once()

        # Verify the returned entry has correct paths and files exist
        assert isinstance(entry, PipelineCacheEntry)
        assert entry.data_parquet_file_path().exists()
        assert entry.stats_json_file_path().exists()

"""Tests for the iqb.scripting.iqb_pipeline module."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import Mock, call, patch

import pytest

from iqb.scripting import iqb_pipeline


class _DummyEntry:
    def __init__(self, *, exists: bool) -> None:
        self._exists = exists
        self.synced = False
        self.locked = 0

    def exists(self) -> bool:
        return self._exists

    def sync(self) -> None:
        self.synced = True

    @contextmanager
    def lock(self):
        self.locked += 1
        yield self


class TestPipelineSyncMlab:
    """Tests for Pipeline.sync_mlab."""

    def test_syncs_missing_entries(self) -> None:
        entry_download = _DummyEntry(exists=False)
        entry_upload = _DummyEntry(exists=False)
        pipeline = Mock()
        pipeline.get_cache_entry.side_effect = [entry_download, entry_upload]

        wrapper = iqb_pipeline.Pipeline(pipeline=pipeline)
        wrapper.sync_mlab("country", start_date="2024-01-01", end_date="2024-02-01")

        assert entry_download.synced is True
        assert entry_upload.synced is True
        assert entry_download.locked == 1
        assert entry_upload.locked == 1
        assert pipeline.get_cache_entry.call_args_list == [
            call(
                dataset_name="downloads_by_country",
                start_date="2024-01-01",
                end_date="2024-02-01",
            ),
            call(
                dataset_name="uploads_by_country",
                start_date="2024-01-01",
                end_date="2024-02-01",
            ),
        ]

    def test_skips_existing_entries(self) -> None:
        entry_download = _DummyEntry(exists=True)
        entry_upload = _DummyEntry(exists=True)
        pipeline = Mock()
        pipeline.get_cache_entry.side_effect = [entry_download, entry_upload]

        wrapper = iqb_pipeline.Pipeline(pipeline=pipeline)
        wrapper.sync_mlab("country", start_date="2024-01-01", end_date="2024-02-01")

        assert entry_download.synced is False
        assert entry_upload.synced is False

    def test_invalid_granularity_raises(self) -> None:
        pipeline = Mock()
        wrapper = iqb_pipeline.Pipeline(pipeline=pipeline)

        with pytest.raises(ValueError, match="invalid granularity value"):
            wrapper.sync_mlab("nope", start_date="2024-01-01", end_date="2024-02-01")

        pipeline.get_cache_entry.assert_not_called()


class TestCreate:
    """Tests for iqb_pipeline.create."""

    def test_defaults(self) -> None:
        pipeline_instance = Mock()
        with (
            patch("iqb.scripting.iqb_pipeline.IQBGitHubRemoteCache") as cache_cls,
            patch(
                "iqb.scripting.iqb_pipeline.IQBPipeline", return_value=pipeline_instance
            ) as pipeline_cls,
        ):
            wrapper = iqb_pipeline.create()

        cache_cls.assert_called_once_with(data_dir=None)
        pipeline_cls.assert_called_once_with(
            project="measurement-lab",
            data_dir=None,
            remote_cache=cache_cls.return_value,
        )
        assert wrapper.pipeline is pipeline_instance

    def test_custom_args(self, tmp_path) -> None:
        pipeline_instance = Mock()
        with (
            patch("iqb.scripting.iqb_pipeline.IQBGitHubRemoteCache") as cache_cls,
            patch(
                "iqb.scripting.iqb_pipeline.IQBPipeline", return_value=pipeline_instance
            ) as pipeline_cls,
        ):
            wrapper = iqb_pipeline.create(data_dir=tmp_path, project="example-project")

        cache_cls.assert_called_once_with(data_dir=tmp_path)
        pipeline_cls.assert_called_once_with(
            project="example-project",
            data_dir=tmp_path,
            remote_cache=cache_cls.return_value,
        )
        assert wrapper.pipeline is pipeline_instance

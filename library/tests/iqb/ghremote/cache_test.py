"""Tests for the iqb.ghremote.cache module."""

import hashlib
import json
import logging
from unittest.mock import Mock, patch
from urllib.error import URLError

import pytest
from dacite.exceptions import WrongTypeError

from iqb.ghremote.cache import (
    FileEntry,
    IQBGitHubRemoteCache,
    Manifest,
)


def _compute_test_sha256(content: bytes) -> str:
    """Helper to compute SHA256 for test data."""
    return hashlib.sha256(content).hexdigest()


def _manifest_path_for_data_dir(data_dir):
    return data_dir / "state" / "ghremote" / "manifest.json"


def _write_manifest(data_dir, manifest_data):
    manifest_path = _manifest_path_for_data_dir(data_dir)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest_data))
    return manifest_path


class TestIQBGitHubLoadManifest:
    """Tests for loading the GitHub remote manifest."""

    def test_load_invalid_json_string(self, tmp_path):
        """Verify we get JSONDecodeError when loading an invalid JSON string."""
        manifest_file = _manifest_path_for_data_dir(tmp_path)
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        manifest_file.write_text("{ invalid json }")

        with pytest.raises(json.JSONDecodeError):
            IQBGitHubRemoteCache(data_dir=tmp_path)

    def test_load_invalid_json_fields_types(self, tmp_path):
        """Verify that dacite throws if the fields have invalid types."""
        manifest_file = _manifest_path_for_data_dir(tmp_path)
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        # v should be int, not string
        manifest_file.write_text('{"v": "not an int", "files": {}}')

        with pytest.raises((WrongTypeError, ValueError, TypeError)):
            IQBGitHubRemoteCache(data_dir=tmp_path)

    def test_load_invalid_version_number(self, tmp_path):
        """Verify that there is a ValueError when the version number is invalid."""
        manifest_file = _manifest_path_for_data_dir(tmp_path)
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        manifest_file.write_text('{"v": 1, "files": {}}')

        with pytest.raises(ValueError, match="Unsupported manifest version"):
            IQBGitHubRemoteCache(data_dir=tmp_path)

    def test_load_success_with_file(self, tmp_path):
        """Verify that we can correctly load a manifest when backed by an existing file."""
        manifest_data = {
            "v": 0,
            "files": {
                "cache/v1/2024-01-01/2024-01-31/test/data.parquet": {
                    "sha256": "abc123def456",
                    "url": "https://example.com/file.parquet",
                }
            },
        }
        _write_manifest(tmp_path, manifest_data)
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)
        manifest = cache.manifest

        assert manifest.v == 0
        assert len(manifest.files) == 1
        assert "cache/v1/2024-01-01/2024-01-31/test/data.parquet" in manifest.files
        entry = manifest.files["cache/v1/2024-01-01/2024-01-31/test/data.parquet"]
        assert entry.sha256 == "abc123def456"
        assert entry.url == "https://example.com/file.parquet"

    def test_load_success_without_file(self, tmp_path):
        """Verify that we return a default manifest when the file is missing."""
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)
        manifest = cache.manifest

        assert manifest.v == 0
        assert len(manifest.files) == 0


class TestManifestGetFileEntry:
    """Tests for the Manifest.get_file_entry method."""

    def test_success(self, tmp_path):
        """Verify that we get a FileEntry when the entry exists."""
        # Create a manifest with a file entry
        entry = FileEntry(sha256="abc123def456", url="https://example.com/file.parquet")
        manifest = Manifest(
            v=0,
            files={"cache/v1/2024-01-01/2024-01-31/test/data.parquet": entry},
        )

        # Set up paths - using tmp_path to ensure they're realistic
        data_dir = tmp_path / "data"
        full_path = data_dir / "cache/v1/2024-01-01/2024-01-31/test/data.parquet"

        # Get the entry
        result = manifest.get_file_entry(full_path=full_path, data_dir=data_dir)

        # Verify it's the same entry
        assert result == entry
        assert result.sha256 == "abc123def456"
        assert result.url == "https://example.com/file.parquet"

    def test_failure(self, tmp_path):
        """Verify that we raise KeyError when the entry does not exist."""
        # Create an empty manifest
        manifest = Manifest(v=0, files={})

        # Set up paths
        data_dir = tmp_path / "data"
        full_path = data_dir / "cache/v1/2024-01-01/2024-01-31/test/data.parquet"

        # Should raise KeyError with the relative path in the message
        expected_key = "cache/v1/2024-01-01/2024-01-31/test/data.parquet"
        with pytest.raises(KeyError, match=f"no remotely-cached file for {expected_key}"):
            manifest.get_file_entry(full_path=full_path, data_dir=data_dir)


class TestIQBGitHubRemoteCacheSync:
    """Tests for the IQBGitHubRemoteCache.sync method."""

    def _create_mock_entry(self, tmp_path):
        """Helper to create a mock PipelineCacheEntry."""
        entry = Mock()
        entry.data_dir = tmp_path
        entry.data_parquet_file_path.return_value = tmp_path / "data.parquet"
        entry.stats_json_file_path.return_value = tmp_path / "stats.json"
        return entry

    def _mock_urlopen_with_content(self, json_content, parquet_content):
        """Helper to create urlopen mock that returns specified content."""

        def mock_urlopen(url):
            # Determine which content to return based on URL
            content = json_content if "stats.json" in url else parquet_content

            # Create mock response with explicit context manager support
            response = Mock()
            response.read = Mock(side_effect=[content, b""])
            response.headers = Mock()
            response.headers.get.return_value = str(len(content))
            response.__enter__ = Mock(return_value=response)
            response.__exit__ = Mock(return_value=False)
            return response

        return mock_urlopen

    def test_missing_parquet_entry(self, tmp_path, caplog):
        """Make sure we return False if there's no remote parquet entry."""
        entry = self._create_mock_entry(tmp_path)

        # Manifest only has JSON, not parquet
        _write_manifest(
            tmp_path,
            {
                "v": 0,
                "files": {
                    "stats.json": {
                        "sha256": "abc123",
                        "url": "https://example.com/stats.json",
                    }
                },
            },
        )
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)

        with caplog.at_level(logging.WARNING):
            result = cache.sync(entry)

        assert result is False
        assert "failure" in caplog.text
        assert "no remotely-cached file for data.parquet" in caplog.text

    def test_missing_json_entry(self, tmp_path, caplog):
        """Make sure we return False if there's no remote JSON entry."""
        entry = self._create_mock_entry(tmp_path)

        # Manifest only has parquet, not JSON
        _write_manifest(
            tmp_path,
            {
                "v": 0,
                "files": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "url": "https://example.com/data.parquet",
                    }
                },
            },
        )
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)

        with caplog.at_level(logging.WARNING):
            result = cache.sync(entry)

        assert result is False
        assert "failure" in caplog.text
        assert "no remotely-cached file for stats.json" in caplog.text

    def test_download_if_not_exists(self, tmp_path):
        """Ensure that we download the file if it does not exist."""
        json_content = b'{"test": "data"}'
        parquet_content = b"PARQUET_DATA"
        json_sha256 = _compute_test_sha256(json_content)
        parquet_sha256 = _compute_test_sha256(parquet_content)

        entry = self._create_mock_entry(tmp_path)

        _write_manifest(
            tmp_path,
            {
                "v": 0,
                "files": {
                    "data.parquet": {
                        "sha256": parquet_sha256,
                        "url": "https://example.com/data.parquet",
                    },
                    "stats.json": {
                        "sha256": json_sha256,
                        "url": "https://example.com/stats.json",
                    },
                },
            },
        )
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)

        mock_urlopen = self._mock_urlopen_with_content(json_content, parquet_content)

        with patch("iqb.ghremote.cache.urlopen", side_effect=mock_urlopen):
            result = cache.sync(entry)

        # Verify success
        assert result is True
        assert entry.stats_json_file_path().exists()
        assert entry.data_parquet_file_path().exists()
        assert entry.stats_json_file_path().read_bytes() == json_content
        assert entry.data_parquet_file_path().read_bytes() == parquet_content

    def test_no_download_if_exists_and_correct_sha256(self, tmp_path):
        """Ensure that we do not re-download a file we already downloaded."""
        json_content = b'{"test": "data"}'
        parquet_content = b"PARQUET_DATA"
        json_sha256 = _compute_test_sha256(json_content)
        parquet_sha256 = _compute_test_sha256(parquet_content)

        entry = self._create_mock_entry(tmp_path)

        # Pre-create files with correct content
        entry.stats_json_file_path().write_bytes(json_content)
        entry.data_parquet_file_path().write_bytes(parquet_content)

        _write_manifest(
            tmp_path,
            {
                "v": 0,
                "files": {
                    "data.parquet": {
                        "sha256": parquet_sha256,
                        "url": "https://example.com/data.parquet",
                    },
                    "stats.json": {
                        "sha256": json_sha256,
                        "url": "https://example.com/stats.json",
                    },
                },
            },
        )
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)

        # Mock urlopen - it should NOT be called
        mock_urlopen = Mock(side_effect=AssertionError("Should not download!"))

        with patch("iqb.ghremote.cache.urlopen", side_effect=mock_urlopen):
            result = cache.sync(entry)

        # Verify success without downloading
        assert result is True
        assert entry.stats_json_file_path().read_bytes() == json_content
        assert entry.data_parquet_file_path().read_bytes() == parquet_content

    def test_download_if_sha256_mismatch(self, tmp_path):
        """Ensure that we download on sha256 mismatch."""
        json_content = b'{"test": "data"}'
        parquet_content = b"PARQUET_DATA"
        json_sha256 = _compute_test_sha256(json_content)
        parquet_sha256 = _compute_test_sha256(parquet_content)

        entry = self._create_mock_entry(tmp_path)

        # Pre-create files with WRONG content
        entry.stats_json_file_path().write_bytes(b"wrong content")
        entry.data_parquet_file_path().write_bytes(b"wrong parquet")

        _write_manifest(
            tmp_path,
            {
                "v": 0,
                "files": {
                    "data.parquet": {
                        "sha256": parquet_sha256,
                        "url": "https://example.com/data.parquet",
                    },
                    "stats.json": {
                        "sha256": json_sha256,
                        "url": "https://example.com/stats.json",
                    },
                },
            },
        )
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)

        mock_urlopen = self._mock_urlopen_with_content(json_content, parquet_content)

        with patch("iqb.ghremote.cache.urlopen", side_effect=mock_urlopen):
            result = cache.sync(entry)

        # Verify files were re-downloaded with correct content
        assert result is True
        assert entry.stats_json_file_path().read_bytes() == json_content
        assert entry.data_parquet_file_path().read_bytes() == parquet_content

    def test_sync_success_both_files(self, tmp_path, caplog):
        """Ensure that both JSON and parquet files are synced successfully."""
        json_content = b'{"test": "data"}'
        parquet_content = b"PARQUET_DATA"
        json_sha256 = _compute_test_sha256(json_content)
        parquet_sha256 = _compute_test_sha256(parquet_content)

        entry = self._create_mock_entry(tmp_path)

        _write_manifest(
            tmp_path,
            {
                "v": 0,
                "files": {
                    "data.parquet": {
                        "sha256": parquet_sha256,
                        "url": "https://example.com/data.parquet",
                    },
                    "stats.json": {
                        "sha256": json_sha256,
                        "url": "https://example.com/stats.json",
                    },
                },
            },
        )
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)

        mock_urlopen = self._mock_urlopen_with_content(json_content, parquet_content)

        with (
            patch("iqb.ghremote.cache.urlopen", side_effect=mock_urlopen),
            caplog.at_level(logging.INFO),
        ):
            result = cache.sync(entry)

        # Verify success
        assert result is True
        assert entry.stats_json_file_path().exists()
        assert entry.data_parquet_file_path().exists()
        # Verify logging shows both files were fetched
        assert "fetching" in caplog.text
        assert "validating" in caplog.text
        assert "syncing" in caplog.text

    def test_download_failure(self, tmp_path, caplog):
        """Ensure that we return False when download fails (network error, 404, etc)."""
        entry = self._create_mock_entry(tmp_path)

        _write_manifest(
            tmp_path,
            {
                "v": 0,
                "files": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "url": "https://example.com/data.parquet",
                    },
                    "stats.json": {
                        "sha256": "def456",
                        "url": "https://example.com/stats.json",
                    },
                },
            },
        )
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)

        # Mock urlopen to raise URLError
        with (
            patch("iqb.ghremote.cache.urlopen", side_effect=URLError("Network error")),
            caplog.at_level(logging.WARNING),
        ):
            result = cache.sync(entry)

        assert result is False
        assert "failure" in caplog.text

    def test_sha256_mismatch_after_download(self, tmp_path, caplog):
        """Ensure that we return False and delete file when SHA256 validation fails."""
        json_content = b'{"test": "data"}'
        parquet_content = b"PARQUET_DATA"
        # Use WRONG sha256 in manifest
        wrong_sha256 = "0" * 64

        entry = self._create_mock_entry(tmp_path)

        _write_manifest(
            tmp_path,
            {
                "v": 0,
                "files": {
                    "data.parquet": {
                        "sha256": wrong_sha256,
                        "url": "https://example.com/data.parquet",
                    },
                    "stats.json": {
                        "sha256": wrong_sha256,
                        "url": "https://example.com/stats.json",
                    },
                },
            },
        )
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)

        mock_urlopen = self._mock_urlopen_with_content(json_content, parquet_content)

        with (
            patch("iqb.ghremote.cache.urlopen", side_effect=mock_urlopen),
            caplog.at_level(logging.WARNING),
        ):
            result = cache.sync(entry)

        # Verify failure and files are deleted
        assert result is False
        assert "SHA256 mismatch" in caplog.text
        # Files should be deleted after failed validation
        assert not entry.stats_json_file_path().exists()

    def test_creates_parent_directories(self, tmp_path):
        """Ensure that parent directories are created when they don't exist."""
        json_content = b'{"test": "data"}'
        parquet_content = b"PARQUET_DATA"
        json_sha256 = _compute_test_sha256(json_content)
        parquet_sha256 = _compute_test_sha256(parquet_content)

        # Create entry with nested paths that don't exist yet
        entry = Mock()
        entry.data_dir = tmp_path
        entry.data_parquet_file_path.return_value = tmp_path / "nested" / "dir" / "data.parquet"
        entry.stats_json_file_path.return_value = tmp_path / "nested" / "dir" / "stats.json"

        _write_manifest(
            tmp_path,
            {
                "v": 0,
                "files": {
                    "nested/dir/data.parquet": {
                        "sha256": parquet_sha256,
                        "url": "https://example.com/data.parquet",
                    },
                    "nested/dir/stats.json": {
                        "sha256": json_sha256,
                        "url": "https://example.com/stats.json",
                    },
                },
            },
        )
        cache = IQBGitHubRemoteCache(data_dir=tmp_path)

        mock_urlopen = self._mock_urlopen_with_content(json_content, parquet_content)

        # Verify parent directories don't exist yet
        assert not entry.data_parquet_file_path().parent.exists()

        with patch("iqb.ghremote.cache.urlopen", side_effect=mock_urlopen):
            result = cache.sync(entry)

        # Verify success and directories were created
        assert result is True
        assert entry.stats_json_file_path().exists()
        assert entry.data_parquet_file_path().exists()
        assert entry.data_parquet_file_path().parent.exists()

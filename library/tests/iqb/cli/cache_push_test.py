"""Tests for the iqb.cli.cache_push module."""

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from iqb.cli import cli


def _sha256(content: bytes) -> str:
    """Compute SHA256 hex digest for test data."""
    return hashlib.sha256(content).hexdigest()


def _write_manifest(data_dir: Path, files: dict[str, dict[str, str]]) -> None:
    """Write a manifest.json under the standard state directory."""
    manifest_path = data_dir / "state" / "ghremote" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"v": 0, "files": files}))


def _read_manifest(data_dir: Path) -> dict:
    """Read and parse the manifest.json from the standard state directory."""
    manifest_path = data_dir / "state" / "ghremote" / "manifest.json"
    return json.loads(manifest_path.read_text())


def _make_cache_file(data_dir: Path, rel_path: str, content: bytes) -> Path:
    """Create a file under data_dir at the given relative path."""
    full = data_dir / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(content)
    return full


_TS1 = "20241001T000000Z"
_TS2 = "20241101T000000Z"
_FILE_A = f"cache/v1/{_TS1}/{_TS2}/downloads/data.parquet"
_FILE_B = f"cache/v1/{_TS1}/{_TS2}/uploads/data.parquet"
_BUCKET = "mlab-sandbox-iqb-us-central1"


def _drain_reader(reader, **_kwargs) -> None:
    """Read all data from the reader, simulating what GCS does internally."""
    while reader.read(8192):
        pass


def _mock_storage_client() -> MagicMock:
    """Create a mock storage.Client whose bucket returns mock blobs."""
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value.upload_from_file.side_effect = _drain_reader
    return mock_client


class TestCachePushEmptyManifestNoLocalFiles:
    """No local-only files produces 'Nothing to upload.'."""

    def test_nothing_to_upload(self, tmp_path: Path):
        _write_manifest(tmp_path, {})
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "push", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "Nothing to upload" in result.output


class TestCachePushOnlyLocal:
    """ONLY_LOCAL entries are uploaded and manifest is updated."""

    @patch("iqb.cli.cache_push.storage.Client")
    def test_uploads_only_local(self, mock_client_cls: MagicMock, tmp_path: Path):
        content = b"local file content"
        sha = _sha256(content)
        _write_manifest(tmp_path, {})
        _make_cache_file(tmp_path, _FILE_A, content)

        mock_client = _mock_storage_client()
        mock_client_cls.return_value = mock_client

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "push", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "Uploaded 1/1" in result.output

        # Blob.upload_from_file was called
        mock_bucket = mock_client.bucket.return_value
        mock_bucket.blob.assert_called_once_with(_FILE_A)
        blob = mock_bucket.blob.return_value
        assert blob.upload_from_file.call_count == 1

        # Manifest should now contain the uploaded file
        manifest = _read_manifest(tmp_path)
        assert _FILE_A in manifest["files"]
        assert manifest["files"][_FILE_A]["sha256"] == sha
        expected_url = f"https://storage.googleapis.com/{_BUCKET}/{_FILE_A}"
        assert manifest["files"][_FILE_A]["url"] == expected_url


class TestCachePushMatchingSkipped:
    """MATCHING entries are not uploaded."""

    @patch("iqb.cli.cache_push.storage.Client")
    def test_skips_matching(self, mock_client_cls: MagicMock, tmp_path: Path):
        content = b"same content"
        sha = _sha256(content)
        _write_manifest(
            tmp_path,
            {_FILE_A: {"sha256": sha, "url": "https://example.com/a"}},
        )
        _make_cache_file(tmp_path, _FILE_A, content)

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "push", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "Nothing to upload" in result.output
        mock_client_cls.assert_not_called()


class TestCachePushMismatchSkippedByDefault:
    """SHA256_MISMATCH entries are skipped without --force."""

    @patch("iqb.cli.cache_push.storage.Client")
    def test_skips_mismatch(self, mock_client_cls: MagicMock, tmp_path: Path):
        remote_content = b"remote version"
        local_content = b"local version"
        _write_manifest(
            tmp_path,
            {_FILE_A: {"sha256": _sha256(remote_content), "url": "https://example.com/a"}},
        )
        _make_cache_file(tmp_path, _FILE_A, local_content)

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "push", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "Nothing to upload" in result.output
        mock_client_cls.assert_not_called()

        # Manifest should be unchanged
        manifest = _read_manifest(tmp_path)
        assert manifest["files"][_FILE_A]["sha256"] == _sha256(remote_content)


class TestCachePushMismatchWithForce:
    """SHA256_MISMATCH entries are uploaded with --force."""

    @patch("iqb.cli.cache_push.storage.Client")
    def test_uploads_with_force(self, mock_client_cls: MagicMock, tmp_path: Path):
        remote_content = b"remote version"
        local_content = b"local version"
        _write_manifest(
            tmp_path,
            {_FILE_A: {"sha256": _sha256(remote_content), "url": "https://example.com/a"}},
        )
        _make_cache_file(tmp_path, _FILE_A, local_content)

        mock_client = _mock_storage_client()
        mock_client_cls.return_value = mock_client

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "push", "-d", str(tmp_path), "--force"])
        assert result.exit_code == 0
        assert "Uploaded 1/1" in result.output

        # Manifest should be updated with local hash
        manifest = _read_manifest(tmp_path)
        assert manifest["files"][_FILE_A]["sha256"] == _sha256(local_content)


class TestCachePushUploadFailure:
    """Upload failure results in exit code 1."""

    @patch("iqb.cli.cache_push.storage.Client")
    def test_upload_failure(self, mock_client_cls: MagicMock, tmp_path: Path):
        content = b"local content"
        _write_manifest(tmp_path, {})
        _make_cache_file(tmp_path, _FILE_A, content)

        mock_client = _mock_storage_client()
        mock_client_cls.return_value = mock_client
        mock_blob = mock_client.bucket.return_value.blob.return_value
        mock_blob.upload_from_file.side_effect = Exception("permission denied")

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "push", "-d", str(tmp_path)])
        assert result.exit_code == 1
        assert "upload(s) failed" in result.output

        # Manifest should NOT contain the failed file
        manifest = _read_manifest(tmp_path)
        assert _FILE_A not in manifest["files"]


class TestCachePushMultipleFiles:
    """Multiple ONLY_LOCAL entries are all uploaded."""

    @patch("iqb.cli.cache_push.storage.Client")
    def test_uploads_multiple(self, mock_client_cls: MagicMock, tmp_path: Path):
        content_a = b"content a"
        content_b = b"content b"
        _write_manifest(tmp_path, {})
        _make_cache_file(tmp_path, _FILE_A, content_a)
        _make_cache_file(tmp_path, _FILE_B, content_b)

        mock_client = _mock_storage_client()
        mock_client_cls.return_value = mock_client

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "push", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "Uploaded 2/2" in result.output

        # Both files should be in the manifest
        manifest = _read_manifest(tmp_path)
        assert _FILE_A in manifest["files"]
        assert _FILE_B in manifest["files"]
        assert manifest["files"][_FILE_A]["sha256"] == _sha256(content_a)
        assert manifest["files"][_FILE_B]["sha256"] == _sha256(content_b)


class TestCachePushManifestCrashSafety:
    """Manifest is saved after each successful upload, not just at the end."""

    @patch("iqb.cli.cache_push.storage.Client")
    def test_manifest_updated_incrementally(self, mock_client_cls: MagicMock, tmp_path: Path):
        content_a = b"content a"
        content_b = b"content b"
        _write_manifest(tmp_path, {})
        _make_cache_file(tmp_path, _FILE_A, content_a)
        _make_cache_file(tmp_path, _FILE_B, content_b)

        mock_client = _mock_storage_client()
        mock_client_cls.return_value = mock_client
        mock_blob = mock_client.bucket.return_value.blob.return_value

        # First upload succeeds (drains reader), second fails
        call_count = 0

        def _succeed_then_fail(reader, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                _drain_reader(reader, **kwargs)
            else:
                raise Exception("network error")

        mock_blob.upload_from_file.side_effect = _succeed_then_fail

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "push", "-d", str(tmp_path)])
        assert result.exit_code == 1

        # The first file should still be in the manifest (crash-safe)
        manifest = _read_manifest(tmp_path)
        # One of the two files should have been recorded
        recorded = [f for f in [_FILE_A, _FILE_B] if f in manifest["files"]]
        assert len(recorded) == 1


class TestCachePushCustomBucket:
    """Custom --bucket flag is used in manifest URLs."""

    @patch("iqb.cli.cache_push.storage.Client")
    def test_custom_bucket(self, mock_client_cls: MagicMock, tmp_path: Path):
        content = b"local content"
        custom_bucket = "my-custom-bucket"
        _write_manifest(tmp_path, {})
        _make_cache_file(tmp_path, _FILE_A, content)

        mock_client = _mock_storage_client()
        mock_client_cls.return_value = mock_client

        runner = CliRunner()
        result = runner.invoke(
            cli, ["cache", "push", "-d", str(tmp_path), "--bucket", custom_bucket]
        )
        assert result.exit_code == 0

        # client.bucket was called with the custom bucket name
        mock_client.bucket.assert_called_once_with(custom_bucket)

        # URL in manifest should use the custom bucket
        manifest = _read_manifest(tmp_path)
        expected_url = f"https://storage.googleapis.com/{custom_bucket}/{_FILE_A}"
        assert manifest["files"][_FILE_A]["url"] == expected_url

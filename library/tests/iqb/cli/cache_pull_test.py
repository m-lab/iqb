"""Tests for the iqb.cli.cache_pull module."""

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


def _fake_response(content: bytes) -> MagicMock:
    """Create a mock response that yields content in chunks."""
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.headers = {"Content-Length": str(len(content))}
    resp.iter_content = MagicMock(return_value=iter([content]))
    return resp


class TestCachePullEmptyManifest:
    """Empty manifest produces 'Nothing to download.'."""

    def test_nothing_to_download(self, tmp_path: Path):
        _write_manifest(tmp_path, {})
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "Nothing to download" in result.output


class TestCachePullOnlyRemote:
    """ONLY_REMOTE entries are downloaded."""

    @patch("iqb.cli.cache_pull.requests.Session")
    def test_downloads_only_remote(self, mock_session_cls: MagicMock, tmp_path: Path):
        content = b"remote file content"
        sha = _sha256(content)
        url = "https://example.com/a"
        _write_manifest(tmp_path, {_FILE_A: {"sha256": sha, "url": url}})

        mock_session = MagicMock()
        mock_session.get.return_value = _fake_response(content)
        mock_session_cls.return_value = mock_session

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path)])
        assert result.exit_code == 0

        # File should exist on disk with correct content
        dest = tmp_path / _FILE_A
        assert dest.exists()
        assert dest.read_bytes() == content

        # Session.get was called with the right URL
        mock_session.get.assert_called_once_with(url, stream=True)


class TestCachePullMatchingSkipped:
    """MATCHING entries are not downloaded."""

    @patch("iqb.cli.cache_pull.requests.Session")
    def test_skips_matching(self, mock_session_cls: MagicMock, tmp_path: Path):
        content = b"same content"
        sha = _sha256(content)
        _write_manifest(tmp_path, {_FILE_A: {"sha256": sha, "url": "https://example.com/a"}})
        _make_cache_file(tmp_path, _FILE_A, content)

        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "Nothing to download" in result.output
        mock_session.get.assert_not_called()


class TestCachePullMismatchSkippedByDefault:
    """SHA256_MISMATCH entries are skipped without --force."""

    @patch("iqb.cli.cache_pull.requests.Session")
    def test_skips_mismatch(self, mock_session_cls: MagicMock, tmp_path: Path):
        remote_content = b"remote version"
        local_content = b"local version"
        _write_manifest(
            tmp_path,
            {_FILE_A: {"sha256": _sha256(remote_content), "url": "https://example.com/a"}},
        )
        _make_cache_file(tmp_path, _FILE_A, local_content)

        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "Nothing to download" in result.output
        mock_session.get.assert_not_called()

        # Local file should be unchanged
        assert (tmp_path / _FILE_A).read_bytes() == local_content


class TestCachePullMismatchWithForce:
    """SHA256_MISMATCH entries are downloaded with --force."""

    @patch("iqb.cli.cache_pull.requests.Session")
    def test_downloads_with_force(self, mock_session_cls: MagicMock, tmp_path: Path):
        remote_content = b"remote version"
        local_content = b"local version"
        _write_manifest(
            tmp_path,
            {_FILE_A: {"sha256": _sha256(remote_content), "url": "https://example.com/a"}},
        )
        _make_cache_file(tmp_path, _FILE_A, local_content)

        mock_session = MagicMock()
        mock_session.get.return_value = _fake_response(remote_content)
        mock_session_cls.return_value = mock_session

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path), "--force"])
        assert result.exit_code == 0

        # File should be replaced with remote content
        assert (tmp_path / _FILE_A).read_bytes() == remote_content


class TestCachePullSha256Failure:
    """SHA256 verification failure results in exit code 1."""

    @patch("iqb.cli.cache_pull.requests.Session")
    def test_sha256_failure(self, mock_session_cls: MagicMock, tmp_path: Path):
        expected_content = b"expected content"
        actual_content = b"corrupted content"
        _write_manifest(
            tmp_path,
            {_FILE_A: {"sha256": _sha256(expected_content), "url": "https://example.com/a"}},
        )

        mock_session = MagicMock()
        mock_session.get.return_value = _fake_response(actual_content)
        mock_session_cls.return_value = mock_session

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path)])
        assert result.exit_code == 1
        assert "download(s) failed" in result.output

        # File should NOT exist on disk (atomic write prevented partial file)
        assert not (tmp_path / _FILE_A).exists()


class TestCachePullMultipleFiles:
    """Multiple ONLY_REMOTE entries are all downloaded."""

    @patch("iqb.cli.cache_pull.requests.Session")
    def test_downloads_multiple(self, mock_session_cls: MagicMock, tmp_path: Path):
        content_a = b"content a"
        content_b = b"content b"
        _write_manifest(
            tmp_path,
            {
                _FILE_A: {"sha256": _sha256(content_a), "url": "https://example.com/a"},
                _FILE_B: {"sha256": _sha256(content_b), "url": "https://example.com/b"},
            },
        )

        mock_session = MagicMock()

        def side_effect(url: str, **kwargs):
            if url == "https://example.com/a":
                return _fake_response(content_a)
            return _fake_response(content_b)

        mock_session.get.side_effect = side_effect
        mock_session_cls.return_value = mock_session

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert (tmp_path / _FILE_A).read_bytes() == content_a
        assert (tmp_path / _FILE_B).read_bytes() == content_b


class TestCachePullJobsFlag:
    """-j flag is accepted."""

    def test_jobs_flag_accepted(self, tmp_path: Path):
        _write_manifest(tmp_path, {})
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path), "-j", "4"])
        assert result.exit_code == 0
        assert "Nothing to download" in result.output


class TestCachePullAtomicWrite:
    """If download fails, no partial file is left on disk."""

    @patch("iqb.cli.cache_pull.requests.Session")
    def test_no_partial_file_on_failure(self, mock_session_cls: MagicMock, tmp_path: Path):
        content = b"some content"
        _write_manifest(
            tmp_path,
            {_FILE_A: {"sha256": _sha256(content), "url": "https://example.com/a"}},
        )

        mock_session = MagicMock()
        mock_session.get.side_effect = ConnectionError("connection refused")
        mock_session_cls.return_value = mock_session

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path)])
        assert result.exit_code == 1

        # No partial file should exist
        assert not (tmp_path / _FILE_A).exists()

"""Tests for the iqb.cli.cache_pull module."""

import hashlib
import json
from datetime import datetime
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


def _find_log_file(data_dir: Path) -> Path | None:
    """Find the single JSONL log file under state/logs/."""
    log_dir = data_dir / "state" / "logs"
    if not log_dir.exists():
        return None
    files = list(log_dir.glob("*_pull.jsonl"))
    return files[0] if len(files) == 1 else None


def _read_spans(log_file: Path) -> list[dict]:
    """Read all spans from a JSONL log file."""
    spans = []
    for line in log_file.read_text().splitlines():
        if line.strip():
            spans.append(json.loads(line))
    return spans


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


class TestCachePullInvalidManifestKeys:
    """Invalid manifest keys are ignored for safety."""

    @patch("iqb.cli.cache_pull.requests.Session")
    def test_traversal_key_is_ignored(self, mock_session_cls: MagicMock, tmp_path: Path):
        _write_manifest(
            tmp_path,
            {
                "../../etc/passwd": {
                    "sha256": _sha256(b"malicious"),
                    "url": "https://example.com/a",
                }
            },
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path)])

        assert result.exit_code == 0
        assert "Nothing to download" in result.output
        mock_session_cls.assert_not_called()


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

        # Metrics log should exist with one span
        log_file = _find_log_file(tmp_path)
        assert log_file is not None
        spans = _read_spans(log_file)
        assert len(spans) == 1
        assert spans[0]["ok"] is True
        assert spans[0]["file"] == _FILE_A
        assert spans[0]["bytes"] == len(content)


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

        # Metrics log should record the failure
        log_file = _find_log_file(tmp_path)
        assert log_file is not None
        spans = _read_spans(log_file)
        assert len(spans) == 1
        assert spans[0]["ok"] is False
        assert spans[0]["error"] is not None
        assert "SHA256 mismatch" in spans[0]["error"]


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

        # Metrics log should record the connection failure
        log_file = _find_log_file(tmp_path)
        assert log_file is not None
        spans = _read_spans(log_file)
        assert len(spans) == 1
        assert spans[0]["ok"] is False
        assert "connection refused" in spans[0]["error"]


class TestCachePullMetrics:
    """Download metrics JSONL file is created with correct schema."""

    _EXPECTED_KEYS = {
        "t0",
        "t",
        "worker_id",
        "file",
        "url",
        "content_length",
        "bytes",
        "ok",
        "error",
    }

    @patch("iqb.cli.cache_pull.requests.Session")
    def test_two_files_produce_two_spans(self, mock_session_cls: MagicMock, tmp_path: Path):
        content_a = b"content a"
        content_b = b"content b"
        url_a = "https://example.com/a"
        url_b = "https://example.com/b"
        _write_manifest(
            tmp_path,
            {
                _FILE_A: {"sha256": _sha256(content_a), "url": url_a},
                _FILE_B: {"sha256": _sha256(content_b), "url": url_b},
            },
        )

        mock_session = MagicMock()

        def side_effect(url: str, **kwargs):
            if url == url_a:
                return _fake_response(content_a)
            return _fake_response(content_b)

        mock_session.get.side_effect = side_effect
        mock_session_cls.return_value = mock_session

        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "pull", "-d", str(tmp_path)])
        assert result.exit_code == 0

        log_file = _find_log_file(tmp_path)
        assert log_file is not None
        assert log_file.name.endswith("_pull.jsonl")
        spans = _read_spans(log_file)
        assert len(spans) == 2

        for span in spans:
            # All expected keys are present
            assert set(span.keys()) == self._EXPECTED_KEYS

            # ok is True
            assert span["ok"] is True
            assert span["error"] is None

            # worker_id is an integer
            assert isinstance(span["worker_id"], int)

            # Timestamps are parseable and t0 <= t
            t0 = datetime.strptime(span["t0"], "%Y-%m-%d %H:%M:%S %z")
            t = datetime.strptime(span["t"], "%Y-%m-%d %H:%M:%S %z")
            assert t0 <= t

            # URL is present
            assert span["url"] in (url_a, url_b)

        # Check per-file details
        spans_by_file = {s["file"]: s for s in spans}
        assert spans_by_file[_FILE_A]["bytes"] == len(content_a)
        assert spans_by_file[_FILE_A]["content_length"] == len(content_a)
        assert spans_by_file[_FILE_B]["bytes"] == len(content_b)
        assert spans_by_file[_FILE_B]["content_length"] == len(content_b)

"""Tests for the iqb.scripting.iqb_cache_pull module."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from iqb.ghremote.cache import Manifest, load_manifest_from_dict
from iqb.scripting import iqb_cache_pull

_TS1 = "20241001T000000Z"
_TS2 = "20241101T000000Z"
_TS3 = "20230601T000000Z"
_TS4 = "20230701T000000Z"
_FILE_A = f"cache/v1/{_TS1}/{_TS2}/downloads/data.parquet"
_FILE_B = f"cache/v1/{_TS1}/{_TS2}/uploads/data.parquet"
_FILE_C = f"cache/v1/{_TS1}/{_TS2}/downloads/stats.json"
_FILE_D = f"cache/v1/{_TS3}/{_TS4}/downloads/data.parquet"


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _write_manifest(data_dir: Path, files: dict[str, dict[str, str]]) -> None:
    manifest_path = data_dir / "state" / "ghremote" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"v": 0, "files": files}))


def _make_manifest(files: dict[str, dict[str, str]]) -> Manifest:
    return load_manifest_from_dict({"v": 0, "files": files})


def _fake_response(content: bytes) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.headers = {"Content-Length": str(len(content))}
    resp.iter_content = MagicMock(return_value=iter([content]))
    return resp


class TestRunNothingToDownload:
    """Returns None when there is nothing to download."""

    def test_empty_manifest_from_disk(self, tmp_path: Path):
        _write_manifest(tmp_path, {})
        result = iqb_cache_pull.run(data_dir=tmp_path)
        assert result is None

    def test_empty_provided_manifest(self, tmp_path: Path):
        manifest = Manifest(v=0, files={})
        result = iqb_cache_pull.run(data_dir=tmp_path, manifest=manifest)
        assert result is None

    def test_all_matching(self, tmp_path: Path):
        content = b"matching"
        sha = _sha256(content)
        _write_manifest(tmp_path, {_FILE_A: {"sha256": sha, "url": "https://example.com/a"}})
        dest = tmp_path / _FILE_A
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)

        result = iqb_cache_pull.run(data_dir=tmp_path)
        assert result is None


class TestRunWithProvidedManifest:
    """Accepts a pre-loaded manifest instead of reading from disk."""

    @patch("iqb.scripting.iqb_cache_pull.requests.Session")
    def test_uses_provided_manifest(self, mock_session_cls: MagicMock, tmp_path: Path):
        content = b"remote content"
        sha = _sha256(content)
        manifest = _make_manifest({_FILE_A: {"sha256": sha, "url": "https://example.com/a"}})

        mock_session = MagicMock()
        mock_session.get.return_value = _fake_response(content)
        mock_session_cls.return_value = mock_session

        # No manifest on disk — uses provided manifest directly
        result = iqb_cache_pull.run(data_dir=tmp_path, manifest=manifest)

        assert result is not None
        assert result.total == 1
        assert result.ok == 1
        assert result.failed == []
        assert (tmp_path / _FILE_A).read_bytes() == content


class TestRunFiltering:
    """Manifest filtering works with provided manifest."""

    @patch("iqb.scripting.iqb_cache_pull.requests.Session")
    def test_filter_by_dataset(self, mock_session_cls: MagicMock, tmp_path: Path):
        ca, cb = b"content_a", b"content_b"
        manifest = _make_manifest(
            {
                _FILE_A: {"sha256": _sha256(ca), "url": "https://example.com/a"},
                _FILE_B: {"sha256": _sha256(cb), "url": "https://example.com/b"},
            }
        )

        mock_session = MagicMock()
        mock_session.get.return_value = _fake_response(cb)
        mock_session_cls.return_value = mock_session

        result = iqb_cache_pull.run(data_dir=tmp_path, manifest=manifest, datasets=("uploads",))

        assert result is not None
        assert result.total == 1
        assert (tmp_path / _FILE_B).exists()
        assert not (tmp_path / _FILE_A).exists()

    @patch("iqb.scripting.iqb_cache_pull.requests.Session")
    def test_filter_by_after(self, mock_session_cls: MagicMock, tmp_path: Path):
        ca, cd = b"content_a", b"content_d"
        manifest = _make_manifest(
            {
                _FILE_A: {"sha256": _sha256(ca), "url": "https://example.com/a"},
                _FILE_D: {"sha256": _sha256(cd), "url": "https://example.com/d"},
            }
        )

        mock_session = MagicMock()
        mock_session.get.return_value = _fake_response(ca)
        mock_session_cls.return_value = mock_session

        result = iqb_cache_pull.run(data_dir=tmp_path, manifest=manifest, after="2024-01-01")

        assert result is not None
        assert result.total == 1
        assert (tmp_path / _FILE_A).exists()
        assert not (tmp_path / _FILE_D).exists()


class TestRunResult:
    """PullResult is populated correctly."""

    @patch("iqb.scripting.iqb_cache_pull.requests.Session")
    def test_result_fields(self, mock_session_cls: MagicMock, tmp_path: Path):
        content = b"data"
        sha = _sha256(content)
        manifest = _make_manifest({_FILE_A: {"sha256": sha, "url": "https://example.com/a"}})

        mock_session = MagicMock()
        mock_session.get.return_value = _fake_response(content)
        mock_session_cls.return_value = mock_session

        result = iqb_cache_pull.run(data_dir=tmp_path, manifest=manifest)

        assert result is not None
        assert result.total == 1
        assert result.ok == 1
        assert result.failed == []
        assert result.log_file.exists()
        assert result.elapsed >= 0

    @patch("iqb.scripting.iqb_cache_pull.requests.Session")
    def test_failure_recorded(self, mock_session_cls: MagicMock, tmp_path: Path):
        content = b"data"
        manifest = _make_manifest(
            {_FILE_A: {"sha256": _sha256(b"different"), "url": "https://example.com/a"}}
        )

        mock_session = MagicMock()
        mock_session.get.return_value = _fake_response(content)
        mock_session_cls.return_value = mock_session

        result = iqb_cache_pull.run(data_dir=tmp_path, manifest=manifest)

        assert result is not None
        assert result.total == 1
        assert result.ok == 0
        assert len(result.failed) == 1
        assert "SHA256 mismatch" in result.failed[0][1]

    @patch("iqb.scripting.iqb_cache_pull.requests.Session")
    def test_metrics_log_written(self, mock_session_cls: MagicMock, tmp_path: Path):
        content = b"data"
        sha = _sha256(content)
        manifest = _make_manifest({_FILE_A: {"sha256": sha, "url": "https://example.com/a"}})

        mock_session = MagicMock()
        mock_session.get.return_value = _fake_response(content)
        mock_session_cls.return_value = mock_session

        result = iqb_cache_pull.run(data_dir=tmp_path, manifest=manifest)

        assert result is not None
        spans = [json.loads(line) for line in result.log_file.read_text().splitlines() if line]
        assert len(spans) == 1
        assert spans[0]["ok"] is True
        assert spans[0]["file"] == _FILE_A


class TestRunForce:
    """The force flag re-downloads mismatched files."""

    @patch("iqb.scripting.iqb_cache_pull.requests.Session")
    def test_mismatch_skipped_without_force(self, mock_session_cls: MagicMock, tmp_path: Path):
        remote = b"remote"
        local = b"local"
        manifest = _make_manifest(
            {_FILE_A: {"sha256": _sha256(remote), "url": "https://example.com/a"}}
        )
        dest = tmp_path / _FILE_A
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(local)

        result = iqb_cache_pull.run(data_dir=tmp_path, manifest=manifest, force=False)
        assert result is None
        assert dest.read_bytes() == local

    @patch("iqb.scripting.iqb_cache_pull.requests.Session")
    def test_mismatch_downloaded_with_force(self, mock_session_cls: MagicMock, tmp_path: Path):
        remote = b"remote"
        local = b"local"
        manifest = _make_manifest(
            {_FILE_A: {"sha256": _sha256(remote), "url": "https://example.com/a"}}
        )
        dest = tmp_path / _FILE_A
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(local)

        mock_session = MagicMock()
        mock_session.get.return_value = _fake_response(remote)
        mock_session_cls.return_value = mock_session

        result = iqb_cache_pull.run(data_dir=tmp_path, manifest=manifest, force=True)
        assert result is not None
        assert result.ok == 1
        assert dest.read_bytes() == remote

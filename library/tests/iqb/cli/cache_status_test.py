"""Tests for the iqb.cli.cache_status module."""

import hashlib
import json
from pathlib import Path

from click.testing import CliRunner

from iqb.cli import cli


def _sha256(content: bytes) -> str:
    """Compute SHA256 hex digest for test data."""
    return hashlib.sha256(content).hexdigest()


def _make_cache_file(data_dir: Path, rel_path: str, content: bytes) -> Path:
    """Create a file under data_dir at the given relative path."""
    full = data_dir / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(content)
    return full


def _write_manifest(data_dir: Path, files: dict[str, dict[str, str]]) -> None:
    """Write a manifest.json under the standard state directory."""
    manifest_path = data_dir / "state" / "ghremote" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"v": 0, "files": files}))


_TS1 = "20241001T000000Z"
_TS2 = "20241101T000000Z"
_FILE_A = f"cache/v1/{_TS1}/{_TS2}/downloads/data.parquet"
_FILE_B = f"cache/v1/{_TS1}/{_TS2}/uploads/data.parquet"


class TestCacheStatusEmpty:
    """Empty manifest and no local files."""

    def test_no_output(self, tmp_path: Path):
        _write_manifest(tmp_path, {})
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "status", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert result.output.strip() == ""


class TestCacheStatusOnlyRemote:
    """Manifest entry exists but file is not on disk."""

    def test_prints_d(self, tmp_path: Path):
        content = b"remote content"
        _write_manifest(
            tmp_path,
            {
                _FILE_A: {"sha256": _sha256(content), "url": "https://example.com/a"},
            },
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "status", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "D" in result.output
        assert _FILE_A in result.output


class TestCacheStatusOnlyLocal:
    """File on disk but not in manifest."""

    def test_prints_a(self, tmp_path: Path):
        _write_manifest(tmp_path, {})
        _make_cache_file(tmp_path, _FILE_A, b"local content")
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "status", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "A" in result.output
        assert _FILE_A in result.output


class TestCacheStatusSha256Mismatch:
    """Both exist but hashes differ."""

    def test_prints_m(self, tmp_path: Path):
        remote_content = b"remote version"
        local_content = b"local version"
        _write_manifest(
            tmp_path,
            {
                _FILE_A: {"sha256": _sha256(remote_content), "url": "https://example.com/a"},
            },
        )
        _make_cache_file(tmp_path, _FILE_A, local_content)
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "status", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "M" in result.output
        assert _FILE_A in result.output


class TestCacheStatusMatching:
    """Matching files hidden by default, shown with --all."""

    def test_hidden_by_default(self, tmp_path: Path):
        content = b"same content"
        _write_manifest(
            tmp_path,
            {
                _FILE_A: {"sha256": _sha256(content), "url": "https://example.com/a"},
            },
        )
        _make_cache_file(tmp_path, _FILE_A, content)
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "status", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert _FILE_A not in result.output

    def test_shown_with_all(self, tmp_path: Path):
        content = b"same content"
        _write_manifest(
            tmp_path,
            {
                _FILE_A: {"sha256": _sha256(content), "url": "https://example.com/a"},
            },
        )
        _make_cache_file(tmp_path, _FILE_A, content)
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "status", "--all", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert _FILE_A in result.output

    def test_shown_with_short_a(self, tmp_path: Path):
        content = b"same content"
        _write_manifest(
            tmp_path,
            {
                _FILE_A: {"sha256": _sha256(content), "url": "https://example.com/a"},
            },
        )
        _make_cache_file(tmp_path, _FILE_A, content)
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "status", "-a", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert _FILE_A in result.output


class TestCacheStatusCustomDir:
    """Custom -d directory works."""

    def test_custom_dir(self, tmp_path: Path):
        custom_dir = tmp_path / "custom"
        custom_dir.mkdir()
        _write_manifest(
            custom_dir,
            {
                _FILE_A: {"sha256": "abc123", "url": "https://example.com/a"},
            },
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "status", "-d", str(custom_dir)])
        assert result.exit_code == 0
        assert "D" in result.output
        assert _FILE_A in result.output


class TestCacheStatusNoManifest:
    """Missing manifest file should produce empty output (not crash)."""

    def test_no_manifest(self, tmp_path: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "status", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert result.output.strip() == ""


class TestCacheStatusMultipleEntries:
    """Multiple entries with different states."""

    def test_mixed_states(self, tmp_path: Path):
        content_a = b"content a"
        content_b = b"content b"
        _write_manifest(
            tmp_path,
            {
                _FILE_A: {"sha256": "wrong_hash", "url": "https://example.com/a"},
                _FILE_B: {"sha256": _sha256(content_b), "url": "https://example.com/b"},
            },
        )
        _make_cache_file(tmp_path, _FILE_A, content_a)
        # _FILE_B not on disk → ONLY_REMOTE
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "status", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "M" in result.output
        assert "D" in result.output

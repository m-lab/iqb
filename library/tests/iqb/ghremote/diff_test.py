"""Tests for the iqb.ghremote.diff module."""

import hashlib
from collections.abc import Iterator
from pathlib import Path

from iqb.ghremote.cache import FileEntry, Manifest
from iqb.ghremote.diff import DiffEntry, DiffState, _validate_cache_path, diff


def _sha256(content: bytes) -> str:
    """Compute SHA256 hex digest for test data."""
    return hashlib.sha256(content).hexdigest()


def _make_cache_file(data_dir: Path, rel_path: str, content: bytes) -> Path:
    """Create a file under data_dir at the given relative path."""
    full = data_dir / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(content)
    return full


# Valid cache path components for test fixtures
_TS1 = "20241001T000000Z"
_TS2 = "20241031T235959Z"
_NAME = "downloads"
_FILE_A = f"cache/v1/{_TS1}/{_TS2}/{_NAME}/data.parquet"
_FILE_B = f"cache/v1/{_TS1}/{_TS2}/{_NAME}/stats.json"
_FILE_C = f"cache/v1/{_TS1}/{_TS2}/uploads/data.parquet"


class TestDiffOnlyRemote:
    """Manifest entry exists but file is not on disk."""

    def test_yields_only_remote(self, tmp_path: Path):
        content = b"remote content"
        entry = FileEntry(sha256=_sha256(content), url="https://example.com/a.parquet")
        manifest = Manifest(v=0, files={_FILE_A: entry})

        results = list(diff(manifest, tmp_path))

        assert len(results) == 1
        assert results[0] == DiffEntry(
            file=_FILE_A,
            url="https://example.com/a.parquet",
            remote_sha256=_sha256(content),
            local_sha256=None,
            state=DiffState.ONLY_REMOTE,
        )

    def test_url_is_populated(self, tmp_path: Path):
        entry = FileEntry(sha256="abc123", url="https://storage.example.com/file")
        manifest = Manifest(v=0, files={_FILE_A: entry})

        results = list(diff(manifest, tmp_path))

        assert results[0].url == "https://storage.example.com/file"
        assert results[0].remote_sha256 == "abc123"
        assert results[0].local_sha256 is None


class TestDiffOnlyLocal:
    """File exists locally with valid cache path but is not in manifest."""

    def test_yields_only_local(self, tmp_path: Path):
        content = b"local content"
        _make_cache_file(tmp_path, _FILE_A, content)
        manifest = Manifest(v=0, files={})

        results = list(diff(manifest, tmp_path))

        assert len(results) == 1
        assert results[0] == DiffEntry(
            file=_FILE_A,
            url=None,
            remote_sha256=None,
            local_sha256=_sha256(content),
            state=DiffState.ONLY_LOCAL,
        )

    def test_url_is_none(self, tmp_path: Path):
        _make_cache_file(tmp_path, _FILE_A, b"local content")
        manifest = Manifest(v=0, files={})

        results = list(diff(manifest, tmp_path))

        assert results[0].url is None

    def test_invalid_local_files_excluded(self, tmp_path: Path):
        """Files that don't match cache path format are excluded."""
        # Create a file with invalid cache path (e.g., a .lock file)
        _make_cache_file(tmp_path, f"cache/v1/{_TS1}/{_TS2}/{_NAME}/.lock", b"")
        # Also create a file outside the cache path structure
        _make_cache_file(tmp_path, "cache/v1/not-a-timestamp/file.txt", b"data")
        manifest = Manifest(v=0, files={})

        results = list(diff(manifest, tmp_path))

        assert len(results) == 0


class TestDiffSha256Mismatch:
    """Entry in manifest and file exists locally but hashes differ."""

    def test_yields_sha256_mismatch(self, tmp_path: Path):
        remote_content = b"remote version"
        local_content = b"local version"
        entry = FileEntry(sha256=_sha256(remote_content), url="https://example.com/a.parquet")
        manifest = Manifest(v=0, files={_FILE_A: entry})
        _make_cache_file(tmp_path, _FILE_A, local_content)

        results = list(diff(manifest, tmp_path))

        assert len(results) == 1
        assert results[0] == DiffEntry(
            file=_FILE_A,
            url="https://example.com/a.parquet",
            remote_sha256=_sha256(remote_content),
            local_sha256=_sha256(local_content),
            state=DiffState.SHA256_MISMATCH,
        )

    def test_url_is_populated(self, tmp_path: Path):
        entry = FileEntry(sha256="0" * 64, url="https://example.com/file")
        manifest = Manifest(v=0, files={_FILE_A: entry})
        _make_cache_file(tmp_path, _FILE_A, b"different")

        results = list(diff(manifest, tmp_path))

        assert results[0].url == "https://example.com/file"


class TestDiffMatching:
    """Entry in manifest matches the local file."""

    def test_yields_matching(self, tmp_path: Path):
        content = b"same content"
        entry = FileEntry(sha256=_sha256(content), url="https://example.com/a.parquet")
        manifest = Manifest(v=0, files={_FILE_A: entry})
        _make_cache_file(tmp_path, _FILE_A, content)

        results = list(diff(manifest, tmp_path))

        assert len(results) == 1
        assert results[0] == DiffEntry(
            file=_FILE_A,
            url="https://example.com/a.parquet",
            remote_sha256=_sha256(content),
            local_sha256=_sha256(content),
            state=DiffState.MATCHING,
        )


class TestDiffAcceptPredicate:
    """The acceptp predicate filters entries."""

    def test_excludes_manifest_entries(self, tmp_path: Path):
        entry_a = FileEntry(sha256="abc", url="https://example.com/a")
        entry_b = FileEntry(sha256="def", url="https://example.com/b")
        manifest = Manifest(v=0, files={_FILE_A: entry_a, _FILE_B: entry_b})

        # Only accept stats.json paths
        results = list(diff(manifest, tmp_path, acceptp=lambda p: p.endswith("stats.json")))

        assert len(results) == 1
        assert results[0].file == _FILE_B

    def test_excludes_local_entries(self, tmp_path: Path):
        _make_cache_file(tmp_path, _FILE_A, b"parquet data")
        _make_cache_file(tmp_path, _FILE_B, b"json data")
        manifest = Manifest(v=0, files={})

        # Only accept data.parquet paths
        results = list(diff(manifest, tmp_path, acceptp=lambda p: p.endswith("data.parquet")))

        assert len(results) == 1
        assert results[0].file == _FILE_A

    def test_applies_to_both_manifest_and_local(self, tmp_path: Path):
        content_a = b"content a"
        entry_a = FileEntry(sha256=_sha256(content_a), url="https://example.com/a")
        manifest = Manifest(v=0, files={_FILE_A: entry_a})
        _make_cache_file(tmp_path, _FILE_A, content_a)
        _make_cache_file(tmp_path, _FILE_B, b"local only json")

        # Reject everything
        results = list(diff(manifest, tmp_path, acceptp=lambda _: False))

        assert len(results) == 0


class TestDiffEmpty:
    """Empty manifest and no local files produces empty result."""

    def test_empty_result(self, tmp_path: Path):
        manifest = Manifest(v=0, files={})

        results = list(diff(manifest, tmp_path))

        assert results == []


class TestDiffOrdering:
    """Manifest entries come before local-only, both sorted by file."""

    def test_manifest_before_local_only(self, tmp_path: Path):
        # _FILE_C (uploads/data.parquet) is in manifest but not local
        entry = FileEntry(sha256="abc", url="https://example.com/c")
        manifest = Manifest(v=0, files={_FILE_C: entry})
        # _FILE_A (downloads/data.parquet) is local but not in manifest
        _make_cache_file(tmp_path, _FILE_A, b"local only")

        results = list(diff(manifest, tmp_path))

        assert len(results) == 2
        # Manifest entry first (ONLY_REMOTE), then local-only (ONLY_LOCAL)
        assert results[0].state == DiffState.ONLY_REMOTE
        assert results[0].file == _FILE_C
        assert results[1].state == DiffState.ONLY_LOCAL
        assert results[1].file == _FILE_A

    def test_sorted_within_phases(self, tmp_path: Path):
        # Two manifest entries in reverse alphabetical order
        entry_b = FileEntry(sha256="b", url="https://example.com/b")
        entry_a = FileEntry(sha256="a", url="https://example.com/a")
        manifest = Manifest(v=0, files={_FILE_B: entry_b, _FILE_A: entry_a})

        results = list(diff(manifest, tmp_path))

        assert len(results) == 2
        # Should be sorted: _FILE_A (downloads/data.parquet) before _FILE_B (downloads/stats.json)
        assert results[0].file == _FILE_A
        assert results[1].file == _FILE_B

    def test_sorted_local_only(self, tmp_path: Path):
        _make_cache_file(tmp_path, _FILE_C, b"c")
        _make_cache_file(tmp_path, _FILE_A, b"a")
        manifest = Manifest(v=0, files={})

        results = list(diff(manifest, tmp_path))

        assert len(results) == 2
        assert results[0].file == _FILE_A
        assert results[1].file == _FILE_C


class TestDiffIsIterator:
    """diff() returns an iterator, not a list."""

    def test_returns_iterator(self, tmp_path: Path):
        manifest = Manifest(v=0, files={})

        result = diff(manifest, tmp_path)

        assert isinstance(result, Iterator)
        # Should not be a list or tuple
        assert not isinstance(result, (list, tuple))


class TestDiffInvalidManifestKeys:
    """Invalid manifest keys are ignored for safety."""

    def test_traversal_manifest_key_is_ignored(self, tmp_path: Path):
        manifest = Manifest(
            v=0,
            files={
                "../../etc/passwd": FileEntry(
                    sha256="0" * 64,
                    url="https://example.com/passwd",
                )
            },
        )

        results = list(diff(manifest, tmp_path))

        assert results == []

    def test_dotdot_inside_cache_path_is_ignored(self, tmp_path: Path):
        manifest = Manifest(
            v=0,
            files={
                f"cache/v1/{_TS1}/{_TS2}/{_NAME}/../data.parquet": FileEntry(
                    sha256="0" * 64,
                    url="https://example.com/a",
                )
            },
        )

        results = list(diff(manifest, tmp_path))

        assert results == []

    def test_mixed_valid_and_invalid_manifest_keys(self, tmp_path: Path):
        content = b"remote content"
        manifest = Manifest(
            v=0,
            files={
                "../../etc/passwd": FileEntry(
                    sha256="0" * 64,
                    url="https://example.com/passwd",
                ),
                _FILE_A: FileEntry(
                    sha256=_sha256(content),
                    url="https://example.com/a.parquet",
                ),
            },
        )

        results = list(diff(manifest, tmp_path))

        assert len(results) == 1
        assert results[0].file == _FILE_A
        assert results[0].state == DiffState.ONLY_REMOTE


class TestValidateCachePath:
    """Tests for _validate_cache_path covering all branches."""

    _VALID = f"cache/v1/{_TS1}/{_TS2}/{_NAME}/data.parquet"

    def test_valid_data_parquet(self):
        assert _validate_cache_path(f"cache/v1/{_TS1}/{_TS2}/{_NAME}/data.parquet") is True

    def test_valid_stats_json(self):
        assert _validate_cache_path(f"cache/v1/{_TS1}/{_TS2}/{_NAME}/stats.json") is True

    def test_too_few_components(self):
        assert _validate_cache_path("cache/v1") is False

    def test_too_many_components(self):
        assert _validate_cache_path(f"cache/v1/{_TS1}/{_TS2}/{_NAME}/extra/data.parquet") is False

    def test_wrong_first_component(self):
        assert _validate_cache_path(f"notcache/v1/{_TS1}/{_TS2}/{_NAME}/data.parquet") is False

    def test_wrong_second_component(self):
        assert _validate_cache_path(f"cache/v2/{_TS1}/{_TS2}/{_NAME}/data.parquet") is False

    def test_invalid_first_timestamp(self):
        assert _validate_cache_path(f"cache/v1/not-a-ts/{_TS2}/{_NAME}/data.parquet") is False

    def test_invalid_second_timestamp(self):
        assert _validate_cache_path(f"cache/v1/{_TS1}/not-a-ts/{_NAME}/data.parquet") is False

    def test_name_with_uppercase(self):
        assert _validate_cache_path(f"cache/v1/{_TS1}/{_TS2}/Downloads/data.parquet") is False

    def test_name_with_hyphen(self):
        assert _validate_cache_path(f"cache/v1/{_TS1}/{_TS2}/my-name/data.parquet") is False

    def test_name_with_underscore(self):
        assert _validate_cache_path(f"cache/v1/{_TS1}/{_TS2}/my_name/data.parquet") is True

    def test_invalid_filename(self):
        assert _validate_cache_path(f"cache/v1/{_TS1}/{_TS2}/{_NAME}/other.txt") is False

    def test_lock_file(self):
        assert _validate_cache_path(f"cache/v1/{_TS1}/{_TS2}/{_NAME}/.lock") is False

    def test_empty_string(self):
        assert _validate_cache_path("") is False

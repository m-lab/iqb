"""Diff between a remote manifest and the local cache directory."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from .cache import FileEntry, Manifest, compute_sha256


class DiffState(str, Enum):
    """State of a diff entry comparing manifest vs local cache."""

    ONLY_REMOTE = "only_remote"
    ONLY_LOCAL = "only_local"
    SHA256_MISMATCH = "sha256_mismatch"
    MATCHING = "matching"


@dataclass(frozen=True, kw_only=True)
class DiffEntry:
    """Single entry in a manifest-vs-local diff."""

    file: str
    url: str | None
    remote_sha256: str | None
    local_sha256: str | None
    state: DiffState


def _validate_cache_path(path: str) -> bool:
    """
    Validate that a path follows the cache/v1 format.

    Valid format:
      cache/v1/{rfc3339_timestamp}/{rfc3339_timestamp}/{name}/{file}

    Where:
      - Component 1: "cache"
      - Component 2: "v1"
      - Component 3: RFC3339 timestamp (e.g., 20241001T000000Z)
      - Component 4: RFC3339 timestamp
      - Component 5: lowercase letters, numbers, and underscores [a-z0-9_]+
      - Component 6: "data.parquet" or "stats.json"
    """
    parts = path.split("/")
    if len(parts) != 6:
        return False
    if parts[0] != "cache":
        return False
    if parts[1] != "v1":
        return False
    rfc3339_pattern = re.compile(r"^\d{8}T\d{6}Z$")
    if not rfc3339_pattern.match(parts[2]):
        return False
    if not rfc3339_pattern.match(parts[3]):
        return False
    name_pattern = re.compile(r"^[a-z0-9_]+$")
    if not name_pattern.match(parts[4]):
        return False
    return parts[5] in ("data.parquet", "stats.json")


def _scan_local_files(data_dir: Path) -> set[str]:
    """Walk the local cache directory and return validated relative paths."""
    result: set[str] = set()
    cache_dir = data_dir / "cache" / "v1"
    if not cache_dir.exists():
        return result
    for path in cache_dir.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(data_dir).as_posix()
        if _validate_cache_path(rel):
            result.add(rel)
    return result


def diff(
    manifest: Manifest,
    data_dir: Path,
    *,
    acceptp: Callable[[str], bool] | None = None,
) -> Iterator[DiffEntry]:
    """
    Compare manifest entries against local cache state.

    Yields ``DiffEntry`` objects in three phases:

    1. Manifest keys present locally  (``MATCHING`` or ``SHA256_MISMATCH``)
       and manifest keys absent locally (``ONLY_REMOTE``), in sorted key order.
    2. Local-only files (``ONLY_LOCAL``, ``url=None``), in sorted order.

    Args:
        manifest: The loaded manifest to compare against.
        data_dir: Root directory where cache files live on disk.
        acceptp: Optional predicate applied to relative path strings.
                 ``None`` means accept everything.
    """
    # Phase 1: scan local files, applying acceptp
    local_files = _scan_local_files(data_dir)
    if acceptp is not None:
        local_files = {f for f in local_files if acceptp(f)}

    # Phase 2: iterate accepted manifest keys in sorted order
    seen: set[str] = set()
    for key in sorted(manifest.files):
        if acceptp is not None and not acceptp(key):
            continue
        seen.add(key)
        entry: FileEntry = manifest.files[key]
        local_path = data_dir / Path(key)
        if key not in local_files:
            yield DiffEntry(
                file=key,
                url=entry.url,
                remote_sha256=entry.sha256,
                local_sha256=None,
                state=DiffState.ONLY_REMOTE,
            )
        else:
            local_sha256 = compute_sha256(local_path)
            if local_sha256 == entry.sha256:
                yield DiffEntry(
                    file=key,
                    url=entry.url,
                    remote_sha256=entry.sha256,
                    local_sha256=local_sha256,
                    state=DiffState.MATCHING,
                )
            else:
                yield DiffEntry(
                    file=key,
                    url=entry.url,
                    remote_sha256=entry.sha256,
                    local_sha256=local_sha256,
                    state=DiffState.SHA256_MISMATCH,
                )

    # Phase 3: remaining local files not in manifest
    remaining = local_files - seen
    for key in sorted(remaining):
        local_sha256 = compute_sha256(data_dir / Path(key))
        yield DiffEntry(
            file=key,
            url=None,
            remote_sha256=None,
            local_sha256=local_sha256,
            state=DiffState.ONLY_LOCAL,
        )

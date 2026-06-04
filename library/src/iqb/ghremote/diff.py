"""Diff between a remote manifest and the local cache directory."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from .cache import FileEntry, Manifest, compute_sha256
from .entrypath import ManifestEntryPath, parse_entry_path

log = logging.getLogger("iqb.ghremote.diff")


class DiffState(StrEnum):
    """State of a diff entry comparing manifest vs local cache."""

    ONLY_REMOTE = "only_remote"
    ONLY_LOCAL = "only_local"
    SHA256_MISMATCH = "sha256_mismatch"
    MATCHING = "matching"


@dataclass(frozen=True, kw_only=True)
class DiffEntry:
    """Single entry in a manifest-vs-local diff."""

    file: ManifestEntryPath
    url: str | None
    remote_sha256: str | None
    local_sha256: str | None
    state: DiffState


def _scan_local_files(data_dir: Path) -> set[ManifestEntryPath]:
    """Walk the local cache directory and return parsed entry paths."""
    result: set[ManifestEntryPath] = set()
    cache_dir = data_dir / "cache" / "v1"
    if not cache_dir.exists():
        return result
    for path in cache_dir.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(data_dir).as_posix()
        try:
            result.add(parse_entry_path(rel))
        except ValueError:
            continue
    return result


def diff(
    manifest: Manifest,
    data_dir: Path,
    *,
    acceptp: Callable[[ManifestEntryPath], bool] | None = None,
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
        acceptp: Optional predicate applied to manifest entry paths.
                 ``None`` means accept everything.
    """
    # Phase 1: scan local files, applying acceptp
    local_files = _scan_local_files(data_dir)
    if acceptp is not None:
        local_files = {f for f in local_files if acceptp(f)}

    # Phase 2: iterate accepted manifest keys in sorted order
    seen: set[ManifestEntryPath] = set()
    for key in sorted(manifest.files, key=str):
        if acceptp is not None and not acceptp(key):
            continue
        seen.add(key)
        entry: FileEntry = manifest.files[key]
        local_path = data_dir / Path(str(key))
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
    for key in sorted(remaining, key=str):
        local_sha256 = compute_sha256(data_dir / Path(str(key)))
        yield DiffEntry(
            file=key,
            url=None,
            remote_sha256=None,
            local_sha256=local_sha256,
            state=DiffState.ONLY_LOCAL,
        )

"""Module containing the IQBRemoteCache implementation."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import urlopen

from dacite import from_dict
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from ..pipeline.cache import PipelineCacheEntry, data_dir_or_default

log = logging.getLogger("iqb.ghremote.cache")


@dataclass(frozen=True, kw_only=True)
class FileEntry:
    """Entry in the manifest for a single cached file."""

    sha256: str
    url: str


@dataclass(kw_only=True)
class Manifest:
    """Manifest for remotely cached files."""

    v: int
    files: dict[str, FileEntry] = field(default_factory=dict)

    def __post_init__(self):
        if self.v != 0:
            raise ValueError(f"Unsupported manifest version: {self.v} (only v=0 supported)")

    def get_file_entry(self, *, full_path: Path, data_dir: Path) -> FileEntry:
        """
        Return the file entry corresponding to a given full path and data directory.

        Raises:
            KeyError: if the given remote entry does not exist.
        """
        # Use .as_posix() to ensure forward slashes for cross-platform compatibility
        # Manifest keys should always use forward slashes regardless of OS
        key = full_path.relative_to(data_dir).as_posix()
        try:
            return self.files[key]
        except KeyError as exc:
            raise KeyError(f"no remotely-cached file for {key}") from exc


def load_manifest(manifest_file: Path) -> Manifest:
    """Load manifest from the given file, or return empty manifest if not found."""
    if not manifest_file.exists():
        return Manifest(v=0, files={})

    with open(manifest_file) as filep:
        data = json.load(filep)

    return from_dict(Manifest, data)


def manifest_path_for_data_dir(data_dir: Path) -> Path:
    """Return the manifest path under the given data directory."""
    return data_dir / "state" / "ghremote" / "manifest.json"


def save_manifest(manifest: Manifest, manifest_file: Path) -> None:
    """Save manifest to the given file path."""
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_file, "w") as filep:
        json.dump(asdict(manifest), filep, indent=2, sort_keys=True)
        filep.write("\n")


class IQBRemoteCache:
    """
    Remote cache for query results.

    This class implements the pipeline.RemoteCache protocol. It downloads
    files from URLs specified in the manifest and verifies their SHA256 hashes.

    The manifest is loaded from $datadir/state/ghremote/manifest.json,
    where $datadir defaults to .iqb in the current working directory.
    """

    def __init__(
        self,
        *,
        data_dir: str | Path | None = None,
    ) -> None:
        self.data_dir = data_dir_or_default(data_dir)
        manifest_path = manifest_path_for_data_dir(self.data_dir)
        self.manifest = load_manifest(manifest_path)

    def sync(self, entry: PipelineCacheEntry) -> bool:
        """
        Sync remote cache entry to disk and return whether
        we successfully synced it or not. Emits logging messages
        explaining what it is doing and warning about issues
        occurred while trying to sync from the remote.
        """
        try:
            log.info("syncing %s... start", entry)
            self._sync(entry)
            log.info("syncing %s... ok", entry)
            return True
        except Exception as exc:
            log.warning("syncing %s... failure: %s", entry, exc)
            return False

    def _sync(self, entry: PipelineCacheEntry):
        # Lookup files in the manifest using pipeline-provided paths
        # so we don't need to revalidate them again.
        parquet_entry = self.manifest.get_file_entry(
            full_path=entry.data_parquet_file_path(),
            data_dir=entry.data_dir,
        )
        json_entry = self.manifest.get_file_entry(
            full_path=entry.stats_json_file_path(),
            data_dir=entry.data_dir,
        )

        # Sync both entries given preference to the JSON since it's smaller
        # and leads to less wasted bandwidth if the parquet doesn't exist.
        _sync_file_entry(json_entry, entry.stats_json_file_path())
        _sync_file_entry(parquet_entry, entry.data_parquet_file_path())


# TODO(bassosimone): this download logic overlaps with cli/cache_pull.py;
# consider unifying into a shared helper once both implementations stabilise.
def _sync_file_entry(entry: FileEntry, dest_path: Path):
    """Sync the given FileEntry with the remotely cached file."""
    # Determine whether we need to download again
    exists = dest_path.exists()
    if not exists or entry.sha256 != compute_sha256(dest_path):
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        # Operate inside a temporary directory in the destination directory so
        # `os.replace()` is atomic and we avoid cross-filesystem moves.
        with TemporaryDirectory(dir=dest_path.parent) as tmp_dir:
            tmp_file = Path(tmp_dir) / dest_path.name
            _sync_file_entry_tmp(entry, tmp_file)
            # On Windows, Arrow opens files without FILE_SHARE_DELETE, which
            # can block replace while readers hold the file open. YAGNI: if
            # this becomes an issue, add a retry/backoff loop or a different
            # synchronization mechanism.
            os.replace(tmp_file, dest_path)


def _sync_file_entry_tmp(entry: FileEntry, tmp_file: Path):
    # Download into the temporary file
    log.info("fetching %s... start", entry)

    with urlopen(entry.url) as response, open(tmp_file, "wb") as filep:
        total = response.headers.get("Content-Length")
        total = int(total) if total is not None else None

        columns = [SpinnerColumn(), TextColumn("{task.description}")]
        if total is not None:
            columns.extend(
                [
                    BarColumn(),
                    DownloadColumn(),
                    TransferSpeedColumn(),
                    TimeRemainingColumn(),
                ]
            )
        columns.append(TimeElapsedColumn())
        with Progress(*columns, transient=False) as progress:
            task = progress.add_task(tmp_file.name, total=total)
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                filep.write(chunk)
                progress.update(task, advance=len(chunk))

    log.info("fetching %s... ok", entry)

    # Make sure the sha256 matches
    log.info("validating %s... start", entry)
    sha256 = compute_sha256(tmp_file)
    if sha256 != entry.sha256:
        raise ValueError(f"SHA256 mismatch: expected {entry.sha256}, got {sha256}")
    log.info("validating %s... ok", entry)


def compute_sha256(path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(path, "rb") as fp:
        while chunk := fp.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()

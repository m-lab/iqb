"""Module containing the RemoteCache implementation."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.request import urlopen

from dacite import from_dict

from ..pipeline.cache import PipelineCacheEntry


@dataclass(frozen=True)
class FileEntry:
    """Entry in the manifest for a single cached file."""

    sha256: str
    url: str


@dataclass(frozen=True)
class Manifest:
    """Manifest for cached files stored in GitHub releases."""

    v: int
    files: dict[str, FileEntry]

    def get_file_entry(self, *, full_path: Path, data_dir: Path) -> FileEntry:
        """
        Return the file entry corresponding to a given full path and data directory.

        Raises:
            KeyError: if the given remote entry does not exist.
        """
        key = str(full_path.relative_to(data_dir))
        try:
            return self.files[key]
        except KeyError as exc:
            raise KeyError(f"no remotely-cached file for {key}") from exc


def iqb_github_load_manifest(manifest_file: Path) -> Manifest:
    """Load manifest from the given file, or return empty manifest if not found."""
    if not manifest_file.exists():
        return Manifest(v=0, files={})

    with open(manifest_file) as filep:
        data = json.load(filep)

    return from_dict(Manifest, data)


class IQBGitHubRemoteCache:
    """
    Remote cache for query results using GitHub releases.

    This class implements the pipeline.RemoteCache protocol.
    """

    def __init__(self, manifest: Manifest) -> None:
        self.manifest = manifest

    def sync(self, entry: PipelineCacheEntry) -> bool:
        """
        Sync remote cache entry to disk and return whether
        we successfully synced it or not. Emits logging messages
        explaining what it is doing and warning about issues
        occurred while trying to sync from the remote.
        """
        try:
            logging.info(f"ghremote: syncing {entry}... start")
            self._sync(entry)
            logging.info(f"ghremote: syncing {entry}... ok")
            return True
        except Exception as exc:
            logging.warning(f"ghremote: syncing {entry}... failure: {exc}")
            return False

    def _sync(self, entry: PipelineCacheEntry):
        # 0. Warn the user that this code probably does not work
        # on Windows systems. TODO(bassosimone): fix this.
        (
            logging.warning("ghremote: this code has not been tested on Windows")
            if sys.platform == "windows"
            else None
        )

        # 1. Lookup files in the manifest using pipeline-provided paths
        # so we don't need to revalidate them again.
        parquet_entry = self.manifest.get_file_entry(
            full_path=entry.data_parquet_file_path(),
            data_dir=entry.data_dir,
        )
        json_entry = self.manifest.get_file_entry(
            full_path=entry.stats_json_file_path(),
            data_dir=entry.data_dir,
        )

        # 2. Sync both entries given preference to the JSON since it's smaller
        # and leads to less wasted bandwidth if the parquet doesn't exist.
        _sync_file_entry(json_entry, entry.stats_json_file_path())
        _sync_file_entry(parquet_entry, entry.data_parquet_file_path())


def _sync_file_entry(entry: FileEntry, dest_path: Path):
    """Sync the given FileEntry with the file cached in a GitHub release."""
    # Determine whether we need to download again
    exists = dest_path.exists()
    if not exists or entry.sha256 != _compute_sha256(dest_path):
        # If old file exists, remove it
        if exists:
            os.unlink(dest_path)

        # Download into the destination file directly
        logging.info(f"ghremote: fetching {entry}... start")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with urlopen(entry.url) as response, open(dest_path, "wb") as fp:
            while chunk := response.read(8192):
                fp.write(chunk)
        logging.info(f"ghremote: fetching {entry}... ok")

        # Make sure the sha256 matches
        logging.info(f"ghremote: validating {entry}... start")
        sha256 = _compute_sha256(dest_path)
        if sha256 != entry.sha256:
            os.unlink(dest_path)
            raise ValueError(f"SHA256 mismatch: expected {entry.sha256}, got {sha256}")
        logging.info(f"ghremote: validating {entry}... ok")


def _compute_sha256(path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(path, "rb") as fp:
        while chunk := fp.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()

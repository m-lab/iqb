#!/usr/bin/env python3

"""
Remote cache management tool for IQB data files.

Usage:
    uv run python data/ghcache.py scan

This tool manages caching of large parquet/JSON files with local SHA256
verification. It scans locally generated files and updates the manifest
with correct GCS URLs for remote distribution.

Subcommands:
  scan - Scan local files and update manifest

The 'scan' command computes SHA256 hashes for new or changed cache files,
updates the manifest with correct GCS URLs, and prints the gcloud storage
rsync command needed to upload the files.

Manifest format (state/ghremote/manifest.json):
{
  "v": 0,
  "files": {
    "cache/v1/.../data.parquet": {
      "sha256": "3a421c62179a...",
      "url": "https://storage.googleapis.com/BUCKET/cache/v1/.../data.parquet"
    }
  }
}

File path format: cache/v1/{start_ts}/{end_ts}/{name}/data.parquet
"""

import argparse
import json
import os
import sys
from pathlib import Path

from dacite import from_dict

from iqb.ghremote.cache import Manifest
from iqb.ghremote.diff import DiffState, diff

MANIFEST_PATH = Path("state") / "ghremote" / "manifest.json"
GCS_BUCKET = "mlab-sandbox-iqb-us-central1"
GCS_BASE_URL = f"https://storage.googleapis.com/{GCS_BUCKET}"


def load_manifest() -> dict:
    """Load manifest from state/ghremote/manifest.json, or return empty if not found."""
    if not MANIFEST_PATH.exists():
        return {"v": 0, "files": {}}

    with open(MANIFEST_PATH) as f:
        return json.load(f)


def save_manifest(manifest: dict) -> None:
    """Save manifest to state/ghremote/manifest.json."""
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")  # Trailing newline


def cmd_scan(args) -> int:
    """
    Scan command: Scan local files and update manifest.

    1. Load or create manifest
    2. Diff manifest against local cache/v1 files
    3. For new or changed files:
       - Compute SHA256
       - Generate GCS URL
       - Update manifest
    4. Save manifest
    5. Print gcloud storage rsync command for uploading
    """
    _ = args
    manifest_dict = load_manifest()
    files_dict = manifest_dict.setdefault("files", {})
    manifest = from_dict(Manifest, manifest_dict)

    print("Scanning local cache files...")

    updated_count = 0

    for entry in diff(manifest, Path(".")):
        if entry.state == DiffState.MATCHING:
            print(f"Already in manifest: {entry.file}")
        elif entry.state in (DiffState.ONLY_LOCAL, DiffState.SHA256_MISMATCH):
            sha256 = entry.local_sha256
            url = f"{GCS_BASE_URL}/{entry.file}"
            action = "Changed" if entry.state == DiffState.SHA256_MISMATCH else "New"
            print(f"{action}: {entry.file}")
            print(f"  SHA256: {sha256}")
            print(f"  URL: {url}")
            files_dict[entry.file] = {"sha256": sha256, "url": url}
            updated_count += 1
        elif entry.state == DiffState.ONLY_REMOTE:
            print(f"In manifest but not on disk: {entry.file}")

    # Save updated manifest
    save_manifest(manifest_dict)
    print(f"\nManifest updated: {MANIFEST_PATH}")

    if updated_count > 0:
        print(f"\n{updated_count} file(s) added/updated in manifest.")
        print("\nNext steps:")
        print("1. Remove zero-length .lock files left over by the pipeline:")
        print("   find data/cache/v1 -type f -name .lock -delete")
        print("2. Upload files to GCS:")
        print(f"   gcloud storage rsync -r data/cache/v1 gs://{GCS_BUCKET}/cache/v1")
        print(f"3. Commit updated data/{MANIFEST_PATH} to repository")

    return 0


def main() -> int:
    # Change to script's directory (./data) so all paths are relative to it
    script_dir = Path(__file__).resolve().parent
    os.chdir(script_dir)

    parser = argparse.ArgumentParser(
        description="Remote cache management tool for IQB data files. "
        "Run with: uv run python data/ghcache.py <command>",
    )
    subparsers = parser.add_subparsers(dest="command", help="Subcommand to run")

    # Scan subcommand
    subparsers.add_parser("scan", help="Scan local files and update manifest")

    args = parser.parse_args()

    if args.command == "scan":
        return cmd_scan(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

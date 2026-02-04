#!/usr/bin/env python3

"""
Remote cache management tool for IQB data files.

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
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

MANIFEST_PATH = Path("state") / "ghremote" / "manifest.json"
CACHE_DIR = Path("cache") / "v1"
GCS_BUCKET = "mlab-sandbox-iqb-us-central1"
GCS_BASE_URL = f"https://storage.googleapis.com/{GCS_BUCKET}"


def compute_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()


def validate_cache_path(path: str) -> bool:
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

    # Component 1: cache
    if parts[0] != "cache":
        return False

    # Component 2: v1
    if parts[1] != "v1":
        return False

    # Components 3-4: RFC3339 timestamps (YYYYMMDDTHHMMSSZ format)
    rfc3339_pattern = re.compile(r"^\d{8}T\d{6}Z$")
    if not rfc3339_pattern.match(parts[2]):
        return False
    if not rfc3339_pattern.match(parts[3]):
        return False

    # Component 5: lowercase letters, numbers, and underscores
    name_pattern = re.compile(r"^[a-z0-9_]+$")
    if not name_pattern.match(parts[4]):
        return False

    # Component 6: data.parquet or stats.json
    return parts[5] in ("data.parquet", "stats.json")


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


def is_git_ignored(file_path: Path) -> bool:
    """Check if a file is ignored by git."""
    try:
        result = subprocess.run(
            ["git", "check-ignore", str(file_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Exit code 0 means the file is ignored
        return result.returncode == 0
    except Exception:
        # If git isn't available or other error, assume not ignored
        return False


def cmd_scan(args) -> int:
    """
    Scan command: Scan local files and update manifest.

    1. Load or create manifest
    2. Scan cache/v1 for git-ignored files
    3. For new or changed files:
       - Compute SHA256
       - Generate GCS URL
       - Update manifest
    4. Save manifest
    5. Print gcloud storage rsync command for uploading
    """
    _ = args
    manifest = load_manifest()
    files_dict = manifest.setdefault("files", {})

    if not CACHE_DIR.exists():
        print(f"Cache directory {CACHE_DIR} does not exist.")
        return 1

    print(f"Scanning {CACHE_DIR} for git-ignored files...")

    # Find all files under cache/v1
    all_files = list(CACHE_DIR.rglob("*"))
    cache_files = [f for f in all_files if f.is_file()]

    # Filter to only git-ignored files
    ignored_files = [f for f in cache_files if is_git_ignored(f)]

    if not ignored_files:
        print("No git-ignored files found.")
        return 0

    print(f"Found {len(ignored_files)} git-ignored files.")

    updated_count = 0

    for file_path in ignored_files:
        # Convert to relative path string with forward slashes for cross-platform compatibility
        rel_path = file_path.as_posix()

        # Validate path format
        if not validate_cache_path(rel_path):
            print(f"Skipping invalid path format: {rel_path}")
            continue

        # Compute SHA256
        sha256 = compute_sha256(file_path)

        # Check if file is already in manifest with same SHA256
        existing_entry = files_dict.get(rel_path)
        if existing_entry and existing_entry["sha256"] == sha256:
            print(f"Already in manifest: {rel_path}")
            continue

        # File is new or changed
        url = f"{GCS_BASE_URL}/{rel_path}"
        print(f"New/changed: {rel_path}")
        print(f"  SHA256: {sha256}")
        print(f"  URL: {url}")

        files_dict[rel_path] = {"sha256": sha256, "url": url}
        updated_count += 1

    # Save updated manifest
    save_manifest(manifest)
    print(f"\nManifest updated: {MANIFEST_PATH}")

    if updated_count > 0:
        print(f"\n{updated_count} file(s) added/updated in manifest.")
        print("\nNext steps:")
        print("1. Remove zero-length .lock files left over by the pipeline:")
        print(f"   find data/{CACHE_DIR} -type f -name .lock -delete")
        print("2. Upload files to GCS:")
        print(f"   gcloud storage rsync -r data/{CACHE_DIR} gs://{GCS_BUCKET}/{CACHE_DIR}")
        print(f"3. Commit updated data/{MANIFEST_PATH} to repository")

    return 0


def main() -> int:
    # Change to script's directory (./data) so all paths are relative to it
    script_dir = Path(__file__).resolve().parent
    os.chdir(script_dir)

    parser = argparse.ArgumentParser(description="Remote cache management tool for IQB data files")
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

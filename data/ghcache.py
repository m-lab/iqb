#!/usr/bin/env python3

"""
GitHub cache synchronization tool for IQB data files.

**INTERIM SOLUTION**: This is a throwaway script for the initial phase of the
project. It will eventually be replaced by a proper GCS-based solution.

This tool manages caching of large parquet/JSON files using GitHub releases
as a distribution mechanism, with local SHA256 verification.

Subcommands:
  scan - Scan local files and prepare them for upload to GitHub release
  sync - Download files from GitHub release based on manifest

The 'scan' command copies files to the current directory with mangled names,
ready for manual upload to GitHub releases.

Manifest format (state/ghremote/manifest.json):
{
  "v": 0,
  "files": {
    "cache/v1/.../data.parquet": {
      "sha256": "3a421c62179a...",
      "url": "https://github.com/.../3a421c62179a__cache__v1__...parquet"
    }
  }
}

File path format: cache/v1/{start_ts}/{end_ts}/{name}/data.parquet
Mangled format: {sha256[:12]}__cache__v1__{start_ts}__{end_ts}__{name}__data.parquet
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


MANIFEST_PATH = Path("state") / "ghremote" / "manifest.json"
CACHE_DIR = Path("cache/v1")
SHA256_PREFIX_LENGTH = 12


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
    if parts[5] not in ("data.parquet", "stats.json"):
        return False

    return True


def mangle_path(local_path: str, sha256: str) -> str:
    """
    Convert local path to mangled GitHub release filename.

    Example:
      Input:  cache/v1/20241001T000000Z/20241101T000000Z/downloads_by_country/data.parquet
      SHA256: 3a421c62179a...
      Output: 3a421c62179a__cache__v1__20241001T000000Z__20241101T000000Z__downloads_by_country__data.parquet
    """
    sha_prefix = sha256[:SHA256_PREFIX_LENGTH]
    mangled = local_path.replace("/", "__")
    return f"{sha_prefix}__{mangled}"


def load_manifest() -> dict:
    """Load manifest from state/ghremote/manifest.json, or return empty if not found."""
    if not MANIFEST_PATH.exists():
        return {"v": 0, "files": {}}

    with open(MANIFEST_PATH, "r") as f:
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
    Scan command: Scan local files and prepare for upload.

    1. Load or create manifest
    2. Scan cache/v1 for git-ignored files
    3. For new or changed files:
       - Compute SHA256
       - Create mangled filename
       - Copy to current directory (ready for manual upload)
       - Update manifest
    4. Save manifest
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

    files_to_upload = []

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
        print(f"New/changed: {rel_path}")
        print(f"  SHA256: {sha256}")

        # Create mangled filename
        mangled_name = mangle_path(rel_path, sha256)
        print(f"  Mangled: {mangled_name}")

        # Copy to current directory with mangled name (for manual upload)
        dest_path = Path(mangled_name)
        shutil.copy2(file_path, dest_path)
        print(f"  Copied to ./{mangled_name} (ready for upload)")

        # Prepare manifest entry (URL will need to be filled in manually or via script)
        # For now, use placeholder URL
        url_placeholder = (
            f"https://github.com/m-lab/iqb/releases/download/v0.2.0/{mangled_name}"
        )

        files_dict[rel_path] = {"sha256": sha256, "url": url_placeholder}
        files_to_upload.append(mangled_name)

    # Save updated manifest
    save_manifest(manifest)
    print(f"\nManifest updated: {MANIFEST_PATH}")

    if files_to_upload:
        print(f"\nFiles ready for upload ({len(files_to_upload)}):")
        for f in files_to_upload:
            print(f"  {f}")
        print("\nNext steps:")
        print("1. Upload mangled files to GitHub release v0.2.0")
        print("2. Update URLs in state/ghremote/manifest.json if needed")
        print("3. Commit updated state/ghremote/manifest.json to repository")

    return 0


def main() -> int:
    # Change to script's directory (./data) so all paths are relative to it
    script_dir = Path(__file__).resolve().parent
    os.chdir(script_dir)

    parser = argparse.ArgumentParser(
        description="GitHub cache synchronization tool for IQB data files (interim solution)"
    )
    subparsers = parser.add_subparsers(dest="command", help="Subcommand to run")

    # Scan subcommand
    subparsers.add_parser("scan", help="Scan local files and prepare for upload")

    args = parser.parse_args()

    if args.command == "scan":
        return cmd_scan(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

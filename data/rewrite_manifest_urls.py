#!/usr/bin/env python3

"""One-off script to rewrite manifest URLs to point to a GCS bucket.

Usage:
    python rewrite_manifest_urls.py https://storage.googleapis.com/mlab-sandbox-iqb-us-central1
"""

import json
import sys
from pathlib import Path

MANIFEST_PATH = Path("state") / "ghremote" / "manifest.json"


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <base-url>", file=sys.stderr)
        return 1

    base_url = sys.argv[1].rstrip("/")

    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)

    for key, entry in manifest["files"].items():
        entry["url"] = f"{base_url}/{key}"

    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"Rewrote {len(manifest['files'])} URLs with base {base_url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

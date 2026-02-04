"""
Remote cache synchronization for IQB data files.

The `ghremote` package (globally-hosted remote) downloads cache files
from URLs listed in a manifest and verifies their SHA256 hashes.

Manifest format:

{
  "v": 0,
  "files": {
    "cache/v1/.../data.parquet": {
      "sha256": "3a421c62179a...",
      "url": "https://storage.googleapis.com/...data.parquet"
    }
  }
}

The manifest is expected at:

    $datadir/state/ghremote/manifest.json

Where $datadir defaults to `.iqb` in the current working directory.
"""

from .cache import IQBRemoteCache
from .diff import DiffEntry, DiffState, diff

# Backward compatibility alias
IQBGitHubRemoteCache = IQBRemoteCache

__all__ = [
    "DiffEntry",
    "DiffState",
    "IQBGitHubRemoteCache",
    "IQBRemoteCache",
    "diff",
]

"""
GitHub remote cache synchronization tool for IQB data files.

This is a throwaway module for the initial phase of the project. It will
eventually be replaced by a proper GCS-based solution.

Manifest format:

{
  "v": 0,
  "files": {
    "cache/v1/.../data.parquet": {
      "sha256": "3a421c62179a...",
      "url": "https://github.com/.../3a421c62179a__cache__v1__...parquet"
    }
  }
}

The manifest is expected at:

    $datadir/state/ghremote/manifest.json

Where $datadir defaults to `.iqb` in the current working directory.
"""

from .cache import IQBGitHubRemoteCache

__all__ = [
    "IQBGitHubRemoteCache",
]

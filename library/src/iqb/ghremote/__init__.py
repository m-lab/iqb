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
"""

from .cache import (
    IQBGitHubRemoteCache,
    iqb_github_load_manifest,
)

__all__ = [
    "IQBGitHubRemoteCache",
    "iqb_github_load_manifest",
]

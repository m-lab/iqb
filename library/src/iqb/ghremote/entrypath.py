"""Parsed representation of a manifest entry path."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from ..pipeline.cache import PIPELINE_CACHE_TS_FORMAT

_RFC3339_RE = re.compile(r"^\d{8}T\d{6}Z$")
_DATASET_RE = re.compile(r"^[a-z0-9_]+$")
_VALID_FILENAMES = frozenset(("data.parquet", "stats.json"))


@dataclass(frozen=True, kw_only=True)
class ManifestEntryPath:
    """Parsed cache path of the form ``cache/v1/{start}/{end}/{dataset}/{filename}``."""

    start: str
    end: str
    dataset: str
    filename: str

    def __str__(self) -> str:
        return f"cache/v1/{self.start}/{self.end}/{self.dataset}/{self.filename}"


def cache_ts_to_date(ts: str) -> str:
    """Convert a ``YYYYMMDDTHHMMSSZ`` cache timestamp to a ``YYYY-MM-DD`` date string."""
    return f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"


def date_to_cache_ts(date_str: str) -> str:
    """Convert a ``YYYY-MM-DD`` date string to the ``YYYYMMDDTHHMMSSZ`` cache timestamp format."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime(PIPELINE_CACHE_TS_FORMAT)


def parse_entry_path(raw: str) -> ManifestEntryPath:
    """Parse a raw cache path string into a :class:`ManifestEntryPath`.

    Raises:
        ValueError: if the path does not match the expected format.
    """
    parts = raw.split("/")
    if len(parts) != 6:
        raise ValueError(f"expected 6 path components, got {len(parts)}: {raw!r}")
    if parts[0] != "cache":
        raise ValueError(f"first component must be 'cache', got {parts[0]!r}: {raw!r}")
    if parts[1] != "v1":
        raise ValueError(f"second component must be 'v1', got {parts[1]!r}: {raw!r}")
    if not _RFC3339_RE.match(parts[2]):
        raise ValueError(f"invalid start timestamp {parts[2]!r}: {raw!r}")
    if not _RFC3339_RE.match(parts[3]):
        raise ValueError(f"invalid end timestamp {parts[3]!r}: {raw!r}")
    if not _DATASET_RE.match(parts[4]):
        raise ValueError(f"invalid dataset {parts[4]!r}: {raw!r}")
    if parts[5] not in _VALID_FILENAMES:
        raise ValueError(f"invalid filename {parts[5]!r}: {raw!r}")
    return ManifestEntryPath(
        start=parts[2],
        end=parts[3],
        dataset=parts[4],
        filename=parts[5],
    )

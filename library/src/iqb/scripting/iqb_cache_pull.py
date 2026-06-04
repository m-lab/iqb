"""Optional scripting extensions to pull cache entries."""

# TODO(bassosimone): this download logic overlaps with ghremote/cache.py;
# consider unifying into a shared helper once both implementations stabilise.

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import requests
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TransferSpeedColumn,
)

from ..ghremote import DiffState, Manifest, diff, load_manifest, manifest_path_for_data_dir
from ..ghremote.diff import DiffEntry
from ..ghremote.entrypath import ManifestEntryPath
from ..pipeline.cache import data_dir_or_default

_thread_local = threading.local()


def _get_session() -> requests.Session:
    """Return a per-thread requests.Session for connection reuse."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


def _short_name(path: ManifestEntryPath) -> str:
    """Extract a short display name from a manifest entry path."""
    return f"{path.dataset}/{path.filename}"


def _now() -> str:
    """Return the current time formatted for metrics spans."""
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


def _download_one(
    entry: DiffEntry,
    data_dir: Path,
    progress: Progress,
) -> dict[str, object]:
    """Download a single file, verify SHA256, atomic-replace. Returns a metrics span."""
    t0 = _now()
    worker_id = threading.get_ident()
    total_bytes = 0
    content_length: int | None = None
    ok = True
    error: str | None = None
    assert entry.url is not None
    assert entry.remote_sha256 is not None
    dest = data_dir / Path(str(entry.file))
    dest.parent.mkdir(parents=True, exist_ok=True)
    task_id = progress.add_task(_short_name(entry.file), total=None)
    try:
        with TemporaryDirectory(dir=dest.parent) as tmp_dir:
            tmp_file = Path(tmp_dir) / dest.name
            session = _get_session()
            resp = session.get(entry.url, stream=True)
            resp.raise_for_status()
            cl = resp.headers.get("Content-Length")
            if cl is not None:
                content_length = int(cl)
                progress.update(task_id, total=content_length)
            sha256 = hashlib.sha256()
            with open(tmp_file, "wb") as fp:
                for chunk in resp.iter_content(chunk_size=8192):
                    fp.write(chunk)
                    sha256.update(chunk)
                    total_bytes += len(chunk)
                    progress.update(task_id, advance=len(chunk))
            got = sha256.hexdigest()
            if got != entry.remote_sha256:
                raise ValueError(
                    f"SHA256 mismatch for {str(entry.file)}: expected {entry.remote_sha256}, got {got}"
                )
            os.replace(tmp_file, dest)
    except Exception as exc:
        ok = False
        error = str(exc)
    finally:
        progress.remove_task(task_id)
    return {
        "t0": t0,
        "t": _now(),
        "worker_id": worker_id,
        "file": str(entry.file),
        "url": entry.url,
        "content_length": content_length,
        "bytes": total_bytes,
        "ok": ok,
        "error": error,
    }


@dataclass(frozen=True, kw_only=True)
class PullResult:
    """Result of a cache pull operation."""

    total: int
    ok: int
    failed: list[tuple[str, str]] = field(default_factory=list)
    log_file: Path
    elapsed: float


def run(
    *,
    data_dir: str | Path | None = None,
    manifest: Manifest | None = None,
    datasets: tuple[str, ...] = (),
    files: tuple[str, ...] = (),
    after: str | None = None,
    before: str | None = None,
    force: bool = False,
    jobs: int = 8,
) -> PullResult | None:
    """Pull cache entries from the remote manifest.

    When *manifest* is provided it is used directly; otherwise the manifest
    is loaded from disk under *data_dir*.

    Returns ``None`` when there is nothing to download.
    """
    resolved = data_dir_or_default(data_dir)
    if manifest is None:
        manifest_path = manifest_path_for_data_dir(resolved)
        manifest = load_manifest(manifest_path)
    manifest = manifest.filter(datasets=datasets, files=files, after=after, before=before)

    # Collect entries to download
    targets: list[DiffEntry] = []
    for entry in diff(manifest, resolved):
        if entry.state == DiffState.ONLY_REMOTE:
            targets.append(entry)
        elif entry.state == DiffState.SHA256_MISMATCH and force:
            targets.append(entry)

    if not targets:
        return None

    failed: list[tuple[str, str]] = []
    spans: list[dict[str, object]] = []
    t0 = time.monotonic()
    with (
        Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
        ) as progress,
        ThreadPoolExecutor(max_workers=jobs) as pool,
    ):
        futures = {
            pool.submit(_download_one, entry, resolved, progress): entry for entry in targets
        }
        for future in as_completed(futures):
            entry = futures[future]
            try:
                span = future.result()
                spans.append(span)
                if not span["ok"]:
                    failed.append((str(span["file"]), str(span["error"] or "unknown")))
            except Exception as exc:
                # Defensive: bug in _download_one itself
                failed.append((str(entry.file), str(exc)))
    elapsed = time.monotonic() - t0

    # Write metrics
    log_dir = resolved / "state" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S")
    log_file = log_dir / f"{now}_{time.time_ns()}_pull.jsonl"
    with open(log_file, "w", encoding="utf-8") as fp:
        for span in spans:
            fp.write(json.dumps(span) + "\n")

    ok_count = len(targets) - len(failed)

    return PullResult(
        total=len(targets),
        ok=ok_count,
        failed=failed,
        log_file=log_file,
        elapsed=elapsed,
    )

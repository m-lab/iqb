"""Cache push command."""

import time
from pathlib import Path

import click
from google.cloud import storage
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TransferSpeedColumn,
)

from ..ghremote import (
    DiffState,
    FileEntry,
    diff,
    load_manifest,
    manifest_path_for_data_dir,
    save_manifest,
)
from ..ghremote.diff import DiffEntry
from ..pipeline.cache import data_dir_or_default
from ..scripting import iqb_logging
from .cache import cache

_DEFAULT_BUCKET = "mlab-sandbox-iqb-us-central1"
_GCS_BASE_URL = "https://storage.googleapis.com"


def _short_name(file: str) -> str:
    """Extract a short display name from a cache path."""
    parts = file.split("/")
    return "/".join(parts[-2:]) if len(parts) >= 2 else file


class _ProgressReader:
    """Wraps a file object to update a Rich progress bar on each read."""

    def __init__(self, fp, progress: Progress, task_id) -> None:  # noqa: ANN001
        self._fp = fp
        self._progress = progress
        self._task_id = task_id

    def read(self, size: int = -1) -> bytes:
        data = self._fp.read(size)
        if data:
            self._progress.update(self._task_id, advance=len(data))
        return data


def _upload_one(
    entry: DiffEntry,
    data_dir: Path,
    bucket: storage.Bucket,
    progress: Progress,
) -> str:
    """Upload a single file to GCS with progress tracking. Returns the file path."""
    assert entry.local_sha256 is not None
    source = data_dir / entry.file
    file_size = source.stat().st_size
    task_id = progress.add_task(_short_name(entry.file), total=file_size)
    try:
        blob = bucket.blob(entry.file)
        with open(source, "rb") as fp:
            reader = _ProgressReader(fp, progress, task_id)
            blob.upload_from_file(reader, size=file_size)
    finally:
        progress.remove_task(task_id)
    return entry.file


@cache.command()
@click.option("-d", "--dir", "data_dir", default=None, help="Data directory (default: .iqb)")
@click.option("--bucket", default=_DEFAULT_BUCKET, show_default=True, help="GCS bucket name")
@click.option("-f", "--force", is_flag=True, help="Re-upload files with mismatched hashes")
def push(data_dir: str | None, bucket: str, force: bool) -> None:
    """Upload new local cache files to GCS and update the manifest."""
    iqb_logging.configure(False)
    resolved = data_dir_or_default(data_dir)
    manifest_path = manifest_path_for_data_dir(resolved)
    manifest = load_manifest(manifest_path)

    # Collect entries to upload
    targets: list[DiffEntry] = []
    for entry in diff(manifest, resolved):
        if entry.state == DiffState.ONLY_LOCAL:
            targets.append(entry)
        elif entry.state == DiffState.SHA256_MISMATCH and force:
            targets.append(entry)

    if not targets:
        click.echo("Nothing to upload.")
        return

    client = storage.Client()
    gcs_bucket = client.bucket(bucket)

    failed: list[tuple[str, str]] = []
    t0 = time.monotonic()
    with Progress(
        TextColumn("{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
    ) as progress:
        for entry in targets:
            try:
                _upload_one(entry, resolved, gcs_bucket, progress)
            except Exception as exc:
                failed.append((entry.file, str(exc)))
                continue
            # Update manifest after each successful upload (crash-safe)
            assert entry.local_sha256 is not None
            url = f"{_GCS_BASE_URL}/{bucket}/{entry.file}"
            manifest.files[entry.file] = FileEntry(sha256=entry.local_sha256, url=url)
            save_manifest(manifest, manifest_path)
    elapsed = time.monotonic() - t0

    ok = len(targets) - len(failed)
    click.echo(f"Uploaded {ok}/{len(targets)} file(s) in {elapsed:.1f}s.")

    if failed:
        click.echo(f"{len(failed)} upload(s) failed:", err=True)
        for file, reason in failed:
            click.echo(f"  {file}: {reason}", err=True)
        raise SystemExit(1)

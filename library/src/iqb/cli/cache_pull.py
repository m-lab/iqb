"""Cache pull command."""

import hashlib
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tempfile import TemporaryDirectory

import click
import requests
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TransferSpeedColumn,
)

from ..ghremote import DiffState, diff, load_manifest, manifest_path_for_data_dir
from ..ghremote.diff import DiffEntry
from ..pipeline.cache import data_dir_or_default
from .cache import cache


def _short_name(file: str) -> str:
    """Extract a short display name from a cache path."""
    parts = file.split("/")
    return "/".join(parts[-2:]) if len(parts) >= 2 else file


def _download_one(
    entry: DiffEntry,
    data_dir: Path,
    session: requests.Session,
    progress: Progress,
) -> str:
    """Download a single file, verify SHA256, atomic-replace. Returns the file path."""
    assert entry.url is not None
    assert entry.remote_sha256 is not None
    dest = data_dir / entry.file
    dest.parent.mkdir(parents=True, exist_ok=True)
    task_id = progress.add_task(_short_name(entry.file), total=None)
    try:
        with TemporaryDirectory(dir=dest.parent) as tmp_dir:
            tmp_file = Path(tmp_dir) / dest.name
            resp = session.get(entry.url, stream=True)
            resp.raise_for_status()
            content_length = resp.headers.get("Content-Length")
            if content_length is not None:
                progress.update(task_id, total=int(content_length))
            sha256 = hashlib.sha256()
            with open(tmp_file, "wb") as fp:
                for chunk in resp.iter_content(chunk_size=8192):
                    fp.write(chunk)
                    sha256.update(chunk)
                    progress.update(task_id, advance=len(chunk))
            got = sha256.hexdigest()
            if got != entry.remote_sha256:
                raise ValueError(
                    f"SHA256 mismatch for {entry.file}: expected {entry.remote_sha256}, got {got}"
                )
            os.replace(tmp_file, dest)
    finally:
        progress.remove_task(task_id)
    return entry.file


@cache.command()
@click.option("-d", "--dir", "data_dir", default=None, help="Data directory (default: .iqb)")
@click.option("-f", "--force", is_flag=True, help="Re-download files with mismatched hashes")
@click.option("-j", "--jobs", default=8, show_default=True, help="Number of parallel downloads")
def pull(data_dir: str | None, force: bool, jobs: int) -> None:
    """Download missing cache files from the manifest."""
    resolved = data_dir_or_default(data_dir)
    manifest_path = manifest_path_for_data_dir(resolved)
    manifest = load_manifest(manifest_path)

    # Collect entries to download
    targets: list[DiffEntry] = []
    for entry in diff(manifest, resolved):
        if entry.state == DiffState.ONLY_REMOTE:
            targets.append(entry)
        elif entry.state == DiffState.SHA256_MISMATCH and force:
            targets.append(entry)

    if not targets:
        click.echo("Nothing to download.")
        return

    session = requests.Session()
    failed: list[tuple[str, str]] = []
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
            pool.submit(_download_one, entry, resolved, session, progress): entry
            for entry in targets
        }
        for future in as_completed(futures):
            entry = futures[future]
            try:
                future.result()
            except Exception as exc:
                failed.append((entry.file, str(exc)))
    elapsed = time.monotonic() - t0

    ok = len(targets) - len(failed)
    click.echo(f"Downloaded {ok}/{len(targets)} file(s) in {elapsed:.1f}s.")

    if failed:
        click.echo(f"{len(failed)} download(s) failed:", err=True)
        for file, reason in failed:
            click.echo(f"  {file}: {reason}", err=True)
        raise SystemExit(1)

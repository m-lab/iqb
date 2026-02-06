"""Cache status command."""

import click
from rich.console import Console

from ..ghremote import DiffState, diff, load_manifest, manifest_path_for_data_dir
from ..pipeline.cache import data_dir_or_default
from .cache import cache

_STATE_CHARS: dict[DiffState, tuple[str, str]] = {
    DiffState.ONLY_REMOTE: ("D", "red"),
    DiffState.SHA256_MISMATCH: ("M", "yellow"),
    DiffState.ONLY_LOCAL: ("A", "green"),
    DiffState.MATCHING: (" ", "dim"),
}


@cache.command()
@click.option("-d", "--dir", "data_dir", default=None, help="Data directory (default: .iqb)")
@click.option("-a", "--all", "show_all", is_flag=True, help="Include matching (unchanged) files")
def status(data_dir: str | None, show_all: bool) -> None:
    """Show cache status relative to the manifest.

    Each file path is prefixed with a status letter:

    \b
      'D'  needs download (in manifest, not on disk)
      'M'  modified (on disk, hash differs from manifest)
      'A'  added locally (on disk, not in manifest)

    Use `-a, --all` to see unmodified files as well, which are
    printed using the following status letter:

    \b
      ' '  not modifed (on dish, in cache, same hash)
    """
    resolved = data_dir_or_default(data_dir)
    manifest_path = manifest_path_for_data_dir(resolved)
    manifest = load_manifest(manifest_path)

    console = Console()
    for entry in diff(manifest, resolved):
        if entry.state == DiffState.MATCHING and not show_all:
            continue
        char, color = _STATE_CHARS[entry.state]
        console.print(f"[{color}]{char}[/] {entry.file}")

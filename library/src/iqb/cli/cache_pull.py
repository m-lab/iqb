"""Cache pull command."""

import click

from ..pipeline.cache import data_dir_or_default
from ..scripting import iqb_cache_pull
from .cache import cache


@cache.command()
@click.option("-d", "--dir", "data_dir", default=None, help="Data directory (default: .iqb)")
@click.option("-f", "--force", is_flag=True, help="Re-download files with mismatched hashes")
@click.option("-j", "--jobs", default=8, show_default=True, help="Number of parallel downloads")
@click.option(
    "--dataset", "datasets", multiple=True, help="Only pull entries for this dataset (repeatable)"
)
@click.option(
    "--file", "files", multiple=True, help="Only pull entries with this filename (repeatable)"
)
@click.option(
    "--after",
    default=None,
    help="Only pull entries starting on or after this date (YYYY-MM-DD). "
    "Required unless --before is given",
)
@click.option(
    "--before",
    default=None,
    help="Only pull entries starting before this date (YYYY-MM-DD). "
    "Required unless --after is given",
)
@click.pass_context
def pull(
    ctx: click.Context,
    data_dir: str | None,
    force: bool,
    jobs: int,
    datasets: tuple[str, ...],
    files: tuple[str, ...],
    after: str | None,
    before: str | None,
) -> None:
    """Download missing cache files from the manifest."""
    try:
        result = iqb_cache_pull.run(
            data_dir=data_dir,
            datasets=datasets,
            files=files,
            after=after,
            before=before,
            force=force,
            jobs=jobs,
        )
    except iqb_cache_pull.MissingDateBoundError as exc:
        raise click.UsageError(
            "The `iqb cache pull` subcommand needs a date range so that\n"
            "it doesn't download the entire remote cache.\n\n"
            "Please use the --after and/or --before flags as follows:\n\n"
            "  iqb cache pull --after 2024-01-01\n"
            "  iqb cache pull --before 2025-01-01 --after 2023-01-01",
            ctx,
        ) from exc

    if result is None:
        click.echo("Nothing to download.")
        return

    resolved = data_dir_or_default(data_dir)
    click.echo(f"Downloaded {result.ok}/{result.total} file(s) in {result.elapsed:.1f}s.")
    click.echo(f"Detailed logs: {result.log_file.relative_to(resolved)}")

    if result.failed:
        click.echo(f"{len(result.failed)} download(s) failed:", err=True)
        for file, reason in result.failed:
            click.echo(f"  {file}: {reason}", err=True)
        raise SystemExit(1)

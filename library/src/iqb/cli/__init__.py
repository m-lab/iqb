"""IQB command-line interface."""

from importlib.metadata import version

import click

_PACKAGE_NAME = "mlab-iqb"


def _get_version() -> str:
    """Return the installed package version string."""
    return version(_PACKAGE_NAME)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(message="%(version)s", package_name=_PACKAGE_NAME)
def cli() -> None:
    """Internet Quality Barometer (IQB) command-line tool."""


@cli.command(hidden=True)
def help() -> None:
    """Show usage information."""
    click.echo('Use "iqb --help" for usage information.')
    click.echo('Use "iqb <command> --help" for help on a specific command.')


@cli.command("version")
def version_cmd() -> None:
    """Print the version number."""
    click.echo(_get_version())


# Register subcommands (must be after cli is defined)
from . import cache as _cache  # noqa: E402, F401
from . import cache_pull as _cache_pull  # noqa: E402, F401
from . import cache_status as _cache_status  # noqa: E402, F401

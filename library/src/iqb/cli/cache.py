"""Cache command group."""

from . import cli


@cli.group()
def cache() -> None:
    """Manage the local data cache."""

"""Pipeline command group."""

from . import cli


@cli.group()
def pipeline() -> None:
    """Run data-generation pipelines."""

"""Pipeline run command."""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import click
import dacite
import yaml
from rich import get_console
from rich.panel import Panel

from .. import IQBPipeline
from ..pipeline.cache import data_dir_or_default
from ..scripting import iqb_exception, iqb_logging
from ..scripting.iqb_pipeline import Pipeline
from .pipeline import pipeline


@dataclass(frozen=True, kw_only=True)
class DateRange:
    start: str
    end: str


@dataclass(frozen=True, kw_only=True)
class PipelineMatrix:
    dates: list[DateRange]
    granularities: list[str]


@dataclass(frozen=True, kw_only=True)
class PipelineConfig:
    version: int
    matrix: PipelineMatrix


@dataclass(frozen=True, kw_only=True)
class _SinceFilter:
    value: str | None

    def is_active(self) -> bool:
        return self.value is not None

    def should_include(self, *, start: str) -> bool:
        return self.value is None or start >= self.value


def _create_since_filter(*, value: str | None) -> _SinceFilter:
    if value is not None:
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d")
            normalized = parsed.strftime("%Y-%m-%d")
            return _SinceFilter(value=normalized)
        except ValueError as exc:
            raise click.ClickException("The `--since` value must be like YYYY-MM-DD") from exc
    return _SinceFilter(value=value)


def load_pipeline_config(
    *,
    config_path: Path,
    since: _SinceFilter,
) -> tuple[list[tuple[str, str]], tuple[str, ...]]:
    """Load pipeline configuration matrix from YAML file."""

    def coerce_str(value: object) -> str:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, str):
            return value
        raise TypeError(f"Cannot coerce {type(value)} to str")

    try:
        content = config_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise click.ClickException(f"Pipeline config not found: {config_path}") from exc

    try:
        data = yaml.safe_load(content) or {}
    except yaml.YAMLError as exc:
        raise click.ClickException(f"Invalid YAML in {config_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise click.ClickException("Pipeline config must be a mapping.")

    try:
        config = dacite.from_dict(
            PipelineConfig,
            data,
            config=dacite.Config(type_hooks={str: coerce_str}),
        )
    except (dacite.DaciteError, TypeError) as exc:
        raise click.ClickException(f"Invalid pipeline config: {exc}") from exc

    if config.version != 0:
        raise click.ClickException(f"Unsupported pipeline config version: {config.version}")

    time_periods = [
        (entry.start, entry.end)
        for entry in config.matrix.dates
        if since.should_include(start=entry.start)
    ]
    if config.matrix.dates and since.is_active() and not time_periods:
        raise click.ClickException("The `--since` filter excluded all time periods.")
    if not time_periods:
        raise click.ClickException("Pipeline config matrix must include non-empty dates.")

    granularities = tuple(grain.strip() for grain in config.matrix.granularities)
    if not granularities or any(not grain for grain in granularities):
        raise click.ClickException("Pipeline config matrix must include non-empty granularities.")

    return time_periods, granularities


@pipeline.command()
@click.option("-d", "--dir", "data_dir", default=None, help="Data directory (default: .iqb)")
@click.option(
    "--file",
    "workflow_file",
    default=None,
    metavar="WORKFLOW",
    help="Path to YAML workflow file (default: <dir>/pipeline.yaml)",
)
@click.option(
    "-f",
    "--force",
    "force_bigquery",
    is_flag=True,
    default=False,
    help="Bypass all caches (local and remote) and re-query BigQuery.",
)
@click.option(
    "-n",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Just print the time periods and granularities that would be queried.",
)
@click.option(
    "--project",
    default="measurement-lab",
    envvar="IQB_PROJECT",
    show_default=True,
    help="BigQuery billing project (env: IQB_PROJECT).",
)
@click.option(
    "--since",
    "since",
    default=None,
    metavar="YYYY-MM-DD",
    help="Drop time periods starting before YYYY-MM-DD (e.g. `--since 2026-10-01` to query from 2026-10-01 onwards).",
)
@click.option("-v", "--verbose", is_flag=True, default=False, help="Verbose mode.")
def run(
    data_dir: str | None,
    dry_run: bool,
    workflow_file: str | None,
    force_bigquery: bool,
    project: str,
    since: str | None,
    verbose: bool,
) -> None:
    """Run the BigQuery pipeline for all matrix entries."""

    console = get_console()
    resolved_dir = data_dir_or_default(data_dir)
    workflow_path = Path(workflow_file) if workflow_file else resolved_dir / "pipeline.yaml"
    iqb_logging.configure(verbose=verbose)
    pipe = Pipeline(pipeline=IQBPipeline(project=project, data_dir=resolved_dir))
    time_periods, granularities = load_pipeline_config(
        config_path=workflow_path,
        since=_create_since_filter(value=since),
    )
    interceptor = iqb_exception.Interceptor()

    for grain in granularities:
        for start, end in time_periods:
            console.print(Panel(f"Sync {grain} data for {start} \u2192 {end}"))
            if dry_run:
                click.echo("Not running query in `-n/--dry-run` mode.")
                continue
            with interceptor:
                pipe.sync_mlab(
                    grain,
                    enable_bigquery=True,
                    start_date=start,
                    end_date=end,
                    force_bigquery=force_bigquery,
                )

    raise SystemExit(interceptor.exitcode())

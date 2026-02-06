"""Pipeline run command."""

# TODO(bassosimone): add support for -f/--force to bypass cache

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


def load_pipeline_config(
    config_path: Path,
) -> tuple[list[tuple[str, str]], tuple[str, ...]]:
    """Load pipeline configuration matrix from YAML file."""

    def coerce_str(value: object) -> str:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, str):
            return value
        raise TypeError(f"Cannot coerce {type(value)} to str")

    try:
        content = config_path.read_text()
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

    time_periods = [(entry.start, entry.end) for entry in config.matrix.dates]
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
@click.option("-v", "--verbose", is_flag=True, default=False, help="Verbose mode.")
def run(data_dir: str | None, workflow_file: str | None, verbose: bool) -> None:
    """Run the BigQuery pipeline for all matrix entries."""

    console = get_console()
    resolved_dir = data_dir_or_default(data_dir)
    workflow_path = Path(workflow_file) if workflow_file else resolved_dir / "pipeline.yaml"
    iqb_logging.configure(verbose=verbose)
    pipe = Pipeline(pipeline=IQBPipeline(project="measurement-lab", data_dir=resolved_dir))
    time_periods, granularities = load_pipeline_config(workflow_path)
    interceptor = iqb_exception.Interceptor()

    for grain in granularities:
        for start, end in time_periods:
            console.print(Panel(f"Sync {grain} data for {start} \u2192 {end}"))
            with interceptor:
                pipe.sync_mlab(
                    grain,
                    enable_bigquery=True,
                    start_date=start,
                    end_date=end,
                )

    raise SystemExit(interceptor.exitcode())

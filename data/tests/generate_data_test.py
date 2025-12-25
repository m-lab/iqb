from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def load_generate_data_module():
    module_path = Path(__file__).parents[1] / "generate_data.py"
    spec = importlib.util.spec_from_file_location("generate_data", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_load_pipeline_config_valid(tmp_path: Path):
    module = load_generate_data_module()
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        "\n".join(
            [
                "version: 0",
                "matrix:",
                "  dates:",
                "    - start: 2024-01-01",
                "      end: 2024-02-01",
                "  granularities:",
                "    - day",
                "    - week",
                "",
            ]
        )
    )

    time_periods, granularities = module.load_pipeline_config(config_path)

    assert time_periods == [("2024-01-01", "2024-02-01")]
    assert granularities == ("day", "week")


def test_load_pipeline_config_rejects_wrong_version(tmp_path: Path):
    module = load_generate_data_module()
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        "\n".join(
            [
                "version: 1",
                "matrix:",
                "  dates:",
                "    - start: 2024-01-01",
                "      end: 2024-02-01",
                "  granularities:",
                "    - day",
                "",
            ]
        )
    )

    with pytest.raises(module.click.ClickException, match="Unsupported pipeline config"):
        module.load_pipeline_config(config_path)


def test_load_pipeline_config_rejects_empty_dates(tmp_path: Path):
    module = load_generate_data_module()
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        "\n".join(
            [
                "version: 0",
                "matrix:",
                "  dates: []",
                "  granularities:",
                "    - day",
                "",
            ]
        )
    )

    with pytest.raises(module.click.ClickException, match="matrix must include non-empty"):
        module.load_pipeline_config(config_path)


def test_load_pipeline_config_rejects_blank_granularity(tmp_path: Path):
    module = load_generate_data_module()
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        "\n".join(
            [
                "version: 0",
                "matrix:",
                "  dates:",
                "    - start: 2024-01-01",
                "      end: 2024-02-01",
                "  granularities:",
                "    - day",
                '    - ""',
                "",
            ]
        )
    )

    with pytest.raises(module.click.ClickException, match="matrix must include non-empty"):
        module.load_pipeline_config(config_path)


def test_load_pipeline_config_rejects_non_mapping(tmp_path: Path):
    module = load_generate_data_module()
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text("- not-a-mapping")

    with pytest.raises(module.click.ClickException, match="must be a mapping"):
        module.load_pipeline_config(config_path)

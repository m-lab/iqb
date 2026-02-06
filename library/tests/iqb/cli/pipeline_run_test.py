"""Tests for the iqb.cli.pipeline_run module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner

from iqb.cli import cli

_VALID_CONFIG = {
    "version": 0,
    "matrix": {
        "dates": [
            {"start": "2024-10-01", "end": "2024-11-01"},
        ],
        "granularities": ["country"],
    },
}

_MULTI_CONFIG = {
    "version": 0,
    "matrix": {
        "dates": [
            {"start": "2024-10-01", "end": "2024-11-01"},
            {"start": "2024-11-01", "end": "2024-12-01"},
        ],
        "granularities": ["country", "continent"],
    },
}


def _write_config(path: Path, data: object) -> None:
    """Write a YAML config file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data))


class TestPipelineRunMissingWorkflow:
    """Missing workflow file produces an error."""

    def test_missing_file(self, tmp_path: Path):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["pipeline", "run", "-d", str(tmp_path)],
        )
        assert result.exit_code != 0
        assert "not found" in result.output


class TestPipelineRunInvalidYaml:
    """Invalid YAML produces an error."""

    def test_invalid_yaml(self, tmp_path: Path):
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(": : : bad yaml [[[")
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["pipeline", "run", "-d", str(tmp_path)],
        )
        assert result.exit_code != 0
        assert "Invalid YAML" in result.output or "Pipeline config" in result.output


class TestPipelineRunNonMapping:
    """Non-mapping YAML produces an error."""

    def test_non_mapping(self, tmp_path: Path):
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text("- not-a-mapping\n")
        runner = CliRunner()
        result = runner.invoke(cli, ["pipeline", "run", "-d", str(tmp_path)])
        assert result.exit_code != 0
        assert "must be a mapping" in result.output


class TestPipelineRunDaciteError:
    """Missing required fields produce a dacite error."""

    def test_missing_matrix(self, tmp_path: Path):
        _write_config(tmp_path / "pipeline.yaml", {"version": 0})
        runner = CliRunner()
        result = runner.invoke(cli, ["pipeline", "run", "-d", str(tmp_path)])
        assert result.exit_code != 0
        assert "Invalid pipeline config" in result.output


class TestPipelineRunWrongVersion:
    """Unsupported version produces an error."""

    def test_wrong_version(self, tmp_path: Path):
        _write_config(
            tmp_path / "pipeline.yaml",
            {
                "version": 99,
                "matrix": {
                    "dates": [{"start": "2024-10-01", "end": "2024-11-01"}],
                    "granularities": ["country"],
                },
            },
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["pipeline", "run", "-d", str(tmp_path)])
        assert result.exit_code != 0
        assert "Unsupported pipeline config version" in result.output


class TestPipelineRunEmptyDates:
    """Empty dates list produces an error."""

    def test_empty_dates(self, tmp_path: Path):
        _write_config(
            tmp_path / "pipeline.yaml",
            {"version": 0, "matrix": {"dates": [], "granularities": ["country"]}},
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["pipeline", "run", "-d", str(tmp_path)])
        assert result.exit_code != 0
        assert "non-empty dates" in result.output


class TestPipelineRunBlankGranularity:
    """Blank granularity produces an error."""

    def test_blank_granularity(self, tmp_path: Path):
        _write_config(
            tmp_path / "pipeline.yaml",
            {
                "version": 0,
                "matrix": {
                    "dates": [{"start": "2024-10-01", "end": "2024-11-01"}],
                    "granularities": ["country", ""],
                },
            },
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["pipeline", "run", "-d", str(tmp_path)])
        assert result.exit_code != 0
        assert "non-empty granularities" in result.output


class TestPipelineRunCoerceNonStringNonDate:
    """A non-string, non-date value in a string field produces a dacite error."""

    def test_integer_in_string_field(self, tmp_path: Path):
        # An integer where a string is expected triggers the TypeError branch
        # in coerce_str, which dacite wraps as a DaciteError
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(
            "version: 0\n"
            "matrix:\n"
            "  dates:\n"
            "    - start: 2024-10-01\n"
            "      end: 2024-11-01\n"
            "  granularities:\n"
            "    - 12345\n"
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["pipeline", "run", "-d", str(tmp_path)])
        assert result.exit_code != 0
        assert "Invalid pipeline config" in result.output


class TestPipelineRunDateCoercion:
    """Bare YAML dates (parsed as datetime.date) are coerced to strings."""

    @patch("iqb.cli.pipeline_run.IQBPipeline")
    @patch("iqb.cli.pipeline_run.Pipeline")
    def test_bare_dates(
        self, mock_pipeline_cls: MagicMock, mock_iqb_pipeline_cls: MagicMock, tmp_path: Path
    ):
        # YAML parses bare dates like 2024-10-01 as datetime.date objects
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(
            "version: 0\n"
            "matrix:\n"
            "  dates:\n"
            "    - start: 2024-10-01\n"
            "      end: 2024-11-01\n"
            "  granularities:\n"
            "    - country\n"
        )
        mock_pipeline = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline

        runner = CliRunner()
        result = runner.invoke(cli, ["pipeline", "run", "-d", str(tmp_path)])
        assert result.exit_code == 0
        mock_pipeline.sync_mlab.assert_called_once_with(
            "country",
            enable_bigquery=True,
            start_date="2024-10-01",
            end_date="2024-11-01",
        )


class TestPipelineRunValid:
    """Valid workflow with mocked sync_mlab exits 0."""

    @patch("iqb.cli.pipeline_run.IQBPipeline")
    @patch("iqb.cli.pipeline_run.Pipeline")
    def test_valid_config(
        self, mock_pipeline_cls: MagicMock, mock_iqb_pipeline_cls: MagicMock, tmp_path: Path
    ):
        _write_config(tmp_path / "pipeline.yaml", _VALID_CONFIG)
        mock_pipeline = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["pipeline", "run", "-d", str(tmp_path)],
        )
        assert result.exit_code == 0
        mock_pipeline.sync_mlab.assert_called_once_with(
            "country",
            enable_bigquery=True,
            start_date="2024-10-01",
            end_date="2024-11-01",
        )


class TestPipelineRunMultipleEntries:
    """Multiple granularities x date ranges produce multiple sync_mlab calls."""

    @patch("iqb.cli.pipeline_run.IQBPipeline")
    @patch("iqb.cli.pipeline_run.Pipeline")
    def test_multiple_entries(
        self, mock_pipeline_cls: MagicMock, mock_iqb_pipeline_cls: MagicMock, tmp_path: Path
    ):
        _write_config(tmp_path / "pipeline.yaml", _MULTI_CONFIG)
        mock_pipeline = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["pipeline", "run", "-d", str(tmp_path)],
        )
        assert result.exit_code == 0
        assert mock_pipeline.sync_mlab.call_count == 4


class TestPipelineRunSyncFailure:
    """sync_mlab raising exits with code 1."""

    @patch("iqb.cli.pipeline_run.IQBPipeline")
    @patch("iqb.cli.pipeline_run.Pipeline")
    def test_sync_failure(
        self, mock_pipeline_cls: MagicMock, mock_iqb_pipeline_cls: MagicMock, tmp_path: Path
    ):
        _write_config(tmp_path / "pipeline.yaml", _VALID_CONFIG)
        mock_pipeline = MagicMock()
        mock_pipeline.sync_mlab.side_effect = RuntimeError("BigQuery error")
        mock_pipeline_cls.return_value = mock_pipeline

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["pipeline", "run", "-d", str(tmp_path)],
        )
        assert result.exit_code == 1


class TestPipelineRunFileOverride:
    """--file overrides default path."""

    @patch("iqb.cli.pipeline_run.IQBPipeline")
    @patch("iqb.cli.pipeline_run.Pipeline")
    def test_file_override(
        self, mock_pipeline_cls: MagicMock, mock_iqb_pipeline_cls: MagicMock, tmp_path: Path
    ):
        custom_path = tmp_path / "custom" / "workflow.yml"
        _write_config(custom_path, _VALID_CONFIG)
        mock_pipeline = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["pipeline", "run", "-d", str(tmp_path), "--file", str(custom_path)],
        )
        assert result.exit_code == 0
        mock_pipeline.sync_mlab.assert_called_once()


class TestPipelineRunVerboseFlag:
    """-v flag is accepted."""

    @patch("iqb.cli.pipeline_run.IQBPipeline")
    @patch("iqb.cli.pipeline_run.Pipeline")
    def test_verbose_accepted(
        self, mock_pipeline_cls: MagicMock, mock_iqb_pipeline_cls: MagicMock, tmp_path: Path
    ):
        _write_config(tmp_path / "pipeline.yaml", _VALID_CONFIG)
        mock_pipeline = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["pipeline", "run", "-d", str(tmp_path), "-v"],
        )
        assert result.exit_code == 0

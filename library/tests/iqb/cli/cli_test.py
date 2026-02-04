"""Tests for the top-level iqb CLI (version, help)."""

from click.testing import CliRunner

from iqb.cli import cli


class TestCliVersionFlag:
    """--version prints just the version number."""

    def test_prints_version_number(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        # Should be just the version number, not "name, version X.Y.Z"
        assert "iqb" not in result.output.lower()
        assert result.output.strip() != ""


class TestCliVersionSubcommand:
    """iqb version prints just the version number."""

    def test_prints_version_number(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["version"])
        assert result.exit_code == 0
        assert result.output.strip() != ""

    def test_same_as_flag(self):
        runner = CliRunner()
        flag_result = runner.invoke(cli, ["--version"])
        cmd_result = runner.invoke(cli, ["version"])
        assert flag_result.output.strip() == cmd_result.output.strip()


class TestCliHelpSubcommand:
    """iqb help prints guidance to use --help."""

    def test_prints_guidance(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["help"])
        assert result.exit_code == 0
        assert "--help" in result.output

    def test_mentions_subcommand_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["help"])
        assert result.exit_code == 0
        assert "<command> --help" in result.output

"""Tests for the iqb.cli.cache_usage module."""

import json
from pathlib import Path

from click.testing import CliRunner

from iqb.cli import cli

_TS1 = "20241001T000000Z"
_TS2 = "20241101T000000Z"
_TS3 = "20241201T000000Z"


def _make_dataset(
    data_dir: Path,
    start: str,
    end: str,
    dataset: str,
    parquet_content: bytes = b"parquet-data",
    stats: dict[str, object] | None = None,
    *,
    skip_stats: bool = False,
) -> None:
    """Create a dataset directory with data.parquet and optionally stats.json."""
    ds_dir = data_dir / "cache" / "v1" / start / end / dataset
    ds_dir.mkdir(parents=True, exist_ok=True)
    (ds_dir / "data.parquet").write_bytes(parquet_content)
    if not skip_stats:
        if stats is None:
            stats = {"total_bytes_billed": 1000, "query_duration_seconds": 2.5}
        (ds_dir / "stats.json").write_text(json.dumps(stats))


class TestCacheUsageEmptyNoDir:
    """No cache/v1 directory at all."""

    def test_no_cached_data(self, tmp_path: Path):
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "No cached data found." in result.output


class TestCacheUsageEmptyV1:
    """cache/v1 directory exists but is empty."""

    def test_no_cached_data(self, tmp_path: Path):
        (tmp_path / "cache" / "v1").mkdir(parents=True)
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "No cached data found." in result.output


class TestCacheUsageSingleDataset:
    """Single dataset in a single period."""

    def test_shows_one_row(self, tmp_path: Path):
        _make_dataset(tmp_path, _TS1, _TS2, "downloads")
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "2024-10-01" in result.output
        assert "2024-11-01" in result.output
        assert "Total" in result.output
        assert "1" in result.output  # dataset count


class TestCacheUsageMultipleDatasetsOnePeriod:
    """Multiple datasets aggregated within one period."""

    def test_aggregated_row(self, tmp_path: Path):
        _make_dataset(
            tmp_path,
            _TS1,
            _TS2,
            "downloads",
            stats={"total_bytes_billed": 2000, "query_duration_seconds": 3.0},
        )
        _make_dataset(
            tmp_path,
            _TS1,
            _TS2,
            "uploads",
            stats={"total_bytes_billed": 1000, "query_duration_seconds": 1.5},
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "2024-10-01" in result.output
        # Should show count of 2 datasets
        assert "2" in result.output


class TestCacheUsageMultiplePeriods:
    """Multiple periods with separate rows."""

    def test_two_rows(self, tmp_path: Path):
        _make_dataset(tmp_path, _TS1, _TS2, "downloads")
        _make_dataset(tmp_path, _TS2, _TS3, "downloads")
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "2024-10-01" in result.output
        assert "2024-11-01" in result.output
        assert "2024-12-01" in result.output
        assert "Total" in result.output


class TestCacheUsageMissingStats:
    """Missing stats.json treated as zeros."""

    def test_zeros_for_missing_stats(self, tmp_path: Path):
        _make_dataset(tmp_path, _TS1, _TS2, "downloads", skip_stats=True)
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "2024-10-01" in result.output
        assert "0 B" in result.output
        assert "0s" in result.output


class TestCacheUsageCorruptStats:
    """Corrupt stats.json tolerated."""

    def test_corrupt_json(self, tmp_path: Path):
        ds_dir = tmp_path / "cache" / "v1" / _TS1 / _TS2 / "downloads"
        ds_dir.mkdir(parents=True)
        (ds_dir / "data.parquet").write_bytes(b"data")
        (ds_dir / "stats.json").write_text("this is not json!!!")
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "2024-10-01" in result.output
        assert "0 B" in result.output


class TestCacheUsageNullFields:
    """Null fields in stats.json treated as zeros."""

    def test_null_values(self, tmp_path: Path):
        _make_dataset(
            tmp_path,
            _TS1,
            _TS2,
            "downloads",
            stats={"total_bytes_billed": None, "query_duration_seconds": None},
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "0 B" in result.output
        assert "0s" in result.output


class TestCacheUsageLargeBytesBilled:
    """Exercise the PB fallthrough in _format_bytes."""

    def test_petabyte_range(self, tmp_path: Path):
        _make_dataset(
            tmp_path,
            _TS1,
            _TS2,
            "downloads",
            stats={"total_bytes_billed": 2 * 1024**5, "query_duration_seconds": 0},
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "PB" in result.output


class TestCacheUsageLongDuration:
    """Exercise the minutes branch in _format_duration."""

    def test_minutes_format(self, tmp_path: Path):
        _make_dataset(
            tmp_path,
            _TS1,
            _TS2,
            "downloads",
            stats={"total_bytes_billed": 0, "query_duration_seconds": 125.5},
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "2m" in result.output


class TestCacheUsageLockFileOnly:
    """Directory with only .lock file is skipped (no data.parquet)."""

    def test_lock_only_skipped(self, tmp_path: Path):
        ds_dir = tmp_path / "cache" / "v1" / _TS1 / _TS2 / "downloads"
        ds_dir.mkdir(parents=True)
        (ds_dir / ".lock").write_text("")
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "No cached data found." in result.output


class TestCacheUsageCustomDir:
    """Custom -d directory works."""

    def test_custom_dir(self, tmp_path: Path):
        custom = tmp_path / "mydata"
        custom.mkdir()
        _make_dataset(custom, _TS1, _TS2, "downloads")
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(custom)])
        assert result.exit_code == 0
        assert "2024-10-01" in result.output


class TestCacheUsageInvalidDirNames:
    """Invalid directory names are silently skipped."""

    def test_bad_start_dir(self, tmp_path: Path):
        bad_dir = tmp_path / "cache" / "v1" / "not-a-timestamp" / _TS2 / "downloads"
        bad_dir.mkdir(parents=True)
        (bad_dir / "data.parquet").write_bytes(b"data")
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "No cached data found." in result.output

    def test_bad_end_dir(self, tmp_path: Path):
        bad_dir = tmp_path / "cache" / "v1" / _TS1 / "bad-end" / "downloads"
        bad_dir.mkdir(parents=True)
        (bad_dir / "data.parquet").write_bytes(b"data")
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "No cached data found." in result.output

    def test_bad_dataset_name(self, tmp_path: Path):
        bad_dir = tmp_path / "cache" / "v1" / _TS1 / _TS2 / "BAD-NAME"
        bad_dir.mkdir(parents=True)
        (bad_dir / "data.parquet").write_bytes(b"data")
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "usage", "-d", str(tmp_path)])
        assert result.exit_code == 0
        assert "No cached data found." in result.output

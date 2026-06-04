"""Tests for the iqb.ghremote.entrypath module."""

import pytest

from iqb.ghremote.entrypath import ManifestEntryPath, parse_entry_path

_TS1 = "20241001T000000Z"
_TS2 = "20241031T235959Z"
_DATASET = "downloads"


class TestParseEntryPath:
    """Tests for parse_entry_path covering all validation branches."""

    def test_valid_data_parquet(self):
        result = parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/data.parquet")
        assert result == ManifestEntryPath(
            start=_TS1, end=_TS2, dataset=_DATASET, filename="data.parquet"
        )

    def test_valid_stats_json(self):
        result = parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/stats.json")
        assert result.filename == "stats.json"

    def test_too_few_components(self):
        with pytest.raises(ValueError, match="expected 6 path components"):
            parse_entry_path("cache/v1")

    def test_too_many_components(self):
        with pytest.raises(ValueError, match="expected 6 path components"):
            parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/extra/data.parquet")

    def test_wrong_first_component(self):
        with pytest.raises(ValueError, match="first component must be 'cache'"):
            parse_entry_path(f"notcache/v1/{_TS1}/{_TS2}/{_DATASET}/data.parquet")

    def test_wrong_second_component(self):
        with pytest.raises(ValueError, match="second component must be 'v1'"):
            parse_entry_path(f"cache/v2/{_TS1}/{_TS2}/{_DATASET}/data.parquet")

    def test_invalid_first_timestamp(self):
        with pytest.raises(ValueError, match="invalid start timestamp"):
            parse_entry_path(f"cache/v1/not-a-ts/{_TS2}/{_DATASET}/data.parquet")

    def test_invalid_second_timestamp(self):
        with pytest.raises(ValueError, match="invalid end timestamp"):
            parse_entry_path(f"cache/v1/{_TS1}/not-a-ts/{_DATASET}/data.parquet")

    def test_name_with_uppercase(self):
        with pytest.raises(ValueError, match="invalid dataset"):
            parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/Downloads/data.parquet")

    def test_name_with_hyphen(self):
        with pytest.raises(ValueError, match="invalid dataset"):
            parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/my-name/data.parquet")

    def test_name_with_underscore(self):
        result = parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/my_name/data.parquet")
        assert result.dataset == "my_name"

    def test_invalid_filename(self):
        with pytest.raises(ValueError, match="invalid filename"):
            parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/other.txt")

    def test_lock_file(self):
        with pytest.raises(ValueError, match="invalid filename"):
            parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/.lock")

    def test_empty_string(self):
        with pytest.raises(ValueError, match="expected 6 path components"):
            parse_entry_path("")


class TestManifestEntryPathStr:
    """Tests for __str__ round-tripping."""

    def test_round_trip(self):
        raw = f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/data.parquet"
        result = parse_entry_path(raw)
        assert str(result) == raw

    def test_round_trip_stats(self):
        raw = f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/stats.json"
        result = parse_entry_path(raw)
        assert str(result) == raw


class TestManifestEntryPathAsKey:
    """ManifestEntryPath must be usable as a dict key."""

    def test_hashable(self):
        path = parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/data.parquet")
        d = {path: "value"}
        assert d[path] == "value"

    def test_equal_instances_same_hash(self):
        a = parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/data.parquet")
        b = parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/data.parquet")
        assert a == b
        assert hash(a) == hash(b)

    def test_different_instances_different(self):
        a = parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/data.parquet")
        b = parse_entry_path(f"cache/v1/{_TS1}/{_TS2}/{_DATASET}/stats.json")
        assert a != b

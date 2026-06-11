"""Tests validating bulk IQB scores against the scalar calculator."""

import pandas as pd
import pytest

from iqb import IQB_DEFAULT_CONFIG, IQBCalculator, IQBData
from iqb.cache.mlab import IQBMetrics, MLabDataFramePair
from iqb.calculator.bulk import iqb_bulk_calculate_score_mlab
from iqb.calculator.calculator import _calculate_use_case_score

_PERCENTILES = [5, 25, 50, 75, 95]

_METRIC_BASES = [
    {"download": 100, "upload": 100, "latency": 5, "loss": 0.001},
    {"download": 1, "upload": 1, "latency": 200, "loss": 0.1},
    {"download": 50, "upload": 50, "latency": 150, "loss": 0.05},
    {"download": 5, "upload": 5, "latency": 5, "loss": 0.001},
    {"download": 25, "upload": 25, "latency": 100, "loss": 0.01},
    {"download": 30, "upload": 8, "latency": 60, "loss": 0.006},
]


def _make_test_dataframe() -> pd.DataFrame:
    """Build a synthetic merged DataFrame.

    Values scale by ``p / 95`` so that different percentile columns
    have different values, exercising the column-routing logic.
    """
    data: dict[str, list] = {
        "country_code": [f"C{i}" for i in range(len(_METRIC_BASES))],
        "city": [f"city_{i}" for i in range(len(_METRIC_BASES))],
    }
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        scale = p / 95.0
        for metric in ("download", "upload", "latency", "loss"):
            data[f"{metric}_p{p}"] = [row[metric] * scale for row in _METRIC_BASES]
    return pd.DataFrame(data)


def _make_test_data_frame_pair() -> MLabDataFramePair:
    """Build a synthetic MLabDataFramePair that merges to the same shape."""
    geo_cols = {
        "country_code": [f"C{i}" for i in range(len(_METRIC_BASES))],
        "city": [f"city_{i}" for i in range(len(_METRIC_BASES))],
    }

    dl_data: dict[str, list] = {**geo_cols, "sample_count": [100] * len(_METRIC_BASES)}
    ul_data: dict[str, list] = {**geo_cols, "sample_count": [100] * len(_METRIC_BASES)}

    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        scale = p / 95.0
        for _i, row in enumerate(_METRIC_BASES):
            for metric in ("download", "latency", "loss"):
                col = f"{metric}_p{p}"
                if col not in dl_data:
                    dl_data[col] = []
                dl_data[col].append(row[metric] * scale)
            col = f"upload_p{p}"
            if col not in ul_data:
                ul_data[col] = []
            ul_data[col].append(row["upload"] * scale)

    return MLabDataFramePair(
        download=pd.DataFrame(dl_data),
        upload=pd.DataFrame(ul_data),
    )


def _metrics_from_row(row: pd.Series, p: int) -> IQBMetrics:  # type: ignore[type-arg]
    dl: float = row[f"download_p{p}"]  # type: ignore[assignment]
    ul: float = row[f"upload_p{p}"]  # type: ignore[assignment]
    lat: float = row[f"latency_p{p}"]  # type: ignore[assignment]
    loss: float = row[f"loss_p{p}"]  # type: ignore[assignment]
    return IQBMetrics(download=dl, upload=ul, latency=lat, loss=loss)


def _scalar_iqb(row: pd.Series, p: int) -> float:  # type: ignore[type-arg]
    return IQBCalculator().calculate_iqb_score(IQBData(mlab=_metrics_from_row(row, p)))


def _scalar_use_case(row: pd.Series, p: int, uc_name: str) -> float:  # type: ignore[type-arg]
    metrics = _metrics_from_row(row, p)
    uc_cfg = IQB_DEFAULT_CONFIG.use_cases[uc_name]
    return _calculate_use_case_score(uc_cfg=uc_cfg, data=IQBData(mlab=metrics))


class TestBulkMatchesScalar:
    """Bulk vectorized scores must match the row-by-row scalar calculator."""

    def test_iqb_scores_match(self):
        df = _make_test_dataframe()
        result = iqb_bulk_calculate_score_mlab(
            data=df, config=IQB_DEFAULT_CONFIG, percentiles=_PERCENTILES
        )
        for p in _PERCENTILES:
            for idx in range(len(df)):
                expected = _scalar_iqb(df.iloc[idx], p)
                actual = result.iloc[idx][f"mlab_iqb_p{p}"]
                assert actual == pytest.approx(expected), (
                    f"row {idx}, p{p}: bulk={actual} != scalar={expected}"
                )

    def test_use_case_scores_match(self):
        df = _make_test_dataframe()
        result = iqb_bulk_calculate_score_mlab(
            data=df, config=IQB_DEFAULT_CONFIG, percentiles=_PERCENTILES
        )
        for p in _PERCENTILES:
            for idx in range(len(df)):
                row = df.iloc[idx]
                for uc_name in IQB_DEFAULT_CONFIG.use_cases:
                    expected = _scalar_use_case(row, p, uc_name)
                    col = f"mlab_{uc_name.replace(' ', '_')}_p{p}"
                    actual = result.iloc[idx][col]
                    assert actual == pytest.approx(expected), (
                        f"row {idx}, p{p}, {uc_name}: bulk={actual} != scalar={expected}"
                    )

    def test_original_columns_preserved(self):
        df = _make_test_dataframe()
        result = iqb_bulk_calculate_score_mlab(data=df, percentiles=[95])
        for col in df.columns:
            assert col in result.columns

    def test_does_not_mutate_input(self):
        df = _make_test_dataframe()
        cols_before = list(df.columns)
        iqb_bulk_calculate_score_mlab(data=df, percentiles=[50, 95])
        assert list(df.columns) == cols_before

    def test_default_percentile_is_95(self):
        df = _make_test_dataframe()
        result = iqb_bulk_calculate_score_mlab(data=df)
        assert "mlab_iqb_p95" in result.columns
        assert "mlab_iqb_p50" not in result.columns


class TestBulkAcceptsDataFramePair:
    """Verify that passing MLabDataFramePair produces the same results."""

    def test_pair_matches_raw_dataframe(self):
        df = _make_test_dataframe()
        pair = _make_test_data_frame_pair()

        result_df = iqb_bulk_calculate_score_mlab(data=df, percentiles=_PERCENTILES)
        result_pair = iqb_bulk_calculate_score_mlab(data=pair, percentiles=_PERCENTILES)

        score_cols = [c for c in result_df.columns if c.startswith("mlab_")]
        for col in score_cols:
            pd.testing.assert_series_equal(result_pair[col], result_df[col], check_names=False)

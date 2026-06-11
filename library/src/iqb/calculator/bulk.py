"""Module implementing bulk (vectorized) IQB score calculation for M-Lab data.

Computes IQB scores for all rows at multiple percentiles simultaneously.
Each row gets binary requirement scores, per-use-case scores, and an
overall IQB score appended as new columns.
"""

import dataclasses

import pandas as pd

from ..cache.mlab import MLabCacheEntry, MLabDataFramePair
from .config import (
    IQB_DEFAULT_CONFIG,
    IQBConfig,
)

_FIELD_TO_COL = {
    "download_throughput_mbps": "download",
    "upload_throughput_mbps": "upload",
    "latency_ms": "latency",
    "packet_loss": "loss",
}


def iqb_bulk_calculate_score_mlab(
    *,
    data: MLabCacheEntry | MLabDataFramePair | pd.DataFrame,
    config: IQBConfig = IQB_DEFAULT_CONFIG,
    percentiles: list[int] | None = None,
) -> pd.DataFrame:
    """Compute IQB scores for all rows at multiple percentiles using M-Lab data.

    Args:
        data: M-Lab measurement data in one of three forms:

            - ``MLabCacheEntry``: reads the data frame pair (may trigger
              a cache sync under an entry-level lock).
            - ``MLabDataFramePair``: merges download/upload into one DataFrame.
            - ``pd.DataFrame``: used directly (must have columns like
              ``download_p95``, ``upload_p95``, ``latency_p95``, ``loss_p95``).
        config: IQB configuration to use.
        percentiles: Which percentiles to compute (default ``[95]``).

    Returns:
        A copy of the input DataFrame with additional columns:

        - ``mlab_brs_{use_case}_{metric}_p{N}`` binary requirement scores (0/1)
        - ``mlab_{use_case}_p{N}`` use-case scores (weighted avg of BRS)
        - ``mlab_iqb_p{N}`` overall IQB score (weighted avg of use-case scores)
    """
    df = (
        data.read_data_frame_pair().to_merged_data_frame()
        if isinstance(data, MLabCacheEntry)
        else data.to_merged_data_frame()
        if isinstance(data, MLabDataFramePair)
        else data
    )
    percentiles = percentiles if percentiles is not None else [95]

    new_cols: dict[str, pd.Series] = {}

    for p in percentiles:
        uc_scores: dict[str, pd.Series] = {}

        for uc_name, uc_cfg in config.use_cases.items():
            nrs = uc_cfg.network_requirements
            weighted_sum = pd.Series(0.0, index=df.index)
            weight_total = 0.0
            uc_slug = uc_name.replace(" ", "_")

            for field in dataclasses.fields(nrs):
                nr_cfg = getattr(nrs, field.name)
                if nr_cfg is None:
                    continue

                col = f"{_FIELD_TO_COL[field.name]}_p{p}"
                brs = (
                    (df[col] > nr_cfg.threshold_min)
                    if nr_cfg.higher_is_better
                    else (df[col] < nr_cfg.threshold_min)
                ).astype(int)
                new_cols[f"mlab_brs_{uc_slug}_{_FIELD_TO_COL[field.name]}_p{p}"] = brs

                weighted_sum += brs * nr_cfg.weight
                weight_total += nr_cfg.weight

            uc_score = weighted_sum / weight_total
            new_cols[f"mlab_{uc_slug}_p{p}"] = uc_score
            uc_scores[uc_name] = uc_score

        iqb_num = pd.Series(0.0, index=df.index)
        iqb_den = 0.0
        for uc_name, uc_score in uc_scores.items():
            w = config.use_cases[uc_name].weight
            iqb_num += uc_score * w
            iqb_den += w
        new_cols[f"mlab_iqb_p{p}"] = iqb_num / iqb_den

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)

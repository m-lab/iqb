#!/usr/bin/env python3
"""Quick and dirty script to fetch more data."""

import sys
import logging
from pathlib import Path

import colorlog

# Add library to path so we can import iqb modules
sys.path.insert(0, str(Path(__file__).parent.parent / "library" / "src"))
from iqb import (
    IQBDatasetGranularity,
    IQBPipeline,
    iqb_dataset_name_for_mlab,
)
from iqb.ghremote import IQBGitHubRemoteCache, iqb_github_load_manifest
from iqb.pipeline.dataset import PipelineDatasetMLabTable  # XXX

if sys.stderr.isatty():
    LOG_COLORS = {
        "DEBUG": "bold_cyan",
        "INFO": "bold_green",
        "WARNING": "bold_yellow",
        "ERROR": "bold_red",
        "CRITICAL": "bold_red,bg_white",
    }
    handler = logging.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            fmt="%(log_color)s[%(asctime)s] <%(name)s> %(levelname)s:%(reset)s %(message)s",
            log_colors=LOG_COLORS,
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[handler],
    )


def doboth(
    pipeline: IQBPipeline,
    start_date: str,
    end_date: str,
    granularity: IQBDatasetGranularity,
):
    for table in (PipelineDatasetMLabTable.DOWNLOAD, PipelineDatasetMLabTable.UPLOAD):
        entry = pipeline.get_cache_entry(
            dataset_name=iqb_dataset_name_for_mlab(
                granularity=granularity,
                table=table,
            ),
            start_date=start_date,
            end_date=end_date,
        )
        with entry.lock():
            if not entry.exists():
                entry.sync()


def main():
    # Data directory is ./iqb/data (where this script lives)
    datadir = Path(__file__).parent
    manifest = iqb_github_load_manifest(datadir / "ghcache.json")
    rcache = IQBGitHubRemoteCache(manifest)

    pipeline = IQBPipeline(
        project="measurement-lab",
        data_dir=datadir,
        remote_cache=rcache,
    )

    # COUNTRY
    # 2025-12-23 quota allows to query this:
    doboth(pipeline, "2025-01-01", "2025-02-01", IQBDatasetGranularity.COUNTRY)
    doboth(pipeline, "2025-02-01", "2025-03-01", IQBDatasetGranularity.COUNTRY)
    doboth(pipeline, "2025-03-01", "2025-04-01", IQBDatasetGranularity.COUNTRY)

    # TODO in the next days:
    # doboth(pipeline, "2025-04-01", "2025-05-01", IQBDatasetGranularity.COUNTRY)
    # doboth(pipeline, "2025-05-01", "2025-06-01", IQBDatasetGranularity.COUNTRY)
    # doboth(pipeline, "2025-06-01", "2025-07-01", IQBDatasetGranularity.COUNTRY)
    # doboth(pipeline, "2025-07-01", "2025-08-01", IQBDatasetGranularity.COUNTRY)
    # doboth(pipeline, "2025-08-01", "2025-09-01", IQBDatasetGranularity.COUNTRY)
    # doboth(pipeline, "2025-09-01", "2025-10-01", IQBDatasetGranularity.COUNTRY)
    # doboth(pipeline, "2025-10-01", "2025-11-01", IQBDatasetGranularity.COUNTRY)
    # doboth(pipeline, "2025-11-01", "2025-12-01", IQBDatasetGranularity.COUNTRY)
    # doboth(pipeline, "2025-12-01", "2026-01-01", IQBDatasetGranularity.COUNTRY)


if __name__ == "__main__":
    main()

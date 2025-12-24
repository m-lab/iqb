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
    IQBDatasetMLabTable,
    IQBGitHubRemoteCache,
    IQBPipeline,
    iqb_dataset_name_for_mlab,
)

from iqb.scripting import iqb_logging


def sync_mlab(
    pipeline: IQBPipeline,
    start_date: str,
    end_date: str,
    granularity: IQBDatasetGranularity,
):
    for table in (IQBDatasetMLabTable.DOWNLOAD, IQBDatasetMLabTable.UPLOAD):
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
    iqb_logging.configure(verbose=True)

    # Data directory is ./iqb/data (where this script lives)
    datadir = Path(__file__).parent
    rcache = IQBGitHubRemoteCache(data_dir=datadir)

    pipeline = IQBPipeline(
        project="measurement-lab",
        data_dir=datadir,
        remote_cache=rcache,
    )

    logging.info("checking which entries to sync... start")

    # COUNTRY
    # 2025-12-23 quota allows to query this:
    # TODO(bassosimone): ensure we upload these files
    sync_mlab(pipeline, "2025-01-01", "2025-02-01", IQBDatasetGranularity.COUNTRY)
    sync_mlab(pipeline, "2025-02-01", "2025-03-01", IQBDatasetGranularity.COUNTRY)
    sync_mlab(pipeline, "2025-03-01", "2025-04-01", IQBDatasetGranularity.COUNTRY)

    # TODO in 2025-12-24
    sync_mlab(pipeline, "2025-04-01", "2025-05-01", IQBDatasetGranularity.COUNTRY)
    sync_mlab(pipeline, "2025-05-01", "2025-06-01", IQBDatasetGranularity.COUNTRY)
    # sync_mlab(pipeline, "2025-06-01", "2025-07-01", IQBDatasetGranularity.COUNTRY)

    # TODO in 2025-12-25
    # sync_mlab(pipeline, "2025-07-01", "2025-08-01", IQBDatasetGranularity.COUNTRY)
    # sync_mlab(pipeline, "2025-08-01", "2025-09-01", IQBDatasetGranularity.COUNTRY)
    # sync_mlab(pipeline, "2025-09-01", "2025-10-01", IQBDatasetGranularity.COUNTRY)

    # TODO in 2025-12-26
    # sync_mlab(pipeline, "2025-10-01", "2025-11-01", IQBDatasetGranularity.COUNTRY)
    # sync_mlab(pipeline, "2025-11-01", "2025-12-01", IQBDatasetGranularity.COUNTRY)
    # sync_mlab(pipeline, "2025-12-01", "2026-01-01", IQBDatasetGranularity.COUNTRY)

    logging.info("checking which entries to sync... ok")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Quick and dirty script to fetch more data."""

import sys
import logging
from pathlib import Path

# Add library to path so we can import iqb modules
sys.path.insert(0, str(Path(__file__).parent.parent / "library" / "src"))

from iqb import IQBPipeline
from iqb.scripting import iqb_logging, iqb_pipeline


def sync_mlab(
    pipeline: IQBPipeline,
    start_date: str,
    end_date: str,
    granularity: str,
):
    iqb_pipeline.sync_mlab(
        start_date=start_date,
        end_date=end_date,
        granularity=granularity,
        pipeline=pipeline,
    )


def main():
    # Data directory is ./iqb/data (where this script lives)
    datadir = Path(__file__).parent

    iqb_logging.configure(verbose=True)
    pipeline = iqb_pipeline.create(datadir)

    # COUNTRY
    # 2025-12-23 quota allows to query this:
    # TODO(bassosimone): ensure we upload these files
    pipeline.sync_mlab("country", start_date="2025-01-01", end_date="2025-02-01")
    pipeline.sync_mlab("country", start_date="2025-02-01", end_date="2025-03-01")
    pipeline.sync_mlab("country", start_date="2025-03-01", end_date="2025-04-01")

    # TODO in 2025-12-24
    pipeline.sync_mlab("country", start_date="2025-04-01", end_date="2025-05-01")
    pipeline.sync_mlab("country", start_date="2025-05-01", end_date="2025-06-01")
    # pipeline.sync_mlab("country", start_date="2025-06-01", end_date="2025-07-01")

    # TODO in 2025-12-25
    # pipeline.sync_mlab("country", start_date="2025-07-01", end_date="2025-08-01")
    # pipeline.sync_mlab("country", start_date="2025-08-01", end_date="2025-09-01")
    # pipeline.sync_mlab("country", start_date="2025-09-01", end_date="2025-10-01")

    # TODO in 2025-12-26
    # pipeline.sync_mlab("country", start_date="2025-10-01", end_date="2025-11-01")
    # pipeline.sync_mlab("country", start_date="2025-11-01", end_date="2025-12-01")
    # pipeline.sync_mlab("country", start_date="2025-12-01", end_date="2026-01-01")


if __name__ == "__main__":
    main()

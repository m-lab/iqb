#!/usr/bin/env python3
"""Execute a BigQuery query and save results to v1 Parquet cache."""

import argparse
import sys
from pathlib import Path

# Add library to path so we can import iqb modules
sys.path.insert(0, str(Path(__file__).parent.parent / "library" / "src"))

from iqb.scripting import iqb_exception, iqb_logging, iqb_pipeline

DEFAULT_PROJECT_ID = "measurement-lab"


def main():
    parser = argparse.ArgumentParser(
        description="Execute BigQuery query template and save results to v1 Parquet cache"
    )
    parser.add_argument(
        "--granularity",
        help="Geographical granularity (e.g., 'country', 'subdivision1', 'city')",
        default="country",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format (inclusive)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format (exclusive)",
    )
    parser.add_argument(
        "--project-id",
        default=DEFAULT_PROJECT_ID,
        help=f"GCP project ID for billing (default: {DEFAULT_PROJECT_ID})",
    )

    args = parser.parse_args()

    data_dir = Path(__file__).parent
    iqb_logging.configure(verbose=True)
    interceptor = iqb_exception.Interceptor()
    with interceptor:
        pipeline = iqb_pipeline.create(data_dir=data_dir, project=args.project_id)
        pipeline.sync_mlab(
            args.granularity,
            enable_bigquery=True,
            start_date=args.start_date,
            end_date=args.end_date,
        )

    sys.exit(interceptor.exitcode())


if __name__ == "__main__":
    main()

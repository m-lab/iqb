#!/usr/bin/env python3
"""Execute a BigQuery query and save results to v1 Parquet cache."""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add library to path so we can import iqb modules
sys.path.insert(0, str(Path(__file__).parent.parent / "library" / "src"))

from iqb.scripting import iqb_logging, iqb_pipeline

iqb_logging.configure(verbose=True)


def validate_date(date_str: str) -> str:
    """
    Validate date string is in YYYY-MM-DD format.

    Args:
        date_str: Date string to validate

    Returns:
        The validated date string

    Raises:
        ValueError: If date format is invalid
    """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError as e:
        raise ValueError(
            f"Invalid date format: {date_str} (expected YYYY-MM-DD)"
        ) from e


def run_bq_query(
    granularity: str,
    project_id: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Execute a BigQuery query and save to v1 Parquet cache.

    Date ranges follow Python slice convention [start, end):
    - start_date is inclusive
    - end_date is exclusive

    SQL templates should use: date >= '{START_DATE}' AND date < '{END_DATE}'
    (NOT BETWEEN, which is inclusive on both ends)

    Example: To query October 2024, pass:
      --start-date 2024-10-01 --end-date 2024-11-01
    Template becomes: date >= '2024-10-01' AND date < '2024-11-01'

    Args:
        query_name: Name of SQL query template (e.g., "downloads_by_country")
        project_id: GCP project ID for billing
        start_date: Start date in YYYY-MM-DD format (inclusive)
        end_date: End date in YYYY-MM-DD format (exclusive)
    """
    # Validate dates
    validate_date(start_date)
    validate_date(end_date)

    if start_date > end_date:
        raise ValueError(
            f"start_date must be <= end_date, got: {start_date} > {end_date}"
        )

    # Data directory is ./iqb/data (where this script lives)
    data_dir = Path(__file__).parent

    pipeline = iqb_pipeline.create(data_dir=data_dir, project=project_id)
    pipeline.sync_mlab(granularity, start_date=start_date, end_date=end_date)


def main():
    DEFAULT_PROJECT_ID = "measurement-lab"

    parser = argparse.ArgumentParser(
        description="Execute BigQuery query template and save results to v1 Parquet cache"
    )
    parser.add_argument(
        "--granularity",
        help="Geographical granularity (e.g., 'country', 'subdivision1', 'city')",
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

    run_bq_query(
        args.granularity,
        args.project_id,
        args.start_date,
        args.end_date,
    )


if __name__ == "__main__":
    main()

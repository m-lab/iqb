#!/usr/bin/env python3
"""Execute a BigQuery query and save results to a JSON file."""

import argparse
import subprocess
import sys
from datetime import datetime
from importlib.resources import files
from pathlib import Path

# Add library to path so we can import iqb.queries
sys.path.insert(0, str(Path(__file__).parent.parent / "library" / "src"))
import iqb.queries


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
    query_name: str,
    output_file: Path | None,
    project_id: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Execute a BigQuery query and save the JSON output.

    Query templates should contain {START_DATE} and {END_DATE} placeholders
    which will be replaced with the provided date values.

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
        output_file: Path where to save JSON output (None = stdout)
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

    print(f"Running query: {query_name}", file=sys.stderr)
    print(f"  Date range: {start_date} to {end_date}", file=sys.stderr)

    # Load query template from iqb.queries package
    query_file = files(iqb.queries).joinpath(f"{query_name}.sql")
    query = query_file.read_text()

    # Substitute template variables
    query = query.replace("{START_DATE}", start_date)
    query = query.replace("{END_DATE}", end_date)

    # Execute BigQuery command
    # stdout = data (JSON), stderr = logs
    cmd = [
        "bq",
        "query",
        "--use_legacy_sql=false",
        f"--project_id={project_id}",
        "--format=json",
        "--max_rows=10000",  # Override default limit of 100 rows
        query,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Print logs (stderr) to console
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        # Write data (stdout) to output file or stdout
        if output_file:
            with open(output_file, "w") as f:
                f.write(result.stdout)
            print(f"✓ Query completed: {output_file}", file=sys.stderr)
        else:
            # Output to stdout for piping
            print(result.stdout)

    except subprocess.CalledProcessError as e:
        print(f"✗ Query failed: {e}", file=sys.stderr)
        if e.stderr:
            print(e.stderr, file=sys.stderr)
        sys.exit(1)


def main():
    DEFAULT_PROJECT_ID = "measurement-lab"

    parser = argparse.ArgumentParser(
        description="Execute BigQuery query template and save results"
    )
    parser.add_argument(
        "query_name",
        help="Name of SQL query template (e.g., 'downloads_by_country', 'uploads_by_country')",
    )
    parser.add_argument(
        "-o", "--output", type=Path, help="Path to output JSON file (default: stdout)"
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
        args.query_name,
        args.output,
        args.project_id,
        args.start_date,
        args.end_date,
    )


if __name__ == "__main__":
    main()

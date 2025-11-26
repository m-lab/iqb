#!/usr/bin/env python3
"""Execute a BigQuery query and save results to a JSON file."""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


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
    query_file: Path,
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
        query_file: Path to SQL query template file
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

    print(f"Running query: {query_file}", file=sys.stderr)
    print(f"  Date range: {start_date} to {end_date}", file=sys.stderr)

    # Read query template
    with open(query_file) as f:
        query = f.read()

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
    # TODO(bassosimone): Should we use 'measurement-lab' as the project ID instead?
    # The web console (https://console.cloud.google.com/bigquery?project=measurement-lab)
    # uses measurement-lab as the project, so I am a bit unsure about what to use here.
    DEFAULT_PROJECT_ID = "mlab-sandbox"

    parser = argparse.ArgumentParser(
        description="Execute BigQuery query template and save results"
    )
    parser.add_argument("query_file", type=Path, help="Path to SQL query template file")
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

    if not args.query_file.exists():
        print(f"Error: Query file not found: {args.query_file}", file=sys.stderr)
        sys.exit(1)

    run_bq_query(
        args.query_file,
        args.output,
        args.project_id,
        args.start_date,
        args.end_date,
    )


if __name__ == "__main__":
    main()

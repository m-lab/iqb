#!/usr/bin/env python3
"""Execute a BigQuery query and save results to a JSON file."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

# Add library to path so we can import iqb modules
sys.path.insert(0, str(Path(__file__).parent.parent / "library" / "src"))
from iqb.pipeline import IQBPipeline


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

    This uses IQBPipeline internally to:
    1. Execute the query and save to ./data/cache/v1/{start}/{end}/{query_name}/
       - data.parquet: Query results
       - stats.json: Query metadata (timing, bytes processed, template hash)
    2. Convert the parquet to JSON format
    3. Write JSON to the output file (for backward compatibility with v0 pipeline)

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

    # Data directory is ./iqb/data (where this script lives)
    data_dir = Path(__file__).parent

    # Step 1: Get or create cache entry
    # This creates: ./iqb/data/cache/v1/{start}/{end}/{query_name}/
    #   - data.parquet: query results (empty file if no results)
    #   - stats.json: query metadata
    # fetch_if_missing=True makes this idempotent: skips query if cache exists
    pipeline = IQBPipeline(project_id=project_id, data_dir=data_dir)
    entry = pipeline.get_cache_entry(
        template=query_name,
        start_date=start_date,
        end_date=end_date,
        fetch_if_missing=True,
    )
    data_path = entry.data_parquet_file_path()
    assert data_path.exists()
    stats_path = entry.stats_json_file_path()
    assert stats_path.exists()
    print(f"✓ Cache entry: {data_path.parent.name}", file=sys.stderr)
    print(f"  Data: {data_path}", file=sys.stderr)
    print(f"  Stats: {stats_path}", file=sys.stderr)

    # Step 2: Convert the parquet file to JSON
    print("Converting parquet to JSON...", file=sys.stderr)
    table = pq.read_table(data_path)
    records = table.to_pylist()

    # Check if query returned no results
    if len(records) <= 0:
        print("⚠ Query returned no results", file=sys.stderr)

    json_output = json.dumps(records, indent=2)

    # Step 3: Write JSON to output file or stdout
    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(json_output)
        print(f"✓ JSON saved: {output_file} ({len(records)} records)", file=sys.stderr)
    else:
        # Output to stdout for piping
        print(json_output)


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

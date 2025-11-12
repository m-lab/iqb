#!/usr/bin/env python3
"""Execute a BigQuery query and save results to a JSON file."""

import argparse
import subprocess
import sys
from pathlib import Path


def run_bq_query(query_file: Path, output_file: Path | None, project_id: str) -> None:
    """
    Execute a BigQuery query and save the JSON output.

    Args:
        query_file: Path to SQL query file
        output_file: Path where to save JSON output (None = stdout)
        project_id: GCP project ID for billing
    """
    print(f"Running query: {query_file}", file=sys.stderr)

    # Read query
    with open(query_file) as f:
        query = f.read()

    # Execute BigQuery command
    # stdout = data (JSON), stderr = logs
    cmd = [
        "bq",
        "query",
        "--use_legacy_sql=false",
        f"--project_id={project_id}",
        "--format=json",
        query,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Write data (stdout) to output file or stdout
        if output_file:
            with open(output_file, "w") as f:
                f.write(result.stdout)
            print(f"✓ Query completed: {output_file}", file=sys.stderr)
        else:
            # Output to stdout for piping
            print(result.stdout)

        # Print logs (stderr) to console
        if result.stderr:
            print(result.stderr, file=sys.stderr)

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
        description="Execute BigQuery query and save results"
    )
    parser.add_argument("query_file", type=Path, help="Path to SQL query file")
    parser.add_argument(
        "-o", "--output", type=Path, help="Path to output JSON file (default: stdout)"
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

    run_bq_query(args.query_file, args.output, args.project_id)


if __name__ == "__main__":
    main()

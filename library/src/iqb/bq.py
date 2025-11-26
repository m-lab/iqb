"""Fetch data from BigQuery and persist as Parquet files.

Design Rationale
----------------

This module implements a content-addressable storage system for BigQuery query
results, optimized for IQB measurement data analysis.

Content-Addressable Directory Structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Query results are stored in a hierarchical directory structure where the path
itself encodes the query parameters::

    {cache_root}/{start_date}T00:00:00Z/{end_date}T00:00:00Z/{aggregation}/
        downloads.parquet
        uploads.parquet

Example::

    data/cache/v1/2024-10-01T00:00:00Z/2024-11-01T00:00:00Z/country/
        downloads.parquet  # Country-level download metrics
        uploads.parquet    # Country-level upload metrics

This design provides:

1. **Explicit time boundaries**: Using ISO 8601 timestamps with "T00:00:00Z"
   suffix makes the time range unambiguous. The start date is inclusive, the
   end date is exclusive (Python slice convention).

2. **Natural cache key**: The directory path IS the cache key. No need for
   separate hash computation or manifest files.

3. **Human-readable**: You can inspect the cache by browsing the filesystem
   and immediately understand what data is stored.

4. **Supports custom time ranges**: Any time period can be represented without
   changing code (e.g., "2024-10-15T00:00:00Z/2024-11-15T00:00:00Z" for
   mid-month ranges).

Why Parquet Format
~~~~~~~~~~~~~~~~~~

Based on empirical measurements with real IQB data (October 2024):

- **Country-level query** (236 rows): 0.06 MB compressed Parquet
- **Country+city+ASN query** (99,516 rows, 1 day): 20.75 MB compressed Parquet
- **Country+city+ASN query** (estimated 3M rows, 1 month): ~600 MB compressed

Parquet provides:

1. **Compression**: 4-10× smaller than JSON for percentile data
2. **Columnar access**: Read only needed columns (e.g., just p95 values)
3. **Predicate pushdown**: Filter by country/city/ASN without loading full file
4. **Memory efficiency**: Row groups allow processing data in chunks
5. **Standard format**: Works with pandas, DuckDB, PyArrow, Polars

Alternative formats considered:

- **JSON**: 3-6× larger, must parse entire file, slower for large datasets
- **CSV**: Poor schema support, no compression, slower parsing
- **Custom sharding**: Row groups provide equivalent memory benefits with less complexity

Memory Characteristics
~~~~~~~~~~~~~~~~~~~~~~

Parquet files use row groups (typically 64 MB uncompressed by default). When
reading with filters::

    df = pd.read_parquet(
        "downloads.parquet",
        filters=[("country", "==", "US")],
        columns=["country", "city", "download_p95"]
    )

PyArrow:
1. Reads file metadata (row group statistics)
2. Identifies row groups that may contain matching rows
3. Decompresses only matching row groups (one at a time)
4. Applies filters and returns matching rows

Peak memory usage = largest row group size (uncompressed) + result DataFrame.

For our largest dataset (country_city_asn, ~600 MB compressed), this means:
- Peak memory: ~100-200 MB per row group during processing
- Much more efficient than loading entire 600 MB file into memory
"""

from pathlib import Path
from google.cloud import bigquery
from importlib.resources import files
import iqb.queries


def _load_query_template(name: str) -> str:
    """
    Load SQL query template from package resources.

    Args:
        name: Query template name (e.g., "downloads_by_country_city_asn")

    Returns:
        SQL query string with {START_DATE} and {END_DATE} placeholders
    """
    query_file = files(iqb.queries).joinpath(f"{name}.sql")
    return query_file.read_text()


def _execute_query(
    query: str, output_file: Path, project_id: str
) -> dict[str, int]:
    """
    Execute BigQuery query and save results as Parquet file.

    Args:
        query: BigQuery SQL query string
        output_file: Path where to save Parquet file
        project_id: GCP project ID for billing

    Returns:
        Metadata dict with keys: rows, bytes_processed, bytes_billed, file_size_bytes
    """
    client = bigquery.Client(project=project_id)

    print(f"Executing query for {output_file.name}...")
    query_job = client.query(query)
    result_iter = query_job.result()

    print(f"Query complete")
    print(f"  Bytes processed: {query_job.total_bytes_processed:,}")
    print(f"  Bytes billed: {query_job.total_bytes_billed:,}")

    # Convert to DataFrame and write as Parquet
    df = result_iter.to_dataframe()

    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(
        output_file,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    file_size = output_file.stat().st_size
    print(f"✓ Saved {len(df):,} rows to {output_file}")
    print(f"  File size: {file_size / 1024 / 1024:.2f} MB")

    return {
        "rows": len(df),
        "bytes_processed": query_job.total_bytes_processed,
        "bytes_billed": query_job.total_bytes_billed,
        "file_size_bytes": file_size,
    }


def iqb_bigquery_fetch(
    aggregation: str,
    start_date: str,
    end_date: str,
    cache_root: Path,
    project_id: str = "mlab-sandbox",
) -> dict[str, dict[str, int]]:
    """
    Fetch aggregated NDT measurement data from BigQuery and save as Parquet.

    Queries both downloads and uploads tables, computes percentiles at the
    specified aggregation level, and saves results in content-addressable
    directory structure organized by time period.

    Directory structure created:
        {cache_root}/{start_date}T00:00:00Z/{end_date}T00:00:00Z/{aggregation}/
            downloads.parquet
            uploads.parquet

    Args:
        aggregation: Aggregation level, one of:
            - "country": Country-level aggregates
            - "country_asn": Per-ASN within each country
            - "country_province": Per-province within each country
            - "country_province_asn": Per-ASN within each province
            - "country_city": Per-city within each country
            - "country_city_asn": Per-ASN within each city
        start_date: Start date in YYYY-MM-DD format (inclusive)
        end_date: End date in YYYY-MM-DD format (exclusive)
        cache_root: Root directory for cache (e.g., "data/cache/v1")
        project_id: GCP project ID for BigQuery billing

    Returns:
        Metadata dict with keys "downloads" and "uploads", each containing:
            - rows: Number of rows returned
            - bytes_processed: Bytes scanned by BigQuery
            - bytes_billed: Bytes billed by BigQuery
            - file_size_bytes: Size of output Parquet file

    Raises:
        ValueError: If aggregation is not a recognized level
        google.auth.exceptions.DefaultCredentialsError: If GCP credentials not found
        google.api_core.exceptions.GoogleAPIError: If BigQuery query fails

    Example:
        >>> from iqb import iqb_bigquery_fetch
        >>> from pathlib import Path
        >>> metadata = iqb_bigquery_fetch(
        ...     aggregation="country_city_asn",
        ...     start_date="2025-10-01",
        ...     end_date="2025-11-01",
        ...     cache_root=Path("data/cache/v1"),
        ... )
        >>> print(f"Downloads: {metadata['downloads']['rows']:,} rows")
        >>> print(f"Cost: ${metadata['downloads']['bytes_billed'] / 1e12 * 6.25:.2f}")
    """
    # Validate aggregation level
    valid_aggregations = [
        "country",
        "country_asn",
        "country_province",
        "country_province_asn",
        "country_city",
        "country_city_asn",
    ]
    if aggregation not in valid_aggregations:
        raise ValueError(
            f"Invalid aggregation: {aggregation}. Must be one of {valid_aggregations}"
        )

    # Create content-addressable directory structure
    # Format: {cache_root}/YYYY-MM-DDT00:00:00Z/YYYY-MM-DDT00:00:00Z/{aggregation}/
    start_timestamp = f"{start_date}T00:00:00Z"
    end_timestamp = f"{end_date}T00:00:00Z"
    output_dir = cache_root / start_timestamp / end_timestamp / aggregation

    print(f"\n{'='*60}")
    print(f"Fetching aggregation: {aggregation}")
    print(f"Time period: [{start_date}, {end_date})")
    print(f"Output directory: {output_dir}")
    print(f"{'='*60}\n")

    # Fetch downloads and uploads
    results = {}
    for metric in ["downloads", "uploads"]:
        # Load query template
        template_name = f"{metric}_by_{aggregation}"
        template = _load_query_template(template_name)

        # Substitute parameters
        query = template.replace("{START_DATE}", start_date)
        query = query.replace("{END_DATE}", end_date)

        # Execute query and save
        output_file = output_dir / f"{metric}.parquet"
        results[metric] = _execute_query(query, output_file, project_id)

    print(f"\n{'='*60}")
    print(f"✓ Completed: {aggregation}")
    print(f"  Downloads: {results['downloads']['rows']:,} rows")
    print(f"  Uploads: {results['uploads']['rows']:,} rows")
    print(f"  Total size: {(results['downloads']['file_size_bytes'] + results['uploads']['file_size_bytes']) / 1024 / 1024:.2f} MB")
    print(f"{'='*60}\n")

    return results

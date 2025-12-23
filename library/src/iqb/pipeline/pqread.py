"""Module to efficiently read parquet tables."""

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


def iqb_parquet_read(
    filepath: Path,
    *,
    country_code: str | None = None,
    asn: int | None = None,
    subdivision1: str | None = None,
    city: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Efficiently read data from a parquet table.

    The filtering is efficient because it happens while reading the physical
    parquet file from disk using PyArrow filter pushdown.

    Arguments:
        filepath: path to the parquet file
        country_code: optionally filter for equality with the given country code.
        asn: optionally filter for equality with the given ASN.
        subdivision1: optionally filter for equality with the given subdivision1 name.
        city: optionally filter for equality with the given city name.
        columns: optionally retain only the selected columns.

    Returns:
        A pandas DataFrame containing the filtered data.

    Raises:
        FileNotFoundError if the requested file does not exist.
        ValueError if one or more of the requested columns do not exist.
    """

    # 1. setup the reading filters to efficiently skip groups of rows
    # PyArrow filters: list of tuples (column, operator, value)
    filters = []
    if asn is not None:
        filters.append(("asn", "=", asn))
    if city is not None:
        filters.append(("city", "=", city))
    if country_code is not None:
        filters.append(("country_code", "=", country_code))
    if subdivision1 is not None:
        filters.append(("subdivision1_name", "=", subdivision1))

    # 2. load in memory using the filters and potentially cutting the columns
    # Note: PyArrow requires filters=None (not []) when there are no filters
    table = pq.read_table(
        filepath,
        filters=filters if filters else None,
        columns=columns,
    )

    # 3. finally convert to data frame
    return table.to_pandas()

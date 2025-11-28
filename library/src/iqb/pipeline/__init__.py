"""Package for populating the IQB-measurement-data cache.

The IQBPipeline class runs BigQuery queries and populates the cache.

Data Directory Convention
-------------------------

If a data directory is specified, we use it. Otherwise, we use `.iqb` in
the current directory. This is similar to git, that uses `.git`.

Design
------

We store files named after the following pattern:

    $datadir/cache/v1/$since/$until/$query_type/data.parquet
    $datadir/cache/v1/$since/$until/$query_type/stats.json

Each query result is stored in a directory containing the data file (data.parquet)
and the query metadata (stats.json). The stats file records query execution details
such as start time (RFC3339 format with Z suffix), duration, and bytes processed
for transparency and debugging

The `$since` and `$until` variables are timestamps using the RFC3339 format
with UTC timezone, formatted to be file-system friendly (i.e., without
including the ":" character). For example: `20251126T100000Z`.

The `$since` timestamp is included and the `$until` one is excluded. This
simplifies specifying time ranges significantly (e.g., October 2025 is
represented using `since=20251001T000000Z` and `until=20251101T000000Z`).

Note that when using BigQuery the second component of the path will always
be `T000000Z` because we do not support hourly range queries for now.

The fact that we use explicit timestamps allows the cache to contain any
kind of time range, including partially overlapping ones. This content-
addressable approach means the time range IS the path identifier, eliminating
the need for metadata files, cache invalidation logic, or naming conventions
to prevent conflicts. Overlapping queries coexist naturally without coordination.

The $query_type is one of the following query granularity types:

1. country_$kind
2. country_asn_$kind
3. country_province_$kind
4. country_province_asn_$kind
5. country_city_$kind
6. country_city_asn_$kind

Where $kind is either "download" or "upload". The final implementation
of this design will have all the required queries implemented.

For each query type, we have a corresponding directory containing the data
and metadata files.

We use parquet as the file format because:

1. we can stream to it when writing BigQuery queries results

2. regardless of the file size, we can always process and filter it in
chunks since parquet divides rows into independent groups

We store the raw results of queries for further processing, since the
query itself is the expensive operation while further elaborations
are comparatively cheaper, and can be done locally with PyArrow/Pandas.

Specifically, reading from the cache (when implemented in cache.py) will
use PyArrow's predicate pushdown for efficient filtering:

1. Use PyArrow's filters parameter to skip row groups at I/O level:
   pq.read_table(path, filters=[('country', '=', 'US')], columns=['...'])

2. Convert to Pandas DataFrame only after filtering for analysis

This approach ensures we can select a country (and other properties when
needed, e.g., the ASN or city) and skip row groups that do not match, while
immediately projecting out columns we don't need. If processing the parquet-
dumped raw query outputs is fast enough, we can directly access the data
without producing intermediate formats.
"""

from .pipeline import IQBPipeline

__all__ = ["IQBPipeline"]

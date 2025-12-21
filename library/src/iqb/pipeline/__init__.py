"""Package for populating the IQB-measurement-data cache.

The `IQBPipeline` class runs BigQuery queries and populates the cache.

The `PipelineCacheManager` class, which is only meant to be used
within the `iqb` library`, allows to read/write query entries.

The `iqb_dataset_name_for_{project}` family of functions allow to
derive the correct dataset name for a given project.

The `iqb_parquet_read` function allows to efficiently read and
filter data inside an arbitrary parquet file.

Cache Spec Ownership
--------------------

Because this package *writes* the cache, it also owns and specifies
the on-disk cache data structure. The cache package, which reads the
cache, conforms to the expected data structure by using code in
this package that hides the specific implementation details.

Remote Cache Support
--------------------

The pipeline supports an optional remote cache layer that sits between the
local cache and BigQuery. When provided, the pipeline will attempt to fetch
missing cache entries from the remote cache before executing expensive
BigQuery queries.

The remote cache is implemented as a Protocol (see `pipeline.RemoteCache`)
that requires a `sync(entry: PipelineCacheEntry) -> bool` method. This
design allows pluggable remote cache implementations (e.g., GCS, GitHub
releases, S3) without coupling the pipeline to specific storage backends.

Cache lookup order:
1. Local disk cache (fast, free)
2. Remote cache if provided (medium speed, cheap)
3. BigQuery query (slow, expensive)

This architecture enables sharing pre-computed query results across team
members and CI/CD environments, significantly reducing BigQuery costs and
query execution time for common datasets.

Data Directory Convention
-------------------------

If a data directory is specified, we use it. Otherwise, we use `.iqb` in
the current directory. This is similar to git, that uses `.git`.

On-Disk Format
--------------

We store files named after the following pattern:

    $datadir/cache/v1/{since}/{until}/{dataset}/data.parquet
    $datadir/cache/v1/{since}/{until}/{dataset}/stats.json

Each query result is stored in a directory containing the data file (data.parquet)
and the query metadata (stats.json). The stats file records query execution details
such as start time (RFC3339 format with Z suffix), duration, and bytes processed
for transparency and debugging

The `{since}` and `{until}` are placeholders for timestamps using
the RFC3339 format with UTC timezone, formatted so to be file-system
friendly (i.e., without including the ":" character). For example,
`20251126T100000Z` is a valid value for `{since}` or `{until}`.

The `{since}` timestamp is included and the `{until}` one is excluded. This
simplifies specifying time ranges significantly (e.g., October 2025 is
represented using `since=20251001T000000Z` and `until=20251101T000000Z`).

The *current* implementation of the pipline enforces YYYY-MM-DD dates
because the underlying queries only support dates. Yet, we design the data
format to accommodate for more fine grained time intervals in the future.

Note that when using BigQuery the second component of the path will always
be `T000000Z` because we do not support hourly range queries for now.

The fact that we use explicit timestamps allows the cache to contain any
kind of time range, including partially overlapping ones. This content-
addressable approach means the time range IS the path identifier, eliminating
the need for metadata files, cache invalidation logic, or naming conventions
to prevent conflicts. Overlapping queries coexist naturally without coordination.

The `{dataset}` identifies the specific dataset we are using. In general, the
structure of the placeholder depends upon how the data is actually organized and
there is no fixed pattern. However, there are functions exported by this
package using the `iqb_dataset_name_for_` prefix that allow generating a valid
dataset value for a valid project value. For example, see, the
`iqb_dataset_name_for_mlab` function.

On disk, each `{dataset}` is a valid directory name containing the `data.parquet`
file and the `stats.json` file (and possibly other files).

We use parquet as the file format because:

1. we can stream the result of BigQuery queries into parquet files

2. regardless of the file size, we can always process and filter it in
chunks since parquet divides rows into independent groups

We store the raw results of queries for further processing, since the
query itself is the expensive operation while further elaborations
are comparatively cheaper, and can be done locally with PyArrow/Pandas.

This package also implements efficiently filtering a parquet file
from disk using PyArrow predicate pushdown for efficient filtering
and column selection. See `iqb_parquet_read`.

Using `iqb_parquet_read` ensures we can select a country (and other properties
when needed, e.g., the ASN or city) and skip row groups that do not match, while
immediately projecting out columns we don't need.
"""

from .dataset import (
    IQBDatasetGranularity,
    iqb_dataset_name_for_mlab,
)
from .pipeline import IQBPipeline
from .pqread import iqb_parquet_read

__all__ = [
    "IQBDatasetGranularity",
    "IQBPipeline",
    "iqb_dataset_name_for_mlab",
    "iqb_parquet_read",
]

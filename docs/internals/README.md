# IQB Internals

This directory documents the internal architecture of the IQB data
pipeline. It is organized as a sequence of chapters that build on
each other.

## Chapters

### Chapter 0 — [Queries](00-queries.md)
How and why we query BigQuery, percentile semantics, polarity
normalization, and SQL template naming conventions.

### Chapter 1 — [Pipeline](01-pipeline.ipynb)
Using `IQBPipeline` to run queries against BigQuery and store
results as local parquet files. Covers cache entry lifecycle,
locking, stats, and direct parquet reading.

### Chapter 2 — [Pipeline Remote Cache](02-pipeline-cache.ipynb)
Using `IQBGitHubRemoteCache` to fetch pre-computed results from
GitHub releases without BigQuery access. Manifest format and
cache layout.

### Chapter 3 — [Researcher API](../../analysis/00-template.ipynb)
The read-only `IQBCache` and `IQBCalculator` APIs are documented
in the analysis template notebook. That notebook shows how to load
cached data, select percentiles, and compute IQB scores end-to-end.

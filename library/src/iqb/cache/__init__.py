"""Package for fetching IQB measurement data from cache.

The IQBCache component manages local caching of IQB measurement data, following
a Git-like convention for storing local state.

Cache Directory Convention
---------------------------
By default, IQBCache looks for a `.iqb/` directory in the current working
directory, similar to how Git uses `.git/` for repository state. This provides:

- Clear ownership (`.iqb/` contains IQB-specific data)
- Per-project isolation (each project has its own cache directory)
- Conventional pattern (like `.cache/`, `.config/`, etc.)

⚠️  PERCENTILE INTERPRETATION (CRITICAL!)
----------------------------------------

For "higher is better" metrics (throughput):
  - Raw p95 = "95% of users have ≤ 625 Mbit/s speed"
  - Directly usable: download_p95 ≥ threshold?
  - No inversion needed (standard statistical definition)

For "lower is better" metrics (latency, packet loss):
  - Raw p95 = "95% of users have ≤ 80ms latency" (worst-case typical)
  - We want p95 to represent best-case typical (to match throughput)
  - Solution: Use p5 raw labeled as p95
  - Mathematical inversion: p(X)_labeled = p(100-X)_raw
  - Example: OFFSET(5) raw → labeled as "latency_p95" in JSON

This inversion happens in BigQuery (see data/query_*.sql),
so this cache code treats all percentiles uniformly.

When you request percentile=95, you get the 95th percentile value
that can be compared uniformly against thresholds.

NOTE: This creates semantics where p95 represents "typical best
performance" - empirical validation will determine if appropriate.

Design Note
-----------

We are going with separate pipelines producing data for separate
data sources, since this scales well incrementally.

Additionally, note how the data is relatively small regardless
of the time window that we're choosing (it's always four metrics
each of which contains between 25 and 100 percentiles). So,
computationally, gluing together N datasets in here will never
become an expensive operation.

Example usage
-------------

    # Uses .iqb/ in current directory
    cache = IQBCache()

    # Or specify custom location
    cache = IQBCache(data_dir="/shared/iqb-cache")
"""

from .cache import IQBCache

__all__ = ["IQBCache"]

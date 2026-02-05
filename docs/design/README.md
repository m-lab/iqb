# Design Documents

This directory contains design documents that capture architectural
decisions, requirements analyses, and technical evaluations. Each
document is dated and records the reasoning at the time it was written,
similar to an RFC or Architecture Decision Record.

- [2025-11-14-geodata.md](2025-11-14-geodata.md): BigQuery geodata field
fill rates. Key finding: city is 90% filled, subdivision1 is 93% filled
-- simple `GROUP BY` queries work, no spatial operations needed.

- [2025-11-24-cache.md](2025-11-24-cache.md): Timestamp-based
content-addressable cache with Parquet storage. Path-as-key design,
why Parquet (with benchmarks), percentile polarity normalization,
and `APPROX_QUANTILES` non-determinism measurements.

- [2025-12-21-remote.md](2025-12-21-remote.md): Three-layer cache lookup
(local → remote → BigQuery) and Protocol-based pluggability. Why
`ghremote` exists and how it evolved from GitHub Releases to GCS.

- [2026-01-20-distribution.md](2026-01-20-distribution.md): Requirements
for IQB data distribution (R1--R8) and why GCS object storage satisfies
them. Covers data structure, manifest design, and same-region egress.

- [2026-01-29-sync.md](2026-01-29-sync.md): Why ThreadPoolExecutor over
async or a download service, and why JSONL spans instead of
OpenTelemetry for download diagnostics.

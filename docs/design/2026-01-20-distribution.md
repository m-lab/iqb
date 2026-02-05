# IQB Data Distribution

**Date:** 2026-01-20
**Status:** Implemented (GCS bucket `mlab-sandbox-iqb-us-central1`, PR #131)

## Requirements

The distribution system must satisfy:

1. **Individual file addressability.** Users download specific parquet
   files without fetching the entire dataset.
2. **Incremental downloads.** A user interested in country-level data
   for October 2025 should not download city-level data or other months.
3. **Incremental uploads.** New months are appendable without
   re-uploading existing data.
4. **Data updates with versioning.** Methodology changes produce new
   version namespaces (`cache/v1/`, `cache/v2/`).
5. **Data integrity.** SHA256 checksums for every file.
6. **Discovery.** A programmatic manifest listing available files,
   checksums, and sizes.
7. **Reasonable performance.** Individual file downloads in 1--10 seconds
   (files range from 58 KB to 47 MB).
8. **Standard tooling.** Accessible via HTTP clients and cloud SDKs
   without custom software.

## Data Structure

The IQB cache contains ~1.4 GB across 156 parquet files (plus 156
`stats.json` metadata files), covering 13 months at 6 aggregation levels:

```
cache/v1/{start_date}/{end_date}/{metric}_by_{aggregation}/
  ├─ data.parquet    (58 KB to 47 MB)
  └─ stats.json      (~260 bytes, query metadata)
```

| Aggregation Level | Typical Size |
|-------------------|--------------|
| `by_country` | 58 KB |
| `by_country_subdivision1` | 558 KB |
| `by_country_asn` | 8.8 MB |
| `by_country_subdivision1_asn` | 11 MB |
| `by_country_city` | 15 MB |
| `by_country_city_asn` | 42--47 MB |

## Decision: GCS Bucket

GCS replaced the original GitHub Releases backend (see
[2025-12-21-remote.md](2025-12-21-remote.md)) that served as the
first remote cache implementation. The `ghremote` package was
repurposed from "GitHub Remote" to "globally-hosted remote", keeping
its name and Protocol interface unchanged.

GCS object storage satisfies all eight requirements:

- **Individual files and incremental access (R1--R3):** Objects map 1:1
  to cache files. New months are uploaded independently.
- **Versioning (R4):** The `cache/v1/` path prefix provides version
  namespaces. GCS object versioning preserves history if needed.
- **Integrity (R5):** GCS stores MD5/CRC32C checksums; the manifest
  additionally provides SHA256.
- **Discovery (R6):** Bucket listing API and manifest file.
- **Performance (R7):** Optimized for this workload. The IQB prototype
  runs on Cloud Run in the same GCS region, so `iqb cache pull` from
  the prototype incurs no cross-region egress.
- **Standard tooling (R8):** `gsutil`, `gcloud`, HTTP, Python cloud SDKs.

The `cache/v1/` directory structure is preserved as-is in the bucket,
so local paths and GCS paths are identical.

## Manifest

File discovery uses a centralized manifest:

```json
{
  "files": {
    "cache/v1/{period}/{aggregation}/data.parquet": {
      "sha256": "3a421c62...",
      "url": "https://storage.googleapis.com/mlab-sandbox-iqb-us-central1/..."
    }
  },
  "v": 0
}
```

The manifest provides a point-in-time snapshot of available files.
Updating it atomically ensures users never see a partial state. SHA256
checksums in the manifest enable integrity verification independently
of the storage backend.

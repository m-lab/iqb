## IQB Queries

This document does not delve into the details of the IQB formula, rather
it aims to introduce you to why and how we query the data to produce input
to feed into the IQB formula.

### What the queries do

Each query aggregates ndt7 measurements over a time window, groups them
by geography (and optionally ASN), and computes percentile summaries.

Downloads (including latency and loss) come from `measurement-lab.ndt.unified_downloads`.

Uploads come from `measurement-lab.ndt.unified_uploads`.

The queries filter out rows with missing geo or metric fields and then compute
the same percentile set (p1, p5, ..., p50, ..., p95, p99) for each metric (download
speed, upload speed, minimum RTT, and packet loss rate).

### Why percentiles

The IQB principle is simple: pick a percentile, compare those metric
values to quality thresholds, and then aggregate across requirements and
use cases. Percentiles let us talk about "most users" vs "few users"
in a concrete, repeatable way.

### Concrete example

Consider for example this `ndt7` snapshot of IQB metrics only including
the 5, 50, and 95 percentiles (2024-10; US; country level):

|                   | p5         | p50    | p95         |
| ----------------- | ---------- | ------ | ----------- |
| Download (Mbit/s) | 2.74       | 96.35  | **625.69**  |
| Upload (Mbit/s)   | 0.15       | 20.95  | **370.48**  |
| Latency (ms)      | **0.806**  | 16.13  | 80.584      |
| Packet Loss Rate  | **0.0000** | 0.0005 | 0.1202      |

**Table 1**: Raw percentile metrics from actual data. Values in bold are
the ones we would pick according to the IQB report. Keep on reading to
understand the reason behind picking them.

This is the data you would (approximately) get if you'd run the query
yourself using BigQuery and the "unified" tables.

### Polarity and "top 5%" rule

Using IQB means selecting a given percentile slice and then using it
inside of the IQB formula. It should be noted that, for properly selecting
a slice we need to harmonize the meaning of percentiles except for the
median.

More specifically, consider this: for download and upload "higher is better"
while for latency and packet losses "lower is better". So, assuming our goal
is to stick to the report's interpretation ("top 5% performance"), then
we would select:

- for higher-is-better metrics (download/upload): use p95 to represent
  "top 5% performance"

- for lower-is-better metrics (latency/loss): use p5 to represent
  "top 5% performance"

As a result, we would pick the numbers in bold in Table 1. And then we will
plug these values in the IQB formula.

### Uniform representation in the queries

To make this easier to use downstream, the queries normalize the
percentile representation so the "top 5% performance" slice is always
expressed as p95. So, the *actual* table looks like this:

|                   | p5      | p50    | p95         |
| ----------------- | ------- | ------ | ----------- |
| Download (Mbit/s) | 2.74    | 96.35  | **625.69**  |
| Upload (Mbit/s)   | 0.15    | 20.95  | **370.48**  |
| Latency (ms)      | 80.584  | 16.13  | **0.806**   |
| Packet Loss Rate  | 0.1202  | 0.0005 | **0.0000**  |

**Table 2**: Metrics with uniform representation. Values in bold are
the ones we would pick according to the IQB report. See how now the
pick boils down to selecting a column in a columnar database.

Note how the percentiles in the two bottom rows have been swapped for p5
and p95, so that 95p corresponds to "top 5% performance".

### Where the queries live

The SQL templates are in `library/src/iqb/queries/` and are named by
metric, geography, and optional ASN granularity, for example:

- `downloads_by_country.sql`
- `downloads_by_country_asn.sql`
- `downloads_by_country_city.sql`
- `downloads_by_country_city_asn.sql`
- `downloads_by_country_subdivision1.sql`
- `downloads_by_country_subdivision1_asn.sql`

Upload queries follow the same naming pattern.

### Granularity and ASN variants

We need distinct geographic granularities because we want to explore the
scores for distinct areas, with and without ASN attribution.

For the upload and the download speeds we support these granularities:

1. `country` and `country` with `asn`;

2. `subdivision1` and `subdivision1` with `asn`;

3. `city` and `city` with `asn`.

We organize data so that it is possible to navigate up and down from
the granularities hierarchy.

For example, you can start with a country view, then drill into the
same country via `country_asn` to see ASNs, or via `subdivision1` to see
regional detail, because subdivision-level rows still carry the country
code and can be filtered to that country. From there you can go further
down to `city` or `subdivision1_asn` using the same filters.

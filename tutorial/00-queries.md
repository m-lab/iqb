## IQB Queries

This document does not delve into the details of the IQB formula, rather
aims to introduce you to why and how we query the data to produce input
to feed into the IQB formula.

### Why percentiles

The IQB principle is simple: pick a percentile, compare those metric
values to quality thresholds, and then aggregate across requirements and
use cases. Percentiles let us talk about "most users" vs "few users"
in a concrete, repeatable way.

### Concrete example

Consider for example this `ndt7` snapshot of IQB metrics only including
the 5, 50, and 95 percentile data points (2024-10 US country level):

|                   | p5      | p50    | p95     |
| ----------------- | ------- | ------ | ------- |
| Download (Mbit/s) | 2.74    | 96.35  | 625.69  |
| Upload (Mbit/s)   | 0.15    | 20.95  | 370.48  |
| Latency (ms)      | 0.806   | 16.13  | 80.584  |
| Packet Loss Rate  | 0.0000  | 0.0005 | 0.1202  |

**Table 1**: Raw percentile metrics from actual data.

This is the data you would (approximately) get if you'd run the query
youself using BigQuery and the `unified-downloads` table.

### Polarity and "top 5%" rule

So, using IQB means selecting a given percentile slice and then using it
inside of the IQB formula. It should be noted that the percentiles we
provide here are actual percentiles, however, for properly selecting a
slice we need to harmonize the meaning of percentiles except for the
median, where there's no need to do so.

This happens because for download and upload "higher is better" while
for latency and packet losses "lower is better". So, assuming our goal
is to stick to the report's interpretation ("top 5% performance"), then
we would select:

Rule of thumb:

- for higher-is-better metrics (download/upload): use p95 to represent
  "top 5% performance"

- for lower-is-better metrics (latency/loss): use p5 to represent
  "top 5% performance"

As a result, we are going to pick these numbers:

1. 625 Mbit/s meaning that 5% of users have that speed or better

2. 370 Mbit/s meaning that 5% of users have that speed or better

3. 0.806 ms meaning that 5% of users have that latency or better

4. 0 meaning that 5% of users have that packet loss rate or better

Then we would plug this into the IQB formula.

### Uniform representation in the queries

To make this easier to use downstream, the queries normalize the
percentile representation so the "top 5% performance" slice is always
expressed as p95.

In practical terms, for simplicity, the queries that IQB uses already
flip the percentiles to give them uniform meaning. So, the actual
table that you fetch from IQB looks like this *instead*:

|                   | p5      | p50    | p95     |
| ----------------- | ------- | ------ | ------- |
| Download (Mbit/s) | 2.74    | 96.35  | 625.69  |
| Upload (Mbit/s)   | 0.15    | 20.95  | 370.48  |
| Latency (ms)      | 80.584  | 16.13  | 0.806   |
| Packet Loss Rate  | 0.1202  | 0.0005 | 0.0000  |

**Table 2**: Metrics with uniform representation.

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

# IQB User Guide

## Intended Audiences

This guide is written for four primary audiences:

- **Consumers** — individuals who want to understand how well their Internet
  connection performs for everyday tasks.
- **Policymakers** — government officials, regulators, and public interest
  groups evaluating broadband quality at a regional or national level.
- **Researchers** — academics and data scientists studying Internet
  performance trends and methodologies.
- **Internet Service Providers (ISPs)** — operators who want to understand
  how their network performance is measured and contextualized.

---

## What Is IQB?

The Internet Quality Barometer (IQB) is an open-source framework developed
by [Measurement Lab (M-Lab)](https://www.measurementlab.net/) that produces
a composite quality score for Internet connections.

Traditional "speed tests" measure a single number — how fast data moves — but
that number alone does not tell you whether a connection is suitable for video
conferencing, online gaming, or cloud backup. Different applications have
different network requirements.

IQB addresses this by evaluating a connection across six use cases:

| Use Case | Primary Concern |
|----------|----------------|
| Web browsing | Download speed, low latency |
| Video streaming | Sustained download speed |
| Audio streaming | Consistent download speed, low packet loss |
| Video conferencing | Symmetric speed, very low latency and packet loss |
| Online backup | Upload speed |
| Gaming | Extremely low latency, very low packet loss |

For each use case, IQB tests whether the connection meets a minimum threshold
for each relevant network requirement. The results are combined into a single
score between 0 and 1.

---

## How the IQB Score Is Calculated

The IQB score is calculated in three steps.

**Step 1 — Binary requirement check.** For each use case, IQB checks whether
a measured value meets the minimum required threshold:

- For download and upload throughput, the connection must exceed the
  threshold (more is better).
- For latency and packet loss, the connection must be below the threshold
  (less is better).

A requirement is scored 1 (pass) or 0 (fail) per dataset.

**Step 2 — Requirement agreement score.** When multiple measurement datasets
are active, their pass/fail results are averaged. This reduces the influence
of any single dataset's measurement conditions.

**Step 3 — Weighted aggregation.** Requirement scores within a use case are
combined using requirement-specific weights. Use case scores are then combined
using use-case weights. The final IQB score is a weighted average across all
active use cases.

A score of 1.0 means the connection meets all thresholds for all use cases.
A score of 0.0 means it fails every threshold. In practice most connections
score somewhere in between.

---

## Understanding Network Metrics

### Latency (Round-Trip Time)

Latency is the time it takes for a data packet to travel from your device to
a server and back, measured in milliseconds (ms). Lower is better.

- Under 50 ms: excellent for all use cases including gaming and video calls
- 50–100 ms: acceptable for most tasks; may cause noticeable delay in gaming
- Over 100 ms: may cause interruptions in real-time applications

Latency is determined by the physical distance to the server, congestion in
the network, and the quality of routing.

### Throughput (Download and Upload Speed)

Throughput is the rate at which data is transferred, measured in megabits per
second (Mbps). Higher is better.

- **Download** — data received by your device (web pages, video streams)
- **Upload** — data sent from your device (video conference streams, backups)

The minimum threshold varies by use case. Video conferencing requires
symmetric speeds (25 Mbps in each direction), while web browsing is
satisfied by lower thresholds (10 Mbps download).

### Packet Loss

Packet loss is the percentage of data packets that are sent but never
received. Lower is better; ideally 0%.

Even small amounts of packet loss (0.5–1%) can cause visible degradation in
video calls and online gaming. Audio and video streaming may buffer or stall.

---

## Interpreting Percentile Charts

IQB scores and measurements are typically presented as percentile
distributions, not single points.

A percentile chart shows what fraction of measured connections achieve a
given speed or score. For example:

- **50th percentile (p50)** — the median. Half of connections perform above
  this value, half below.
- **10th percentile (p10)** — 10% of connections perform at or below this
  level; represents the experience of the worst-served users.
- **90th percentile (p90)** — 90% of connections perform at or below this
  level; represents the best-served majority.

When evaluating a region or ISP, the 10th percentile is often the most
policy-relevant number: it characterises the experience of users who are
most poorly served, not the average or the best case.

---

## Comparing ISPs Responsibly

IQB measurements do not rank ISPs as simply better or worse. Several
factors affect interpretation:

**Coverage and sample size.** ISPs serving dense urban areas accumulate more
test samples than those serving rural areas. Smaller sample sizes produce
less stable estimates. Always check the sample count before drawing
conclusions from comparisons.

**Geography.** Latency depends heavily on where a server is located relative
to users. An ISP that routes traffic locally may show lower latency than one
that backhaults traffic over long distances, regardless of underlying network
quality.

**Test methodology.** M-Lab NDT measurements are conducted under specific
conditions. They reflect the performance experienced by users who run the
test, which may not represent all traffic or all times of day.

**Time periods.** Compare measurements taken within the same time window.
Infrastructure upgrades or network events can cause significant changes
between periods.

---

## Why Sample Size Matters

A single measurement taken at one moment gives limited information. IQB
aggregates measurements from many users over a calendar month to produce
stable estimates. However:

- Areas with fewer than a few hundred measurements in a period should be
  interpreted with caution.
- A jurisdiction with 100,000 tests per month provides far more reliable
  estimates than one with 200.
- The dashboard indicates sample counts where available. Treat low-sample
  results as indicative, not conclusive.

---

## Limitations of Internet Performance Measurements

IQB is designed to be transparent about what it can and cannot measure.

**Measurement conditions are not controlled.** Tests measure performance at
the moment a user runs them. Results can vary by time of day, congestion on
the local loop, the user's device, and the location of the test server.

**IQB reflects a specific set of use cases.** The current framework covers
six use cases. A connection might perform well for web browsing but poorly
for gaming. The composite score is useful for general comparisons but does
not capture every dimension of quality.

**Thresholds reflect current standards, not absolutes.** The thresholds used
to determine pass/fail for each requirement are based on current application
requirements. As applications evolve — for example, 4K video streaming
requires higher download speeds than 1080p — thresholds may need revision.

**IQB does not measure network neutrality or congestion throttling.** The
score reflects observed performance, not the underlying causes. Poor scores
may result from infrastructure limitations, congestion, or other factors
unrelated to deliberate network management.

**Data availability varies by region.** Countries with lower M-Lab test
deployment or lower user participation will have less comprehensive
measurement coverage.

---

## Further Reading

- M-Lab blog post: [Introducing IQB](https://www.measurementlab.net/blog/iqb/)
- IQB framework report: [Detailed Report (PDF)](https://www.measurementlab.net/publications/IQB_report_2025.pdf)
- IQB executive summary: [Executive Summary (PDF)](https://www.measurementlab.net/publications/IQB_executive_summary_2025.pdf)
- IQB poster (ACM IMC 2025): [arXiv](https://arxiv.org/pdf/2509.19034)
- Live prototype: [iqb.mlab-staging.measurementlab.net](https://iqb.mlab-staging.measurementlab.net/)

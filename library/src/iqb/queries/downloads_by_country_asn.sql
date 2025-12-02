SELECT
    client.Geo.CountryCode as country_code,
    client.Network.ASNumber as asn,
    client.Network.ASName as as_name,
    COUNT(*) as sample_count,

    -- ============================================================================
    -- PERCENTILE LABELING CONVENTION FOR IQB QUALITY ASSESSMENT
    -- ============================================================================
    --
    -- For "higher is better" metrics (throughput):
    --   - Raw p95 = "95% of users have ≤ X Mbit/s"
    --   - Label: OFFSET(95) → download_p95 (standard statistical definition)
    --   - Interpretation: top ~5% of users have > p95 throughput
    --
    -- For "lower is better" metrics (latency, packet loss):
    --   - Raw p95 = "95% of users have ≤ X ms latency" (worst-case typical)
    --   - We want p95 to represent best-case typical (to match throughput semantics)
    --   - Solution: Invert labels - use raw p5 labeled as p95
    --   - Label: OFFSET(5) → latency_p95 (inverted!)
    --   - Interpretation: top ~5% of users (best latency) have < p95
    --
    -- Result: Uniform comparison logic where p95 always means "typical best
    -- performance" rather than "typical worst performance"
    --
    -- NOTE: This creates semantics where checking p95 thresholds asks
    -- "Can the top ~5% of users perform this use case?" - empirical validation
    -- against real data will determine if this interpretation is appropriate.
    -- ============================================================================

    -- Download throughput (higher is better - NO INVERSION)
    -- Standard percentile labels matching statistical definition
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(1)] as download_p1,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(5)] as download_p5,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(10)] as download_p10,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(25)] as download_p25,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(50)] as download_p50,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(75)] as download_p75,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(90)] as download_p90,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(95)] as download_p95,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(99)] as download_p99,

    -- Latency/MinRTT (lower is better - INVERTED LABELS!)
    -- ⚠️  OFFSET(99) = worst latency = top 1% worst users → labeled as p1
    -- ⚠️  OFFSET(5) = 5th percentile = best ~5% of users → labeled as p95
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(99)] as latency_p1,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(95)] as latency_p5,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(90)] as latency_p10,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(75)] as latency_p25,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(50)] as latency_p50,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(25)] as latency_p75,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(10)] as latency_p90,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(5)] as latency_p95,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(1)] as latency_p99,

    -- Packet Loss Rate (lower is better - INVERTED LABELS!)
    -- ⚠️  OFFSET(99) = worst loss = top 1% worst users → labeled as p1
    -- ⚠️  OFFSET(5) = 5th percentile = best ~5% of users → labeled as p95
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(99)] as loss_p1,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(95)] as loss_p5,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(90)] as loss_p10,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(75)] as loss_p25,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(50)] as loss_p50,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(25)] as loss_p75,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(10)] as loss_p90,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(5)] as loss_p95,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(1)] as loss_p99
FROM
    -- TODO(bassosimone): switch to union tables `measurement-lab.ndt.ndt7_union`
    -- when they have been blessed as the new stable tables.
    `measurement-lab.ndt.unified_downloads`
WHERE
    date >= "{START_DATE}" AND date < "{END_DATE}"
    AND client.Geo.CountryCode IS NOT NULL
    AND client.Network.ASNumber IS NOT NULL
    AND client.Network.ASName IS NOT NULL
    AND a.MeanThroughputMbps IS NOT NULL
    AND a.MinRTT IS NOT NULL
    AND a.LossRate IS NOT NULL
GROUP BY country_code, asn, as_name
ORDER BY country_code, asn, as_name

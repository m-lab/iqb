SELECT
    client.Geo.CountryCode as country_code,
    COUNT(*) as sample_count,

    -- ============================================================================
    -- PERCENTILE LABELING CONVENTION FOR IQB QUALITY ASSESSMENT
    -- ============================================================================
    --
    -- Upload throughput is "higher is better", so we use standard percentile
    -- labels (no inversion).
    --
    -- See query_downloads.sql for detailed explanation and rationale.
    -- ============================================================================

    -- Upload throughput (higher is better - NO INVERSION)
    -- Standard percentile labels matching statistical definition
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(1)] as upload_p1,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(5)] as upload_p5,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(10)] as upload_p10,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(25)] as upload_p25,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(50)] as upload_p50,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(75)] as upload_p75,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(90)] as upload_p90,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(95)] as upload_p95,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(99)] as upload_p99
FROM
    `measurement-lab.ndt.unified_uploads`
WHERE
    date >= "{START_DATE}" AND date < "{END_DATE}"
    AND client.Geo.CountryCode IN ("US", "DE", "BR")
    AND a.MeanThroughputMbps IS NOT NULL
GROUP BY country_code
ORDER BY country_code

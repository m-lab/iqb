SELECT
    client.Geo.CountryCode as country_code,
    client.Geo.city as city,
    client.Network.ASNumber as asn,
    client.Network.ASName as asn_name,
    COUNT(*) as sample_count,

    -- Download throughput percentiles (higher is better - NO INVERSION)
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(1)] as download_p1,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(5)] as download_p5,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(10)] as download_p10,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(25)] as download_p25,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(50)] as download_p50,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(75)] as download_p75,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(90)] as download_p90,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(95)] as download_p95,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100)[OFFSET(99)] as download_p99,

    -- Latency percentiles (lower is better - INVERTED LABELS)
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(99)] as latency_p1,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(95)] as latency_p5,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(90)] as latency_p10,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(75)] as latency_p25,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(50)] as latency_p50,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(25)] as latency_p75,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(10)] as latency_p90,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(5)] as latency_p95,
    APPROX_QUANTILES(a.MinRTT, 100)[OFFSET(1)] as latency_p99,

    -- Packet loss percentiles (lower is better - INVERTED LABELS)
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(99)] as loss_p1,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(95)] as loss_p5,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(90)] as loss_p10,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(75)] as loss_p25,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(50)] as loss_p50,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(25)] as loss_p75,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(10)] as loss_p90,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(5)] as loss_p95,
    APPROX_QUANTILES(a.LossRate, 100)[OFFSET(1)] as loss_p99

FROM `measurement-lab.ndt.unified_downloads`
WHERE
    date >= "{START_DATE}" AND date < "{END_DATE}"
    AND client.Geo.CountryCode IS NOT NULL
    AND client.Geo.city IS NOT NULL
    AND client.Network.ASNumber IS NOT NULL
    AND a.MeanThroughputMbps IS NOT NULL
    AND a.MinRTT IS NOT NULL
GROUP BY country_code, city, asn, asn_name
ORDER BY country_code, city, asn

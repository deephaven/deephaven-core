SELECT
    total,
    to_timestamp(max_received_at) max_received_ts,
    mz_logical_timestamp() - max_received_at*1000 AS dt_ms
FROM pageviews_summary;

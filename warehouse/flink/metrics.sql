-- Run in a SQL client against an accepted Parquet DWD snapshot.
-- Realtime ingestion, watermark/late side output and Doris 2PC live in RealtimeJob.
SET 'table.local-time-zone' = 'Asia/Hong_Kong';
CREATE TEMPORARY VIEW daily_metrics AS
SELECT source, app, business_date,
 SUM(CASE WHEN event_type='page_view' THEN 1 ELSE 0 END) AS pv,
 COUNT(DISTINCT anonymous_id) AS uv,
 SUM(CASE WHEN event_type='request_complete' THEN 1 ELSE 0 END) AS requests,
 SUM(CASE WHEN event_type='request_complete' AND success=TRUE THEN 1 ELSE 0 END) AS successes
FROM dwd_events GROUP BY source, app, business_date;
SELECT * FROM daily_metrics;

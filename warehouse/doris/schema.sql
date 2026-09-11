CREATE DATABASE IF NOT EXISTS snow;
CREATE TABLE IF NOT EXISTS snow.events_realtime (
 source VARCHAR(16), app VARCHAR(32), business_key VARCHAR(80),
 event_type VARCHAR(32), business_date DATE, anonymous_id VARCHAR(36),
 page VARCHAR(200), character_id VARCHAR(64), success BOOLEAN,
 event_time DATETIMEV2(3), accepted_at DATETIMEV2(3), business_version BIGINT
) UNIQUE KEY(source,app,business_key)
DISTRIBUTED BY HASH(source,app,business_key) BUCKETS 2
PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true", "function_column.sequence_col"="business_version");
CREATE VIEW IF NOT EXISTS snow.daily_realtime AS
SELECT source,app,business_date,
 SUM(IF(event_type='page_view',1,0)) AS pv,
 COUNT(DISTINCT anonymous_id) AS uv,
 SUM(IF(event_type='request_complete',1,0)) AS requests,
 SUM(IF(event_type='request_complete' AND success=TRUE,1,0)) AS successes
FROM snow.events_realtime GROUP BY source,app,business_date;
CREATE TABLE IF NOT EXISTS snow.daily_offline (
 source VARCHAR(16), app VARCHAR(32), business_date DATE,
 pv BIGINT, uv BIGINT, requests BIGINT, successes BIGINT,
 business_version BIGINT, cutoff DATETIMEV2(3)
) UNIQUE KEY(source,app,business_date)
DISTRIBUTED BY HASH(source,app,business_date) BUCKETS 2
PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true", "function_column.sequence_col"="business_version");

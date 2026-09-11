CREATE DATABASE IF NOT EXISTS snow;
CREATE TABLE IF NOT EXISTS snow.daily_snapshots (
 snapshot_id VARCHAR(64), source VARCHAR(16), app VARCHAR(32), business_date DATE,
 pv BIGINT, uv BIGINT, requests BIGINT, successes BIGINT
) UNIQUE KEY(snapshot_id,source,app,business_date)
DISTRIBUTED BY HASH(snapshot_id,source,app) BUCKETS 2
PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true");
CREATE TABLE IF NOT EXISTS snow.offline_releases (
 source VARCHAR(16), business_date DATE, snapshot_id VARCHAR(64), run_id VARCHAR(100),
 content_hash VARCHAR(64), business_version BIGINT, cutoff DATETIMEV2(6)
) UNIQUE KEY(source,business_date)
DISTRIBUTED BY HASH(source,business_date) BUCKETS 2
PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true", "function_column.sequence_col"="business_version");
CREATE VIEW IF NOT EXISTS snow.daily_published AS
SELECT d.source,d.app,d.business_date,d.pv,d.uv,d.requests,d.successes
FROM snow.daily_snapshots d JOIN snow.offline_releases r
ON d.snapshot_id=r.snapshot_id AND d.source=r.source AND d.business_date=r.business_date;
CREATE VIEW IF NOT EXISTS snow.report_published AS
SELECT r.source,r.business_date,r.run_id,r.cutoff,d.app,d.pv,d.uv,d.requests,d.successes
FROM snow.offline_releases r LEFT JOIN snow.daily_snapshots d
ON d.snapshot_id=r.snapshot_id AND d.source=r.source AND d.business_date=r.business_date;

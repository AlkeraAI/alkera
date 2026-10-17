-- =============================================================================
-- Alkera HA Data Layer: S3 Tiering Policies & Grafana Rollup Views
-- Migration: 002_s3_tiering_and_materialized_views.sql
-- =============================================================================

-- 1. Minute-Level Throughput & Arrival Skew Rollup for Grafana Dashboards
CREATE TABLE IF NOT EXISTS market_data.minute_ingestion_stats (
    venue_id                UInt8,
    bucket_minute           DateTime('UTC'),
    instrument_iid          UInt32,
    delta_count             SimpleAggregateFunction(sum, UInt64),
    unverified_bbo_count    SimpleAggregateFunction(sum, UInt64),
    min_recv_ts_ns          SimpleAggregateFunction(min, UInt64),
    max_recv_ts_ns          SimpleAggregateFunction(max, UInt64)
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/minute_ingestion_stats', '{replica}')
PARTITION BY toYYYYMM(bucket_minute)
ORDER BY (venue_id, bucket_minute, instrument_iid);

CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.mv_minute_ingestion_stats
TO market_data.minute_ingestion_stats AS
SELECT
    venue_id,
    toStartOfMinute(fromUnixTimestamp64Milli(exchange_ts_ms)) AS bucket_minute,
    instrument_iid,
    count() AS delta_count,
    countIf(is_bbo_verified = 0) AS unverified_bbo_count,
    min(recv_ts_ns) AS min_recv_ts_ns,
    max(recv_ts_ns) AS max_recv_ts_ns
FROM market_data.l2_deltas
GROUP BY venue_id, bucket_minute, instrument_iid;

-- 2. Storage Compression & Compaction Audit View for Grafana
CREATE VIEW IF NOT EXISTS market_data.v_table_compression_stats AS
SELECT
    table,
    sum(rows) AS total_rows,
    sum(data_uncompressed_bytes) AS uncompressed_bytes,
    sum(data_compressed_bytes) AS compressed_bytes,
    round(sum(data_uncompressed_bytes) / nullIf(sum(data_compressed_bytes), 0), 2) AS compression_ratio,
    round(sum(data_compressed_bytes) / nullIf(sum(rows), 0), 2) AS compressed_bytes_per_row
FROM system.parts
WHERE database = 'market_data' AND active = 1
GROUP BY table
ORDER BY compressed_bytes DESC;

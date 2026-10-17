-- =============================================================================
-- Alkera HA Data Layer: ClickHouse Replicated Hot Analytical Store & S3 Tiering
-- Migration: 001_market_data_tables.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS market_data;

-- 1. Unified L2 Orderbook Deltas
-- Stores both Kalshi relative deltas (is_delta = 1) and Polymarket absolute level updates (is_delta = 0).
CREATE TABLE IF NOT EXISTS market_data.l2_deltas (
    venue_id                UInt8                           CODEC(ZSTD(1)), -- 1=Kalshi, 2=Polymarket
    instrument_iid          UInt32                          CODEC(ZSTD(1)),
    market_uid              UInt32                          CODEC(ZSTD(1)),
    exchange_ts_ms          UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    sending_ts_ms           UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    recv_ts_ns              UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    ingest_ts_ns            UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    collector_session       UInt64                          CODEC(ZSTD(1)),
    collector_leg           UInt8                           CODEC(ZSTD(1)), -- 0=LegA, 1=LegB
    sid                     UInt32                          CODEC(ZSTD(1)),
    seq                     UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    side                    Enum8('BID' = 1, 'ASK' = 2, 'KALSHI_YES' = 3, 'KALSHI_NO' = 4),
    is_delta                UInt8                           CODEC(ZSTD(1)), -- 1=Kalshi relative delta, 0=Polymarket absolute set
    price_micros            UInt32                          CODEC(T64, ZSTD(3)),
    size_micros             Int64                           CODEC(ZSTD(3)),
    resulting_size_micros   UInt64                          CODEC(ZSTD(3)),
    best_bid_micros         UInt32                          CODEC(T64, ZSTD(3)),
    best_ask_micros         UInt32                          CODEC(T64, ZSTD(3)),
    is_bbo_verified         UInt8                           CODEC(ZSTD(1)),
    event_hash              String                          CODEC(ZSTD(3)),
    dedup_key               UInt64                          CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/l2_deltas', '{replica}', recv_ts_ns)
PARTITION BY (venue_id, toYYYYMMDD(fromUnixTimestamp64Milli(exchange_ts_ms)))
ORDER BY (venue_id, instrument_iid, exchange_ts_ms, seq, side, price_micros, dedup_key)
SETTINGS index_granularity = 8192;

-- Canonical View: Projects Kalshi NO bids at price P into canonical YES asks at (1_000_000 - P)
CREATE VIEW IF NOT EXISTS market_data.v_canonical_l2_deltas AS
SELECT
    venue_id,
    instrument_iid,
    market_uid,
    exchange_ts_ms,
    recv_ts_ns,
    seq,
    multiIf(
        side = 'KALSHI_YES', 'BID',
        side = 'KALSHI_NO', 'ASK',
        toString(side)
    ) AS canonical_side,
    is_delta,
    if(side = 'KALSHI_NO', 1000000 - price_micros, price_micros) AS canonical_price_micros,
    size_micros,
    resulting_size_micros,
    best_bid_micros,
    best_ask_micros,
    is_bbo_verified,
    dedup_key
FROM market_data.l2_deltas FINAL;

-- 2. Periodic & Event-Driven L2 Orderbook Checkpoints (Parallel Sorted Arrays)
CREATE TABLE IF NOT EXISTS market_data.l2_checkpoints (
    venue_id                UInt8                           CODEC(ZSTD(1)),
    instrument_iid          UInt32                          CODEC(ZSTD(1)),
    market_uid              UInt32                          CODEC(ZSTD(1)),
    exchange_ts_ms          UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    recv_ts_ns              UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    collector_session       UInt64                          CODEC(ZSTD(1)),
    seq                     UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    checkpoint_reason       Enum8('SUBSCRIBE_INIT' = 1, 'PERIODIC_15M' = 2, 'GAP_RESYNC' = 3, 'POST_TRADE_VERIFIED' = 4, 'REST_SWEEP' = 5),
    book_hash               String                          CODEC(ZSTD(3)),
    bids_price_micros       Array(UInt32)                   CODEC(ZSTD(3)), -- Sorted descending (best bid first)
    bids_size_micros        Array(UInt64)                   CODEC(ZSTD(3)),
    asks_price_micros       Array(UInt32)                   CODEC(ZSTD(3)), -- Sorted ascending (best ask first)
    asks_size_micros        Array(UInt64)                   CODEC(ZSTD(3)),
    dedup_key               UInt64                          CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/l2_checkpoints', '{replica}', recv_ts_ns)
PARTITION BY (venue_id, toYYYYMM(fromUnixTimestamp64Milli(exchange_ts_ms)))
ORDER BY (venue_id, instrument_iid, exchange_ts_ms, seq, dedup_key)
SETTINGS index_granularity = 8192;

-- 3. Public Trade Executions
CREATE TABLE IF NOT EXISTS market_data.trades (
    venue_id                UInt8                           CODEC(ZSTD(1)),
    instrument_iid          UInt32                          CODEC(ZSTD(1)),
    market_uid              UInt32                          CODEC(ZSTD(1)),
    exchange_ts_ms          UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    sending_ts_ms           UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    recv_ts_ns              UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    leg_a_recv_ns           UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    leg_b_recv_ns           UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    trade_id                String                          CODEC(ZSTD(3)),
    tx_hash                 String                          CODEC(ZSTD(3)),
    price_micros            UInt32                          CODEC(T64, ZSTD(3)),
    complement_price_micros UInt32                          CODEC(T64, ZSTD(3)),
    size_micros             UInt64                          CODEC(ZSTD(3)),
    taker_side              Enum8('BUY' = 1, 'SELL' = 2, 'YES' = 3, 'NO' = 4),
    fee_rate_bps            UInt16                          CODEC(ZSTD(1)),
    is_block_trade          UInt8                           CODEC(ZSTD(1)),
    dedup_key               UInt64                          CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/trades', '{replica}', recv_ts_ns)
PARTITION BY (venue_id, toYYYYMM(fromUnixTimestamp64Milli(exchange_ts_ms)))
ORDER BY (venue_id, instrument_iid, exchange_ts_ms, trade_id, dedup_key)
SETTINGS index_granularity = 8192;

-- 4. Top-of-Book Ticker, Volume & Open Interest Updates
CREATE TABLE IF NOT EXISTS market_data.tickers (
    venue_id                UInt8                           CODEC(ZSTD(1)),
    instrument_iid          UInt32                          CODEC(ZSTD(1)),
    market_uid              UInt32                          CODEC(ZSTD(1)),
    exchange_ts_ms          UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    recv_ts_ns              UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    last_price_micros       UInt32                          CODEC(T64, ZSTD(3)),
    best_bid_micros         UInt32                          CODEC(T64, ZSTD(3)),
    best_ask_micros         UInt32                          CODEC(T64, ZSTD(3)),
    best_bid_size_micros    UInt64                          CODEC(ZSTD(3)),
    best_ask_size_micros    UInt64                          CODEC(ZSTD(3)),
    volume_micros           UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    open_interest_micros    UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    dollar_volume           UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    dollar_open_interest    UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    dedup_key               UInt64                          CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/tickers', '{replica}', recv_ts_ns)
PARTITION BY (venue_id, toYYYYMM(fromUnixTimestamp64Milli(exchange_ts_ms)))
ORDER BY (venue_id, instrument_iid, exchange_ts_ms, dedup_key)
SETTINGS index_granularity = 8192;

-- 5. Market Lifecycle & Dynamic Tick-Size Transitions
CREATE TABLE IF NOT EXISTS market_data.lifecycle_events (
    venue_id                UInt8                           CODEC(ZSTD(1)),
    market_uid              UInt32                          CODEC(ZSTD(1)),
    instrument_iid          UInt32                          CODEC(ZSTD(1)),
    exchange_ts_ms          UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    recv_ts_ns              UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    lifecycle_type          LowCardinality(String),
    old_tick_size_micros    UInt32                          CODEC(ZSTD(1)),
    new_tick_size_micros    UInt32                          CODEC(ZSTD(1)),
    result                  LowCardinality(String),
    settlement_value_micros Int64                           CODEC(ZSTD(1)),
    payload_json            String                          CODEC(ZSTD(3)),
    dedup_key               UInt64                          CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/lifecycle_events', '{replica}', recv_ts_ns)
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(exchange_ts_ms))
ORDER BY (venue_id, market_uid, exchange_ts_ms, lifecycle_type, dedup_key)
SETTINGS index_granularity = 8192;

-- 6. Underlying Reference & Oracle Feeds (CF Benchmarks, Pyth, PolyBolt Chainlink)
CREATE TABLE IF NOT EXISTS market_data.oracle_ticks (
    venue_id                UInt8                           CODEC(ZSTD(1)),
    source                  LowCardinality(String),
    symbol                  LowCardinality(String),
    exchange_ts_ms          UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    recv_ts_ns              UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    seq                     UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    dropped_count           UInt32                          CODEC(ZSTD(1)),
    price_micros            Int64                           CODEC(Delta, ZSTD(3)),
    confidence_micros       UInt64                          CODEC(ZSTD(3)),
    dedup_key               UInt64                          CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/oracle_ticks', '{replica}', recv_ts_ns)
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(exchange_ts_ms))
ORDER BY (source, symbol, exchange_ts_ms, seq, dedup_key)
SETTINGS index_granularity = 8192;

-- 7. Collector Sessions & Capture Gap Audit Log
CREATE TABLE IF NOT EXISTS market_data.capture_gaps (
    venue_id                UInt8                           CODEC(ZSTD(1)),
    instrument_iid          UInt32                          CODEC(ZSTD(1)),
    collector_session       UInt64                          CODEC(ZSTD(1)),
    collector_leg           UInt8                           CODEC(ZSTD(1)),
    shard_id                UInt16                          CODEC(ZSTD(1)),
    gap_type                Enum8('SEQ_GAP' = 1, 'BBO_MISMATCH' = 2, 'HASH_MISMATCH' = 3, 'WATCHDOG_TIMEOUT' = 4, 'SOCKET_EOF' = 5),
    gap_start_ns            UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    gap_resolved_ns         UInt64                          CODEC(DoubleDelta, ZSTD(3)),
    expected_seq            UInt64                          CODEC(ZSTD(1)),
    observed_seq            UInt64                          CODEC(ZSTD(1)),
    healed_by_twin_leg      UInt8                           CODEC(ZSTD(1)),
    recovery_checkpoint_seq UInt64                          CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/capture_gaps', '{replica}', gap_resolved_ns)
PARTITION BY toYYYYMM(fromUnixTimestamp64Nano(gap_start_ns))
ORDER BY (venue_id, instrument_iid, gap_start_ns, collector_leg)
SETTINGS index_granularity = 8192;

use alkera_schema::{
    CaptureGapEvent, L2CheckpointEvent, L2DeltaEvent, LifecycleEvent, MarketDataEnvelope,
    OracleTickEvent, TickerEvent, TradeEvent,
};
use bytes::{BufMut, Bytes, BytesMut};
use sha2::{Digest, Sha256};
use std::mem;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ClickHouseTable {
    L2Deltas,
    L2Checkpoints,
    Trades,
    Tickers,
    LifecycleEvents,
    OracleTicks,
    CaptureGaps,
}

impl ClickHouseTable {
    pub const fn qualified_name(self) -> &'static str {
        match self {
            Self::L2Deltas => "market_data.l2_deltas",
            Self::L2Checkpoints => "market_data.l2_checkpoints",
            Self::Trades => "market_data.trades",
            Self::Tickers => "market_data.tickers",
            Self::LifecycleEvents => "market_data.lifecycle_events",
            Self::OracleTicks => "market_data.oracle_ticks",
            Self::CaptureGaps => "market_data.capture_gaps",
        }
    }
}

/// An immutable, ready-to-flush ClickHouse batch carrying a deterministic `insert_deduplication_token`
/// accepted by ClickHouse `ReplicatedReplacingMergeTree` + ClickHouse Keeper for idempotent retries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedClickHouseBlock {
    pub table: ClickHouseTable,
    pub shard_id: u16,
    pub row_count: usize,
    pub min_offset: u64,
    pub max_offset: u64,
    /// Deterministic SHA-256 token sent via `X-ClickHouse-Insert-Deduplication-Token` / query setting
    /// `insert_deduplication_token` so retries never duplicate blocks on ClickHouse replicas.
    pub insert_deduplication_token: String,
    /// Compact ClickHouse `RowBinary` payload for `l2_deltas` (or `JSONEachRow` for variable-schema tables).
    pub payload: Bytes,
}

#[derive(Default)]
struct TableAccumulator {
    deltas: Vec<L2DeltaEvent>,
    checkpoints: Vec<L2CheckpointEvent>,
    trades: Vec<TradeEvent>,
    tickers: Vec<TickerEvent>,
    lifecycles: Vec<LifecycleEvent>,
    oracles: Vec<OracleTickEvent>,
    gaps: Vec<CaptureGapEvent>,
    min_offset: Option<u64>,
    max_offset: Option<u64>,
    total_rows: usize,
}

impl TableAccumulator {
    fn record_offset(&mut self, offset: u64) {
        self.min_offset = Some(self.min_offset.map_or(offset, |m| m.min(offset)));
        self.max_offset = Some(self.max_offset.map_or(offset, |m| m.max(offset)));
        self.total_rows += 1;
    }
}

/// Double-buffered ClickHouse batch sink.
/// Writers append to `active` while background flushers swap `active` with `standby` in `O(1)` time.
pub struct ClickHouseDoubleBufferSink {
    shard_id: u16,
    max_batch_rows: usize,
    active: TableAccumulator,
    standby: TableAccumulator,
}

impl ClickHouseDoubleBufferSink {
    pub fn new(shard_id: u16, max_batch_rows: usize) -> Self {
        Self {
            shard_id,
            max_batch_rows: max_batch_rows.max(1),
            active: TableAccumulator::default(),
            standby: TableAccumulator::default(),
        }
    }

    /// Pushes an envelope and its Redpanda/WAL monotonic offset into the active buffer.
    /// Returns `true` if `max_batch_rows` has been reached and a swap/flush should be triggered.
    pub fn push(&mut self, offset: u64, envelope: MarketDataEnvelope) -> bool {
        self.active.record_offset(offset);
        match envelope {
            MarketDataEnvelope::L2Delta(e) => self.active.deltas.push(e),
            MarketDataEnvelope::L2Checkpoint(e) => self.active.checkpoints.push(e),
            MarketDataEnvelope::Trade(e) => self.active.trades.push(e),
            MarketDataEnvelope::Ticker(e) => self.active.tickers.push(e),
            MarketDataEnvelope::Lifecycle(e) => self.active.lifecycles.push(e),
            MarketDataEnvelope::Oracle(e) => self.active.oracles.push(e),
            MarketDataEnvelope::CaptureGap(e) => self.active.gaps.push(e),
        }
        self.active.total_rows >= self.max_batch_rows
    }

    #[inline]
    pub fn buffered_rows(&self) -> usize {
        self.active.total_rows
    }

    /// Atomically swaps the active and standby buffers and builds deterministic `PreparedClickHouseBlock`s
    /// sorted by table primary keys (`ORDER BY (venue_id, instrument_iid, exchange_ts_ms, seq, ...)`).
    pub fn swap_and_build_blocks(&mut self) -> Vec<PreparedClickHouseBlock> {
        if self.active.total_rows == 0 {
            return Vec::new();
        }
        mem::swap(&mut self.active, &mut self.standby);
        let mut batch = mem::take(&mut self.standby);
        let min_offset = batch.min_offset.unwrap_or(0);
        let max_offset = batch.max_offset.unwrap_or(0);

        let mut blocks = Vec::new();

        if !batch.deltas.is_empty() {
            batch.deltas.sort_unstable_by_key(|d| {
                (
                    d.venue_id.as_u8(),
                    d.instrument_iid,
                    d.exchange_ts_ms,
                    d.seq,
                    d.side.as_u8(),
                    d.price_micros.0,
                    d.dedup_key,
                )
            });
            let payload = Self::encode_l2_deltas_row_binary(&batch.deltas);
            let token = Self::compute_dedup_token(
                ClickHouseTable::L2Deltas,
                self.shard_id,
                min_offset,
                max_offset,
                &payload,
            );
            blocks.push(PreparedClickHouseBlock {
                table: ClickHouseTable::L2Deltas,
                shard_id: self.shard_id,
                row_count: batch.deltas.len(),
                min_offset,
                max_offset,
                insert_deduplication_token: token,
                payload,
            });
        }

        if !batch.checkpoints.is_empty() {
            batch.checkpoints.sort_unstable_by_key(|c| {
                (
                    c.venue_id.as_u8(),
                    c.instrument_iid,
                    c.exchange_ts_ms,
                    c.seq,
                    c.dedup_key,
                )
            });
            let payload = Self::encode_json_each_row(&batch.checkpoints);
            let token = Self::compute_dedup_token(
                ClickHouseTable::L2Checkpoints,
                self.shard_id,
                min_offset,
                max_offset,
                &payload,
            );
            blocks.push(PreparedClickHouseBlock {
                table: ClickHouseTable::L2Checkpoints,
                shard_id: self.shard_id,
                row_count: batch.checkpoints.len(),
                min_offset,
                max_offset,
                insert_deduplication_token: token,
                payload,
            });
        }

        if !batch.trades.is_empty() {
            batch.trades.sort_unstable_by_key(|t| {
                (
                    t.venue_id.as_u8(),
                    t.instrument_iid,
                    t.exchange_ts_ms,
                    t.dedup_key,
                )
            });
            let payload = Self::encode_json_each_row(&batch.trades);
            let token = Self::compute_dedup_token(
                ClickHouseTable::Trades,
                self.shard_id,
                min_offset,
                max_offset,
                &payload,
            );
            blocks.push(PreparedClickHouseBlock {
                table: ClickHouseTable::Trades,
                shard_id: self.shard_id,
                row_count: batch.trades.len(),
                min_offset,
                max_offset,
                insert_deduplication_token: token,
                payload,
            });
        }

        if !batch.gaps.is_empty() {
            let payload = Self::encode_json_each_row(&batch.gaps);
            let token = Self::compute_dedup_token(
                ClickHouseTable::CaptureGaps,
                self.shard_id,
                min_offset,
                max_offset,
                &payload,
            );
            blocks.push(PreparedClickHouseBlock {
                table: ClickHouseTable::CaptureGaps,
                shard_id: self.shard_id,
                row_count: batch.gaps.len(),
                min_offset,
                max_offset,
                insert_deduplication_token: token,
                payload,
            });
        }

        blocks
    }

    /// Encodes `L2DeltaEvent` rows in ClickHouse native `RowBinary` layout matching `market_data.l2_deltas`.
    fn encode_l2_deltas_row_binary(rows: &[L2DeltaEvent]) -> Bytes {
        let mut buf = BytesMut::with_capacity(rows.len() * 96);
        for r in rows {
            buf.put_u8(r.venue_id.as_u8());
            buf.put_u32_le(r.instrument_iid);
            buf.put_u32_le(r.market_uid);
            buf.put_u64_le(r.exchange_ts_ms);
            buf.put_u64_le(r.sending_ts_ms);
            buf.put_u64_le(r.recv_ts_ns);
            buf.put_u64_le(r.ingest_ts_ns);
            buf.put_u64_le(r.collector_session);
            buf.put_u8(r.collector_leg);
            buf.put_u32_le(r.sid);
            buf.put_u64_le(r.seq);
            buf.put_u8(r.side.as_u8());
            buf.put_u8(u8::from(r.is_delta));
            buf.put_u32_le(r.price_micros.0);
            buf.put_i64_le(r.size_micros.0);
            buf.put_u64_le(r.resulting_size_micros);
            buf.put_u32_le(r.best_bid_micros.0);
            buf.put_u32_le(r.best_ask_micros.0);
            buf.put_u8(u8::from(r.is_bbo_verified));
            Self::put_leb128_string(&mut buf, &r.event_hash);
            buf.put_u64_le(r.dedup_key);
        }
        buf.freeze()
    }

    fn put_leb128_string(buf: &mut BytesMut, s: &str) {
        let mut len = s.len();
        loop {
            let mut byte = (len & 0x7f) as u8;
            len >>= 7;
            if len != 0 {
                byte |= 0x80;
            }
            buf.put_u8(byte);
            if len == 0 {
                break;
            }
        }
        buf.put_slice(s.as_bytes());
    }

    fn encode_json_each_row<T: serde::Serialize>(rows: &[T]) -> Bytes {
        let mut buf = Vec::with_capacity(rows.len() * 128);
        for row in rows {
            serde_json::to_writer(&mut buf, row).expect("serialize row");
            buf.push(b'\n');
        }
        Bytes::from(buf)
    }

    fn compute_dedup_token(
        table: ClickHouseTable,
        shard_id: u16,
        min_offset: u64,
        max_offset: u64,
        payload: &[u8],
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(table.qualified_name().as_bytes());
        hasher.update(shard_id.to_le_bytes());
        hasher.update(min_offset.to_le_bytes());
        hasher.update(max_offset.to_le_bytes());
        hasher.update(payload);
        hex::encode(hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alkera_schema::{BookSide, PriceMicros, SizeMicros, VenueId};

    #[test]
    fn test_double_buffer_deterministic_dedup_token() {
        let mut sink1 = ClickHouseDoubleBufferSink::new(1, 2);
        let mut sink2 = ClickHouseDoubleBufferSink::new(1, 2);

        let delta = MarketDataEnvelope::L2Delta(L2DeltaEvent {
            venue_id: VenueId::Kalshi,
            instrument_iid: 55,
            market_uid: 12,
            exchange_ts_ms: 1_000,
            sending_ts_ms: 1_000,
            recv_ts_ns: 1_000_000,
            ingest_ts_ns: 1_000_100,
            collector_session: 1,
            collector_leg: 0,
            sid: 1,
            seq: 10,
            side: BookSide::KalshiYes,
            is_delta: true,
            price_micros: PriceMicros(500_000),
            size_micros: SizeMicros(10_000_000),
            resulting_size_micros: 10_000_000,
            best_bid_micros: PriceMicros(500_000),
            best_ask_micros: PriceMicros(510_000),
            is_bbo_verified: true,
            event_hash: String::new(),
            dedup_key: 999,
        });

        sink1.push(100, delta.clone());
        sink2.push(100, delta);

        let b1 = sink1.swap_and_build_blocks();
        let b2 = sink2.swap_and_build_blocks();
        assert_eq!(b1.len(), 1);
        assert_eq!(
            b1[0].insert_deduplication_token,
            b2[0].insert_deduplication_token
        );
        assert_eq!(b1[0].row_count, 1);
    }

    #[test]
    fn test_clickhouse_block_sorting_invariance() {
        let mut sink_forward = ClickHouseDoubleBufferSink::new(2, 10);
        let mut sink_reverse = ClickHouseDoubleBufferSink::new(2, 10);

        let make_row = |seq: u64| {
            MarketDataEnvelope::L2Delta(L2DeltaEvent {
                venue_id: VenueId::Kalshi,
                instrument_iid: 77,
                market_uid: 15,
                exchange_ts_ms: 1_000 + seq,
                sending_ts_ms: 1_000 + seq,
                recv_ts_ns: 1_000_000 + seq * 1_000,
                ingest_ts_ns: 1_000_100 + seq * 1_000,
                collector_session: 1,
                collector_leg: 0,
                sid: 1,
                seq,
                side: BookSide::KalshiYes,
                is_delta: true,
                price_micros: PriceMicros(520_000),
                size_micros: SizeMicros(5_000_000),
                resulting_size_micros: seq * 5_000_000,
                best_bid_micros: PriceMicros::ZERO,
                best_ask_micros: PriceMicros::ZERO,
                is_bbo_verified: true,
                event_hash: String::new(),
                dedup_key: 1_000 + seq,
            })
        };

        for s in 1..=5 {
            sink_forward.push(s, make_row(s));
        }
        for s in (1..=5).rev() {
            sink_reverse.push(s, make_row(s));
        }

        let block_fwd = sink_forward.swap_and_build_blocks();
        let block_rev = sink_reverse.swap_and_build_blocks();
        assert_eq!(block_fwd[0].payload, block_rev[0].payload);
        assert_eq!(
            block_fwd[0].insert_deduplication_token,
            block_rev[0].insert_deduplication_token
        );
    }
}

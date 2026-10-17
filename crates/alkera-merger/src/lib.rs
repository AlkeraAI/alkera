use alkera_book::{BookError, FlatOrderBook};
use alkera_schema::{
    CaptureGapEvent, CheckpointReason, GapType, L2CheckpointEvent, L2DeltaEvent,
    MarketDataEnvelope, PriceMicros, VenueId,
};
use std::collections::{HashMap, VecDeque};

/// Default periodic L2 checkpoint interval: 15 minutes in nanoseconds (`900_000_000_000` ns).
pub const DEFAULT_CHECKPOINT_INTERVAL_NS: u64 = 15 * 60 * 1_000_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergerOutcome {
    /// First-seen canonical event emitted immediately with zero buffering delay.
    EmittedPrimary(MarketDataEnvelope),
    /// Emitted both the primary delta and an automatic periodic 15-minute L2 checkpoint.
    EmittedWithPeriodicCheckpoint {
        primary: MarketDataEnvelope,
        checkpoint: L2CheckpointEvent,
    },
    /// Suppressed duplicate frame arriving on the slower HA leg; records arrival skew (`|leg_a_ns - leg_b_ns|`).
    SuppressedTwinDuplicate {
        dedup_key: u64,
        first_recv_ts_ns: u64,
        twin_recv_ts_ns: u64,
        skew_ns: u64,
    },
    /// Suppressed redundant Polymarket post-trade `book` snapshot because the incremental `price_change`
    /// state already matches the snapshot's levels and `book_hash`.
    CompactedRedundantPostTradeBook {
        instrument_iid: u32,
        book_hash: String,
        bytes_saved_estimate: usize,
    },
    /// Recorded a sequence gap or BBO mismatch on one leg that was seamlessly healed by the active twin leg.
    GapRecorded(CaptureGapEvent),
}

#[derive(Clone, Copy, Debug)]
struct SeenEntry {
    first_recv_ts_ns: u64,
    first_leg: u8,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MergerMetrics {
    pub primary_emitted_total: u64,
    pub twin_duplicates_suppressed_total: u64,
    pub post_trade_books_compacted_total: u64,
    pub post_trade_bytes_saved_total: u64,
    pub periodic_checkpoints_emitted_total: u64,
    pub gaps_healed_by_twin_total: u64,
    pub gaps_requiring_resync_total: u64,
}

/// Active-Active Multi-AZ Stream Merger, Fast-First Deduplicator, Twin-Leg Gap Healer,
/// and Polymarket Post-Trade `book` Snapshot Compactor.
pub struct ActiveActiveMerger {
    dedup_window_ns: u64,
    max_window_entries: usize,
    checkpoint_interval_ns: u64,
    seen_map: HashMap<u64, SeenEntry>,
    seen_queue: VecDeque<(u64, u64)>,
    /// Per-leg `(venue_id, leg_id, sid)` sequence trackers for Kalshi gap detection.
    leg_sid_seq: HashMap<(VenueId, u8, u32), u64>,
    /// Canonical merged L2 orderbooks indexed by `instrument_iid`.
    canonical_books: HashMap<u32, FlatOrderBook>,
    pub metrics: MergerMetrics,
}

impl ActiveActiveMerger {
    pub fn new(
        dedup_window_ns: u64,
        max_window_entries: usize,
        checkpoint_interval_ns: u64,
    ) -> Self {
        Self {
            dedup_window_ns,
            max_window_entries,
            checkpoint_interval_ns,
            seen_map: HashMap::with_capacity(65_536),
            seen_queue: VecDeque::with_capacity(65_536),
            leg_sid_seq: HashMap::new(),
            canonical_books: HashMap::new(),
            metrics: MergerMetrics::default(),
        }
    }

    pub fn canonical_book(&self, instrument_iid: u32) -> Option<&FlatOrderBook> {
        self.canonical_books.get(&instrument_iid)
    }

    fn evict_expired(&mut self, now_ns: u64) {
        let cutoff = now_ns.saturating_sub(self.dedup_window_ns);
        while let Some(&(key, ts)) = self.seen_queue.front() {
            if ts < cutoff || self.seen_map.len() > self.max_window_entries {
                self.seen_queue.pop_front();
                self.seen_map.remove(&key);
            } else {
                break;
            }
        }
    }

    /// Processes an incoming `L2DeltaEvent` from either Collector Leg A (`0`) or Leg B (`1`).
    /// 1. Validates per-leg `(sid, seq)` continuity; if a leg dropped frames, checks whether the twin leg
    ///    already advanced the canonical book cleanly (`healed_by_twin_leg = true`).
    /// 2. Performs fast-first deduplication on `delta.dedup_key`.
    /// 3. Applies the winner delta to the canonical `FlatOrderBook` and emits a `PERIODIC_15M` checkpoint
    ///    whenever `checkpoint_interval_ns` has elapsed.
    pub fn ingest_l2_delta(&mut self, mut delta: L2DeltaEvent) -> Vec<MergerOutcome> {
        let mut outcomes = Vec::with_capacity(2);
        self.evict_expired(delta.recv_ts_ns);

        // 1. Per-leg sequence continuity check (Kalshi sid/seq)
        if delta.venue_id == VenueId::Kalshi && delta.seq > 0 {
            let sid_key = (delta.venue_id, delta.collector_leg, delta.sid);
            if let Some(&prev_leg_seq) = self.leg_sid_seq.get(&sid_key) {
                if delta.seq > prev_leg_seq + 1 {
                    let twin_leg = 1 - delta.collector_leg;
                    let twin_seq = self
                        .leg_sid_seq
                        .get(&(delta.venue_id, twin_leg, delta.sid))
                        .copied()
                        .unwrap_or(0);
                    let healed_by_twin = twin_seq >= delta.seq - 1
                        || self
                            .canonical_books
                            .get(&delta.instrument_iid)
                            .map(|b| b.is_synced && b.last_seq >= delta.seq - 1)
                            .unwrap_or(false);

                    if healed_by_twin {
                        self.metrics.gaps_healed_by_twin_total += 1;
                    } else {
                        self.metrics.gaps_requiring_resync_total += 1;
                    }

                    outcomes.push(MergerOutcome::GapRecorded(CaptureGapEvent {
                        venue_id: delta.venue_id,
                        instrument_iid: delta.instrument_iid,
                        collector_session: delta.collector_session,
                        collector_leg: delta.collector_leg,
                        shard_id: delta.sid as u16,
                        gap_type: GapType::SeqGap,
                        gap_start_ns: delta.recv_ts_ns,
                        gap_resolved_ns: delta.recv_ts_ns,
                        expected_seq: prev_leg_seq + 1,
                        observed_seq: delta.seq,
                        healed_by_twin_leg: healed_by_twin,
                        recovery_checkpoint_seq: delta.seq,
                    }));
                }
            }
            self.leg_sid_seq.insert(sid_key, delta.seq);
        }

        // 2. Fast-first deduplication check
        if let Some(seen) = self.seen_map.get(&delta.dedup_key).copied() {
            if seen.first_leg != delta.collector_leg {
                self.metrics.twin_duplicates_suppressed_total += 1;
                let skew_ns = delta.recv_ts_ns.abs_diff(seen.first_recv_ts_ns);
                outcomes.push(MergerOutcome::SuppressedTwinDuplicate {
                    dedup_key: delta.dedup_key,
                    first_recv_ts_ns: seen.first_recv_ts_ns,
                    twin_recv_ts_ns: delta.recv_ts_ns,
                    skew_ns,
                });
                return outcomes;
            }
        }

        self.seen_map.insert(
            delta.dedup_key,
            SeenEntry {
                first_recv_ts_ns: delta.recv_ts_ns,
                first_leg: delta.collector_leg,
            },
        );
        self.seen_queue
            .push_back((delta.dedup_key, delta.recv_ts_ns));

        // 3. Apply to canonical book & check BBO verification
        let book = self
            .canonical_books
            .entry(delta.instrument_iid)
            .or_insert_with(|| {
                FlatOrderBook::new(delta.venue_id, delta.instrument_iid, delta.market_uid)
            });

        match book.apply_delta(&delta, false) {
            Ok(resulting_size) => {
                delta.resulting_size_micros = resulting_size;
                delta.is_bbo_verified = true;
            }
            Err(BookError::BboMismatch { .. }) => {
                delta.is_bbo_verified = false;
                self.metrics.gaps_requiring_resync_total += 1;
                outcomes.push(MergerOutcome::GapRecorded(CaptureGapEvent {
                    venue_id: delta.venue_id,
                    instrument_iid: delta.instrument_iid,
                    collector_session: delta.collector_session,
                    collector_leg: delta.collector_leg,
                    shard_id: delta.sid as u16,
                    gap_type: GapType::BboMismatch,
                    gap_start_ns: delta.recv_ts_ns,
                    gap_resolved_ns: delta.recv_ts_ns,
                    expected_seq: delta.seq,
                    observed_seq: delta.seq,
                    healed_by_twin_leg: false,
                    recovery_checkpoint_seq: delta.seq,
                }));
            }
            Err(_) => {}
        }

        self.metrics.primary_emitted_total += 1;

        // Check if 15-minute periodic L2 checkpoint is due
        if book.last_checkpoint_recv_ns > 0
            && delta
                .recv_ts_ns
                .saturating_sub(book.last_checkpoint_recv_ns)
                >= self.checkpoint_interval_ns
        {
            book.last_checkpoint_recv_ns = delta.recv_ts_ns;
            let cp = book.to_checkpoint(
                delta.collector_session,
                CheckpointReason::Periodic15m,
                None,
            );
            self.metrics.periodic_checkpoints_emitted_total += 1;
            outcomes.push(MergerOutcome::EmittedWithPeriodicCheckpoint {
                primary: MarketDataEnvelope::L2Delta(delta),
                checkpoint: cp,
            });
        } else {
            outcomes.push(MergerOutcome::EmittedPrimary(MarketDataEnvelope::L2Delta(
                delta,
            )));
        }

        outcomes
    }

    /// Processes a full L2 `L2CheckpointEvent` (including Polymarket post-trade `book` snapshots).
    /// When `checkpoint_reason == CheckpointReason::PostTradeVerified` and the canonical book already
    /// has the exact same bid/ask price levels (from preceding `price_change` messages) and is within
    /// the 15-minute checkpoint window, suppresses the redundant multi-KB array write.
    pub fn ingest_l2_checkpoint(&mut self, mut cp: L2CheckpointEvent) -> MergerOutcome {
        self.evict_expired(cp.recv_ts_ns);

        if let Some(seen) = self.seen_map.get(&cp.dedup_key).copied() {
            self.metrics.twin_duplicates_suppressed_total += 1;
            return MergerOutcome::SuppressedTwinDuplicate {
                dedup_key: cp.dedup_key,
                first_recv_ts_ns: seen.first_recv_ts_ns,
                twin_recv_ts_ns: cp.recv_ts_ns,
                skew_ns: cp.recv_ts_ns.abs_diff(seen.first_recv_ts_ns),
            };
        }

        self.seen_map.insert(
            cp.dedup_key,
            SeenEntry {
                first_recv_ts_ns: cp.recv_ts_ns,
                first_leg: 0,
            },
        );
        self.seen_queue.push_back((cp.dedup_key, cp.recv_ts_ns));

        let raw_bids: Vec<(PriceMicros, u64)> = cp
            .bids_price_micros
            .iter()
            .copied()
            .map(PriceMicros)
            .zip(cp.bids_size_micros.iter().copied())
            .collect();
        let raw_asks: Vec<(PriceMicros, u64)> = cp
            .asks_price_micros
            .iter()
            .copied()
            .map(PriceMicros)
            .zip(cp.asks_size_micros.iter().copied())
            .collect();

        let book = self
            .canonical_books
            .entry(cp.instrument_iid)
            .or_insert_with(|| FlatOrderBook::new(cp.venue_id, cp.instrument_iid, cp.market_uid));

        if cp.checkpoint_reason == CheckpointReason::PostTradeVerified
            && book.is_synced
            && book.matches_levels(&raw_bids, &raw_asks)
            && cp.recv_ts_ns.saturating_sub(book.last_checkpoint_recv_ns)
                < self.checkpoint_interval_ns
        {
            let estimated_bytes =
                64 + (cp.bids_price_micros.len() + cp.asks_price_micros.len()) * 24;
            self.metrics.post_trade_books_compacted_total += 1;
            self.metrics.post_trade_bytes_saved_total += estimated_bytes as u64;
            return MergerOutcome::CompactedRedundantPostTradeBook {
                instrument_iid: cp.instrument_iid,
                book_hash: cp.book_hash,
                bytes_saved_estimate: estimated_bytes,
            };
        }

        // Normalize into canonical sorted order (descending bids, ascending asks) before persisting
        book.apply_polymarket_snapshot(cp.seq, cp.exchange_ts_ms, cp.recv_ts_ns, &raw_bids, &raw_asks);
        let normalized = book.to_checkpoint(
            cp.collector_session,
            cp.checkpoint_reason,
            Some(cp.book_hash.clone()),
        );
        cp.bids_price_micros = normalized.bids_price_micros;
        cp.bids_size_micros = normalized.bids_size_micros;
        cp.asks_price_micros = normalized.asks_price_micros;
        cp.asks_size_micros = normalized.asks_size_micros;

        self.metrics.primary_emitted_total += 1;
        MergerOutcome::EmittedPrimary(MarketDataEnvelope::L2Checkpoint(cp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alkera_schema::{BookSide, SizeMicros};

    #[test]
    fn test_active_active_deduplication_and_twin_gap_healing() {
        let mut merger = ActiveActiveMerger::new(10_000_000_000, 100_000, DEFAULT_CHECKPOINT_INTERVAL_NS);

        let make_delta = |leg: u8, seq: u64, recv_ns: u64, price: u32, delta_size: i64, res_size: u64| {
            let dedup = L2DeltaEvent::compute_dedup_key(
                VenueId::Kalshi,
                10,
                5,
                1_000 + seq,
                BookSide::KalshiYes,
                true,
                PriceMicros(price),
                SizeMicros(delta_size),
                res_size,
                "",
            );
            L2DeltaEvent {
                venue_id: VenueId::Kalshi,
                instrument_iid: 10,
                market_uid: 5,
                exchange_ts_ms: 1_000 + seq,
                sending_ts_ms: 1_000 + seq,
                recv_ts_ns: recv_ns,
                ingest_ts_ns: recv_ns + 100,
                collector_session: 1,
                collector_leg: leg,
                sid: 1,
                seq,
                side: BookSide::KalshiYes,
                is_delta: true,
                price_micros: PriceMicros(price),
                size_micros: SizeMicros(delta_size),
                resulting_size_micros: res_size,
                best_bid_micros: PriceMicros::ZERO,
                best_ask_micros: PriceMicros::ZERO,
                is_bbo_verified: true,
                event_hash: String::new(),
                dedup_key: dedup,
            }
        };

        // Seq 1 arrives on Leg A first, then 2ms later on Leg B
        let out_a1 = merger.ingest_l2_delta(make_delta(0, 1, 1_000_000, 500_000, 100_000_000, 100_000_000));
        assert!(matches!(out_a1.as_slice(), [MergerOutcome::EmittedPrimary(_)]));

        let out_b1 = merger.ingest_l2_delta(make_delta(1, 1, 3_000_000, 500_000, 100_000_000, 100_000_000));
        assert!(matches!(
            out_b1.as_slice(),
            [MergerOutcome::SuppressedTwinDuplicate { skew_ns: 2_000_000, .. }]
        ));

        // Simulate Leg A dropping Seq 2 while Leg B delivers Seq 2 and Seq 3
        let out_b2 = merger.ingest_l2_delta(make_delta(1, 2, 4_000_000, 500_000, 50_000_000, 150_000_000));
        assert!(matches!(out_b2.as_slice(), [MergerOutcome::EmittedPrimary(_)]));

        // Now Leg A delivers Seq 3 (skipping Seq 2 on Leg A!). Merger detects Leg A's gap,
        // marks it healed_by_twin_leg = true (since Leg B already delivered Seq 2), and emits Seq 3.
        let out_a3 = merger.ingest_l2_delta(make_delta(0, 3, 5_000_000, 500_000, 25_000_000, 175_000_000));
        assert_eq!(out_a3.len(), 2);
        match &out_a3[0] {
            MergerOutcome::GapRecorded(gap) => {
                assert!(gap.healed_by_twin_leg);
                assert_eq!(gap.expected_seq, 2);
                assert_eq!(gap.observed_seq, 3);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert_eq!(merger.metrics.gaps_healed_by_twin_total, 1);
    }

    #[test]
    fn test_polymarket_post_trade_book_compaction() {
        let mut merger = ActiveActiveMerger::new(10_000_000_000, 100_000, DEFAULT_CHECKPOINT_INTERVAL_NS);

        // 1. Initial subscription snapshot on Polymarket instrument 200
        let init_cp = L2CheckpointEvent {
            venue_id: VenueId::Polymarket,
            instrument_iid: 200,
            market_uid: 99,
            exchange_ts_ms: 10_000,
            recv_ts_ns: 10_000_000_000,
            collector_session: 1,
            seq: 1,
            checkpoint_reason: CheckpointReason::SubscribeInit,
            book_hash: "0xinit".to_string(),
            bids_price_micros: vec![490_000, 500_000],
            bids_size_micros: vec![20_000_000, 15_000_000],
            asks_price_micros: vec![530_000, 520_000],
            asks_size_micros: vec![60_000_000, 25_000_000],
            dedup_key: 1,
        };
        assert!(matches!(
            merger.ingest_l2_checkpoint(init_cp),
            MergerOutcome::EmittedPrimary(_)
        ));

        // 2. Trade consumes the 0.52 ask level completely via price_change (size -> 0)
        let delta = L2DeltaEvent {
            venue_id: VenueId::Polymarket,
            instrument_iid: 200,
            market_uid: 99,
            exchange_ts_ms: 10_050,
            sending_ts_ms: 10_050,
            recv_ts_ns: 10_050_000_000,
            ingest_ts_ns: 10_050_000_100,
            collector_session: 1,
            collector_leg: 0,
            sid: 0,
            seq: 2,
            side: BookSide::Ask,
            is_delta: false,
            price_micros: PriceMicros(520_000),
            size_micros: SizeMicros(0),
            resulting_size_micros: 0,
            best_bid_micros: PriceMicros(500_000),
            best_ask_micros: PriceMicros(530_000),
            is_bbo_verified: true,
            event_hash: "0xorderhash1".to_string(),
            dedup_key: 2,
        };
        merger.ingest_l2_delta(delta);

        // 3. Polymarket broadcasts redundant post-trade full `book` snapshot with the exact same state
        let post_trade_cp = L2CheckpointEvent {
            venue_id: VenueId::Polymarket,
            instrument_iid: 200,
            market_uid: 99,
            exchange_ts_ms: 10_051,
            recv_ts_ns: 10_051_000_000,
            collector_session: 1,
            seq: 3,
            checkpoint_reason: CheckpointReason::PostTradeVerified,
            book_hash: "0xposttrade".to_string(),
            bids_price_micros: vec![490_000, 500_000],
            bids_size_micros: vec![20_000_000, 15_000_000],
            asks_price_micros: vec![530_000],
            asks_size_micros: vec![60_000_000],
            dedup_key: 3,
        };

        let outcome = merger.ingest_l2_checkpoint(post_trade_cp);
        assert!(matches!(
            outcome,
            MergerOutcome::CompactedRedundantPostTradeBook { instrument_iid: 200, .. }
        ));
        assert_eq!(merger.metrics.post_trade_books_compacted_total, 1);
    }

    #[test]
    fn test_kalshi_simultaneous_both_leg_gap_flags_resync_required() {
        let mut merger = ActiveActiveMerger::new(10_000_000_000, 100_000, DEFAULT_CHECKPOINT_INTERVAL_NS);

        let make_delta = |leg: u8, seq: u64, recv_ns: u64| L2DeltaEvent {
            venue_id: VenueId::Kalshi,
            instrument_iid: 42,
            market_uid: 10,
            exchange_ts_ms: 1_000 + seq,
            sending_ts_ms: 1_000 + seq,
            recv_ts_ns: recv_ns,
            ingest_ts_ns: recv_ns,
            collector_session: 1,
            collector_leg: leg,
            sid: 5,
            seq,
            side: BookSide::KalshiYes,
            is_delta: true,
            price_micros: PriceMicros(550_000),
            size_micros: SizeMicros(10_000_000),
            resulting_size_micros: seq * 10_000_000,
            best_bid_micros: PriceMicros::ZERO,
            best_ask_micros: PriceMicros::ZERO,
            is_bbo_verified: true,
            event_hash: String::new(),
            dedup_key: seq,
        };

        // Both legs deliver seq 1
        merger.ingest_l2_delta(make_delta(0, 1, 1_000_000));
        merger.ingest_l2_delta(make_delta(1, 1, 1_100_000));

        // Now BOTH legs miss seq 2 and Leg A delivers seq 3 first -> must record healed_by_twin_leg = false
        let out = merger.ingest_l2_delta(make_delta(0, 3, 2_000_000));
        match &out[0] {
            MergerOutcome::GapRecorded(gap) => {
                assert!(!gap.healed_by_twin_leg);
                assert_eq!(gap.expected_seq, 2);
                assert_eq!(gap.observed_seq, 3);
            }
            other => panic!("expected GapRecorded, got {other:?}"),
        }
        assert_eq!(merger.metrics.gaps_requiring_resync_total, 1);
    }
}

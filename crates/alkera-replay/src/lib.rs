use alkera_book::{BookError, FlatOrderBook};
use alkera_schema::{CaptureGapEvent, L2CheckpointEvent, L2DeltaEvent};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReplayError {
    #[error("no L2 checkpoint found at or before target timestamp {target_ts_ns} for instrument {instrument_iid}")]
    MissingAnchorCheckpoint {
        instrument_iid: u32,
        target_ts_ns: u64,
    },
    #[error("orderbook state machine error during replay: {0}")]
    Book(#[from] BookError),
}

/// Continuity audit classification for a point-in-time reconstructed orderbook.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContinuityProof {
    /// Zero sequence or BBO gaps occurred on either leg between the anchor checkpoint and `target_recv_ts_ns`.
    Clean,
    /// One collector leg experienced a gap, but the active twin leg healed it with zero data loss (`healed_by_twin_leg == true`).
    HealedByTwinLeg { healed_gap_count: usize },
    /// An unhealed gap occurred on both legs overlapping `(checkpoint_recv_ts_ns, target_recv_ts_ns]`.
    UnhealedGapDetected { unhealed_gaps: Vec<CaptureGapEvent> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconstructedBookSnapshot {
    pub book: FlatOrderBook,
    pub anchor_checkpoint_recv_ns: u64,
    pub target_recv_ts_ns: u64,
    pub deltas_applied: usize,
    pub state_hash: String,
    pub continuity: ContinuityProof,
}

/// Deterministic Point-in-Time L2 Orderbook Replay & Verification Engine.
pub struct PointInTimeReconstructor;

impl PointInTimeReconstructor {
    /// Reconstructs the exact `FlatOrderBook` for `instrument_iid` at `target_recv_ts_ns`:
    /// 1. Selects the latest `L2CheckpointEvent` with `recv_ts_ns <= target_recv_ts_ns`.
    /// 2. Seeds the initial `FlatOrderBook` from the checkpoint's sorted parallel arrays.
    /// 3. Streams all `L2DeltaEvent`s where `checkpoint.recv_ts_ns < delta.recv_ts_ns <= target_recv_ts_ns`
    ///    ordered by `(exchange_ts_ms, seq, recv_ts_ns)`.
    /// 4. Audits `CaptureGapEvent`s overlapping `[checkpoint.recv_ts_ns, target_recv_ts_ns]` to produce a `ContinuityProof`.
    pub fn reconstruct_at_recv_ns(
        instrument_iid: u32,
        target_recv_ts_ns: u64,
        checkpoints: &[L2CheckpointEvent],
        deltas: &[L2DeltaEvent],
        gaps: &[CaptureGapEvent],
    ) -> Result<ReconstructedBookSnapshot, ReplayError> {
        let anchor = checkpoints
            .iter()
            .filter(|cp| cp.instrument_iid == instrument_iid && cp.recv_ts_ns <= target_recv_ts_ns)
            .max_by_key(|cp| (cp.recv_ts_ns, cp.seq))
            .ok_or(ReplayError::MissingAnchorCheckpoint {
                instrument_iid,
                target_ts_ns: target_recv_ts_ns,
            })?;

        let mut book = FlatOrderBook::new(anchor.venue_id, anchor.instrument_iid, anchor.market_uid);
        book.apply_checkpoint(anchor)?;

        let mut window_deltas: Vec<&L2DeltaEvent> = deltas
            .iter()
            .filter(|d| {
                d.instrument_iid == instrument_iid
                    && d.recv_ts_ns > anchor.recv_ts_ns
                    && d.recv_ts_ns <= target_recv_ts_ns
            })
            .collect();

        window_deltas.sort_unstable_by_key(|d| (d.exchange_ts_ms, d.seq, d.recv_ts_ns));

        for delta in &window_deltas {
            book.apply_delta(delta, false)?;
        }

        let overlapping_gaps: Vec<&CaptureGapEvent> = gaps
            .iter()
            .filter(|g| {
                g.instrument_iid == instrument_iid
                    && g.gap_resolved_ns >= anchor.recv_ts_ns
                    && g.gap_start_ns <= target_recv_ts_ns
            })
            .collect();

        let continuity = if overlapping_gaps.is_empty() {
            ContinuityProof::Clean
        } else {
            let unhealed: Vec<CaptureGapEvent> = overlapping_gaps
                .iter()
                .filter(|g| !g.healed_by_twin_leg)
                .map(|&g| g.clone())
                .collect();
            if unhealed.is_empty() {
                ContinuityProof::HealedByTwinLeg {
                    healed_gap_count: overlapping_gaps.len(),
                }
            } else {
                ContinuityProof::UnhealedGapDetected {
                    unhealed_gaps: unhealed,
                }
            }
        };

        let state_hash = book.compute_state_hash();
        Ok(ReconstructedBookSnapshot {
            book,
            anchor_checkpoint_recv_ns: anchor.recv_ts_ns,
            target_recv_ts_ns,
            deltas_applied: window_deltas.len(),
            state_hash,
            continuity,
        })
    }

    /// Verifies that replaying from `checkpoint_start` through `deltas` up to `checkpoint_end.recv_ts_ns`
    /// produces the exact same bid/ask ladders as `checkpoint_end`.
    pub fn verify_checkpoint_transition(
        checkpoint_start: &L2CheckpointEvent,
        checkpoint_end: &L2CheckpointEvent,
        deltas: &[L2DeltaEvent],
        gaps: &[CaptureGapEvent],
    ) -> Result<bool, ReplayError> {
        let replayed = Self::reconstruct_at_recv_ns(
            checkpoint_start.instrument_iid,
            checkpoint_end.recv_ts_ns,
            std::slice::from_ref(checkpoint_start),
            deltas,
            gaps,
        )?;

        let mut expected_book = FlatOrderBook::new(
            checkpoint_end.venue_id,
            checkpoint_end.instrument_iid,
            checkpoint_end.market_uid,
        );
        expected_book.apply_checkpoint(checkpoint_end)?;

        Ok(replayed.book.bids_descending() == expected_book.bids_descending()
            && replayed.book.asks_ascending() == expected_book.asks_ascending())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alkera_merger::{ActiveActiveMerger, MergerOutcome, DEFAULT_CHECKPOINT_INTERVAL_NS};
    use alkera_schema::{BookSide, CheckpointReason, MarketDataEnvelope, PriceMicros, SizeMicros, VenueId};
    use alkera_sink_parquet::{read_l2_deltas_parquet, write_l2_deltas_parquet};
    use tempfile::NamedTempFile;

    #[test]
    fn test_end_to_end_failover_compaction_parquet_and_nanosecond_replay() {
        let mut merger = ActiveActiveMerger::new(10_000_000_000, 100_000, DEFAULT_CHECKPOINT_INTERVAL_NS);
        let mut stored_checkpoints: Vec<L2CheckpointEvent> = Vec::new();
        let mut stored_deltas: Vec<L2DeltaEvent> = Vec::new();
        let mut stored_gaps: Vec<CaptureGapEvent> = Vec::new();

        // 1. Seed initial Polymarket orderbook checkpoint at t = 1_000_000_000 ns
        let init_cp = L2CheckpointEvent {
            venue_id: VenueId::Polymarket,
            instrument_iid: 500,
            market_uid: 250,
            exchange_ts_ms: 1_000,
            recv_ts_ns: 1_000_000_000,
            collector_session: 1,
            seq: 1,
            checkpoint_reason: CheckpointReason::SubscribeInit,
            book_hash: "0xinit".to_string(),
            bids_price_micros: vec![480_000, 500_000],
            bids_size_micros: vec![100_000_000, 50_000_000],
            asks_price_micros: vec![540_000, 520_000],
            asks_size_micros: vec![100_000_000, 40_000_000],
            dedup_key: 100,
        };
        if let MergerOutcome::EmittedPrimary(MarketDataEnvelope::L2Checkpoint(cp)) =
            merger.ingest_l2_checkpoint(init_cp)
        {
            stored_checkpoints.push(cp);
        }

        // 2. Stream 100 incremental updates across Leg A and Leg B, killing Leg A for sequences 40..60
        for step in 2..=100_u64 {
            let price = if step % 2 == 0 { 500_000 } else { 520_000 };
            let side = if step % 2 == 0 {
                BookSide::Bid
            } else {
                BookSide::Ask
            };
            let new_size = (50_000_000 + step * 1_000_000) as i64;
            let dedup = L2DeltaEvent::compute_dedup_key(
                VenueId::Polymarket,
                500,
                250,
                1_000 + step,
                side,
                false,
                PriceMicros(price),
                SizeMicros(new_size),
                new_size as u64,
                &format!("0xhash_{step}"),
            );

            // Leg A is alive outside [40, 60]
            if !(40..=60).contains(&step) {
                let leg_a_delta = L2DeltaEvent {
                    venue_id: VenueId::Polymarket,
                    instrument_iid: 500,
                    market_uid: 250,
                    exchange_ts_ms: 1_000 + step,
                    sending_ts_ms: 1_000 + step,
                    recv_ts_ns: 1_000_000_000 + step * 1_000_000,
                    ingest_ts_ns: 1_000_000_100 + step * 1_000_000,
                    collector_session: 1,
                    collector_leg: 0,
                    sid: 1,
                    seq: step,
                    side,
                    is_delta: false,
                    price_micros: PriceMicros(price),
                    size_micros: SizeMicros(new_size),
                    resulting_size_micros: new_size as u64,
                    best_bid_micros: PriceMicros(500_000),
                    best_ask_micros: PriceMicros(520_000),
                    is_bbo_verified: true,
                    event_hash: format!("0xhash_{step}"),
                    dedup_key: dedup,
                };
                for outcome in merger.ingest_l2_delta(leg_a_delta) {
                    match outcome {
                        MergerOutcome::EmittedPrimary(MarketDataEnvelope::L2Delta(d)) => {
                            stored_deltas.push(d)
                        }
                        MergerOutcome::GapRecorded(g) => stored_gaps.push(g),
                        _ => {}
                    }
                }
            }

            // Leg B stays up 100% of the time (arriving 500 microseconds after Leg A when both are up)
            let leg_b_delta = L2DeltaEvent {
                venue_id: VenueId::Polymarket,
                instrument_iid: 500,
                market_uid: 250,
                exchange_ts_ms: 1_000 + step,
                sending_ts_ms: 1_000 + step,
                recv_ts_ns: 1_000_500_000 + step * 1_000_000,
                ingest_ts_ns: 1_000_500_100 + step * 1_000_000,
                collector_session: 2,
                collector_leg: 1,
                sid: 1,
                seq: step,
                side,
                is_delta: false,
                price_micros: PriceMicros(price),
                size_micros: SizeMicros(new_size),
                resulting_size_micros: new_size as u64,
                best_bid_micros: PriceMicros(500_000),
                best_ask_micros: PriceMicros(520_000),
                is_bbo_verified: true,
                event_hash: format!("0xhash_{step}"),
                dedup_key: dedup,
            };
            for outcome in merger.ingest_l2_delta(leg_b_delta) {
                match outcome {
                    MergerOutcome::EmittedPrimary(MarketDataEnvelope::L2Delta(d)) => {
                        stored_deltas.push(d)
                    }
                    MergerOutcome::GapRecorded(g) => stored_gaps.push(g),
                    _ => {}
                }
            }
        }

        // Exactly 99 unique deltas (steps 2..=100) must be emitted despite Leg A outage on 40..=60
        assert_eq!(stored_deltas.len(), 99);

        // Write deltas to ZSTD(9) Parquet and read them back
        let tmp_parquet = NamedTempFile::new().unwrap();
        write_l2_deltas_parquet(tmp_parquet.path(), stored_deltas).unwrap();
        let parquet_deltas = read_l2_deltas_parquet(tmp_parquet.path()).unwrap();

        // Reconstruct orderbook at step 100 from the Parquet cold lakehouse + initial checkpoint
        let snap = PointInTimeReconstructor::reconstruct_at_recv_ns(
            500,
            2_000_000_000,
            &stored_checkpoints,
            &parquet_deltas,
            &stored_gaps,
        )
        .unwrap();

        assert_eq!(snap.deltas_applied, 99);
        let live_book = merger.canonical_book(500).unwrap();
        assert_eq!(snap.book.bids_descending(), live_book.bids_descending());
        assert_eq!(snap.book.asks_ascending(), live_book.asks_ascending());
        assert_eq!(snap.state_hash, live_book.compute_state_hash());
    }

    #[test]
    fn test_replay_missing_anchor_and_unhealed_gap_continuity_proof() {
        let err = PointInTimeReconstructor::reconstruct_at_recv_ns(999, 5_000_000, &[], &[], &[])
            .unwrap_err();
        assert_eq!(
            err,
            ReplayError::MissingAnchorCheckpoint {
                instrument_iid: 999,
                target_ts_ns: 5_000_000
            }
        );

        let cp = L2CheckpointEvent {
            venue_id: VenueId::Kalshi,
            instrument_iid: 10,
            market_uid: 5,
            exchange_ts_ms: 1_000,
            recv_ts_ns: 1_000_000_000,
            collector_session: 1,
            seq: 1,
            checkpoint_reason: CheckpointReason::SubscribeInit,
            book_hash: "h1".to_string(),
            bids_price_micros: vec![540_000],
            bids_size_micros: vec![100_000_000],
            asks_price_micros: vec![560_000],
            asks_size_micros: vec![100_000_000],
            dedup_key: 1,
        };
        let unhealed_gap = CaptureGapEvent {
            venue_id: VenueId::Kalshi,
            instrument_iid: 10,
            collector_session: 1,
            collector_leg: 0,
            shard_id: 1,
            gap_type: alkera_schema::GapType::SeqGap,
            gap_start_ns: 1_100_000_000,
            gap_resolved_ns: 1_200_000_000,
            expected_seq: 2,
            observed_seq: 5,
            healed_by_twin_leg: false,
            recovery_checkpoint_seq: 5,
        };

        let snap = PointInTimeReconstructor::reconstruct_at_recv_ns(
            10,
            1_500_000_000,
            &[cp],
            &[],
            &[unhealed_gap],
        )
        .unwrap();
        assert!(matches!(
            snap.continuity,
            ContinuityProof::UnhealedGapDetected { .. }
        ));
    }
}

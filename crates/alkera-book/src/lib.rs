use alkera_schema::{
    BookSide, CheckpointReason, L2CheckpointEvent, L2DeltaEvent, PriceMicros, VenueId,
};
use std::collections::BTreeMap;
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BookError {
    #[error("sequence gap on sid {sid}: expected {expected_seq}, observed {observed_seq}")]
    SequenceGap {
        sid: u32,
        expected_seq: u64,
        observed_seq: u64,
    },
    #[error("server BBO mismatch on instrument {instrument_iid}: local=({local_bid:?}, {local_ask:?}), server=({server_bid:?}, {server_ask:?})")]
    BboMismatch {
        instrument_iid: u32,
        local_bid: Option<PriceMicros>,
        local_ask: Option<PriceMicros>,
        server_bid: Option<PriceMicros>,
        server_ask: Option<PriceMicros>,
    },
    #[error("mismatched parallel checkpoint arrays for instrument {0}")]
    MalformedCheckpoint(u32),
}

/// Deterministic fixed-point L2 orderbook supporting both Kalshi (`YES`/`NO` bid duality)
/// and Polymarket (`BUY`/`SELL` absolute level updates + server BBO/hash verification).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlatOrderBook {
    pub venue_id: VenueId,
    pub instrument_iid: u32,
    pub market_uid: u32,
    /// Canonical bids mapped by price (`PriceMicros` -> `u64` size in micro-units).
    /// Iterated in reverse (`iter().next_back()`) to yield highest bid first.
    bids: BTreeMap<PriceMicros, u64>,
    /// Canonical asks mapped by price (`PriceMicros` -> `u64` size in micro-units).
    /// Iterated forward (`iter().next()`) to yield lowest ask first.
    asks: BTreeMap<PriceMicros, u64>,
    pub last_seq: u64,
    pub last_exchange_ts_ms: u64,
    pub last_recv_ts_ns: u64,
    pub last_checkpoint_recv_ns: u64,
    pub is_synced: bool,
}

impl FlatOrderBook {
    pub fn new(venue_id: VenueId, instrument_iid: u32, market_uid: u32) -> Self {
        Self {
            venue_id,
            instrument_iid,
            market_uid,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_seq: 0,
            last_exchange_ts_ms: 0,
            last_recv_ts_ns: 0,
            last_checkpoint_recv_ns: 0,
            is_synced: false,
        }
    }

    /// Hydrates the L2 orderbook from a `L2CheckpointEvent` (parallel sorted arrays).
    pub fn apply_checkpoint(&mut self, cp: &L2CheckpointEvent) -> Result<(), BookError> {
        if cp.bids_price_micros.len() != cp.bids_size_micros.len()
            || cp.asks_price_micros.len() != cp.asks_size_micros.len()
        {
            return Err(BookError::MalformedCheckpoint(cp.instrument_iid));
        }

        self.bids.clear();
        self.asks.clear();

        for (&price, &size) in cp.bids_price_micros.iter().zip(cp.bids_size_micros.iter()) {
            if size > 0 {
                self.bids.insert(PriceMicros(price), size);
            }
        }
        for (&price, &size) in cp.asks_price_micros.iter().zip(cp.asks_size_micros.iter()) {
            if size > 0 {
                self.asks.insert(PriceMicros(price), size);
            }
        }

        self.last_seq = cp.seq;
        self.last_exchange_ts_ms = cp.exchange_ts_ms;
        self.last_recv_ts_ns = cp.recv_ts_ns;
        self.last_checkpoint_recv_ns = cp.recv_ts_ns;
        self.is_synced = true;
        Ok(())
    }

    /// Hydrates a Kalshi `orderbook_snapshot` where `yes_levels` are `(yes_bid_price, size)`
    /// and `no_levels` are `(no_bid_price, size)` which project to canonical `YES` asks at `$1.00 - no_bid_price`.
    pub fn apply_kalshi_snapshot(
        &mut self,
        seq: u64,
        exchange_ts_ms: u64,
        recv_ts_ns: u64,
        yes_levels: &[(PriceMicros, u64)],
        no_levels: &[(PriceMicros, u64)],
    ) {
        self.bids.clear();
        self.asks.clear();

        for &(price, size) in yes_levels {
            if size > 0 {
                self.bids.insert(price, size);
            }
        }
        for &(no_price, size) in no_levels {
            if size > 0 {
                let ask_price = no_price.complement();
                self.asks.insert(ask_price, size);
            }
        }

        self.last_seq = seq;
        self.last_exchange_ts_ms = exchange_ts_ms;
        self.last_recv_ts_ns = recv_ts_ns;
        self.last_checkpoint_recv_ns = recv_ts_ns;
        self.is_synced = true;
    }

    /// Hydrates a Polymarket `book` snapshot (which arrives worst-first on the wire) into canonical sorted maps.
    pub fn apply_polymarket_snapshot(
        &mut self,
        seq: u64,
        exchange_ts_ms: u64,
        recv_ts_ns: u64,
        raw_bids: &[(PriceMicros, u64)],
        raw_asks: &[(PriceMicros, u64)],
    ) {
        self.bids.clear();
        self.asks.clear();

        for &(price, size) in raw_bids {
            if size > 0 {
                self.bids.insert(price, size);
            }
        }
        for &(price, size) in raw_asks {
            if size > 0 {
                self.asks.insert(price, size);
            }
        }

        self.last_seq = seq;
        self.last_exchange_ts_ms = exchange_ts_ms;
        self.last_recv_ts_ns = recv_ts_ns;
        self.last_checkpoint_recv_ns = recv_ts_ns;
        self.is_synced = true;
    }

    /// Applies an incremental `L2DeltaEvent` and returns the resulting aggregate size at the modified price level.
    /// Validates sequence continuity (when `enforce_seq` is true) and server BBO agreement (when `best_bid_micros` / `best_ask_micros` are non-zero).
    pub fn apply_delta(
        &mut self,
        delta: &L2DeltaEvent,
        enforce_seq: bool,
    ) -> Result<u64, BookError> {
        if enforce_seq && self.last_seq > 0 && delta.seq > self.last_seq + 1 {
            self.is_synced = false;
            return Err(BookError::SequenceGap {
                sid: delta.sid,
                expected_seq: self.last_seq + 1,
                observed_seq: delta.seq,
            });
        }

        let (target_map_is_bid, effective_price) = match delta.side {
            BookSide::Bid | BookSide::KalshiYes => (true, delta.price_micros),
            BookSide::Ask => (false, delta.price_micros),
            BookSide::KalshiNo => (false, delta.price_micros.complement()),
        };

        let map = if target_map_is_bid {
            &mut self.bids
        } else {
            &mut self.asks
        };

        let resulting_size = if delta.is_delta {
            let current = map.get(&effective_price).copied().unwrap_or(0) as i64;
            let updated = (current + delta.size_micros.0).max(0) as u64;
            if updated == 0 {
                map.remove(&effective_price);
            } else {
                map.insert(effective_price, updated);
            }
            updated
        } else {
            let updated = delta.size_micros.0.max(0) as u64;
            if updated == 0 {
                map.remove(&effective_price);
            } else {
                map.insert(effective_price, updated);
            }
            updated
        };

        if delta.seq > self.last_seq {
            self.last_seq = delta.seq;
        }
        self.last_exchange_ts_ms = delta.exchange_ts_ms;
        self.last_recv_ts_ns = delta.recv_ts_ns;

        // Verify server-reported BBO on Polymarket `price_change` events when provided
        if delta.best_bid_micros.0 > 0 || delta.best_ask_micros.0 > 0 {
            let local_bid = self.best_bid().map(|(p, _)| p);
            let local_ask = self.best_ask().map(|(p, _)| p);
            let bid_mismatch =
                delta.best_bid_micros.0 > 0 && local_bid != Some(delta.best_bid_micros);
            let ask_mismatch =
                delta.best_ask_micros.0 > 0 && local_ask != Some(delta.best_ask_micros);
            if bid_mismatch || ask_mismatch {
                self.is_synced = false;
                return Err(BookError::BboMismatch {
                    instrument_iid: self.instrument_iid,
                    local_bid,
                    local_ask,
                    server_bid: (delta.best_bid_micros.0 > 0).then_some(delta.best_bid_micros),
                    server_ask: (delta.best_ask_micros.0 > 0).then_some(delta.best_ask_micros),
                });
            }
        }

        Ok(resulting_size)
    }

    /// Returns `(best_bid_price, best_bid_size)`.
    #[inline]
    pub fn best_bid(&self) -> Option<(PriceMicros, u64)> {
        self.bids.iter().next_back().map(|(&p, &s)| (p, s))
    }

    /// Returns `(best_ask_price, best_ask_size)`.
    #[inline]
    pub fn best_ask(&self) -> Option<(PriceMicros, u64)> {
        self.asks.iter().next().map(|(&p, &s)| (p, s))
    }

    /// Returns bids sorted best-to-worst (descending price).
    pub fn bids_descending(&self) -> Vec<(PriceMicros, u64)> {
        self.bids.iter().rev().map(|(&p, &s)| (p, s)).collect()
    }

    /// Returns asks sorted best-to-worst (ascending price).
    pub fn asks_ascending(&self) -> Vec<(PriceMicros, u64)> {
        self.asks.iter().map(|(&p, &s)| (p, s)).collect()
    }

    /// Computes a deterministic 64-bit XXH3 checksum over all active bid and ask levels.
    pub fn compute_state_hash(&self) -> String {
        let mut buf = Vec::with_capacity((self.bids.len() + self.asks.len()) * 12 + 8);
        buf.extend_from_slice(&self.instrument_iid.to_le_bytes());
        for (&p, &s) in self.bids.iter().rev() {
            buf.push(b'B');
            buf.extend_from_slice(&p.0.to_le_bytes());
            buf.extend_from_slice(&s.to_le_bytes());
        }
        for (&p, &s) in self.asks.iter() {
            buf.push(b'A');
            buf.extend_from_slice(&p.0.to_le_bytes());
            buf.extend_from_slice(&s.to_le_bytes());
        }
        format!("{:016x}", xxh3_64(&buf))
    }

    /// Checks whether a candidate snapshot (`raw_bids`, `raw_asks`) matches the current in-memory L2 state.
    /// Used by the Polymarket Post-Trade `book` Compactor to suppress redundant multi-KB snapshot dumps.
    pub fn matches_levels(
        &self,
        raw_bids: &[(PriceMicros, u64)],
        raw_asks: &[(PriceMicros, u64)],
    ) -> bool {
        let non_zero_bids: Vec<(PriceMicros, u64)> =
            raw_bids.iter().copied().filter(|&(_, s)| s > 0).collect();
        let non_zero_asks: Vec<(PriceMicros, u64)> =
            raw_asks.iter().copied().filter(|&(_, s)| s > 0).collect();

        if non_zero_bids.len() != self.bids.len() || non_zero_asks.len() != self.asks.len() {
            return false;
        }
        for (p, s) in non_zero_bids {
            if self.bids.get(&p).copied() != Some(s) {
                return false;
            }
        }
        for (p, s) in non_zero_asks {
            if self.asks.get(&p).copied() != Some(s) {
                return false;
            }
        }
        true
    }

    /// Exports the current orderbook state as a single-row `L2CheckpointEvent` (parallel sorted arrays).
    pub fn to_checkpoint(
        &self,
        collector_session: u64,
        reason: CheckpointReason,
        override_hash: Option<String>,
    ) -> L2CheckpointEvent {
        let mut bids_price_micros = Vec::with_capacity(self.bids.len());
        let mut bids_size_micros = Vec::with_capacity(self.bids.len());
        for (&p, &s) in self.bids.iter().rev() {
            bids_price_micros.push(p.0);
            bids_size_micros.push(s);
        }

        let mut asks_price_micros = Vec::with_capacity(self.asks.len());
        let mut asks_size_micros = Vec::with_capacity(self.asks.len());
        for (&p, &s) in self.asks.iter() {
            asks_price_micros.push(p.0);
            asks_size_micros.push(s);
        }

        let book_hash = override_hash.unwrap_or_else(|| self.compute_state_hash());
        let dedup_key = L2CheckpointEvent::compute_dedup_key(
            self.venue_id,
            self.instrument_iid,
            self.last_exchange_ts_ms,
            reason,
            &book_hash,
        );

        L2CheckpointEvent {
            venue_id: self.venue_id,
            instrument_iid: self.instrument_iid,
            market_uid: self.market_uid,
            exchange_ts_ms: self.last_exchange_ts_ms,
            recv_ts_ns: self.last_recv_ts_ns,
            collector_session,
            seq: self.last_seq,
            checkpoint_reason: reason,
            book_hash,
            bids_price_micros,
            bids_size_micros,
            asks_price_micros,
            asks_size_micros,
            dedup_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alkera_schema::SizeMicros;

    #[test]
    fn test_kalshi_yes_no_duality_and_delta_application() {
        let mut book = FlatOrderBook::new(VenueId::Kalshi, 10, 5);
        // YES bid @ $0.54 (100 contracts), NO bid @ $0.44 (200 contracts -> YES ask @ $0.56)
        book.apply_kalshi_snapshot(
            1,
            1_000,
            1_000_000,
            &[(PriceMicros(540_000), 100_000_000)],
            &[(PriceMicros(440_000), 200_000_000)],
        );

        assert_eq!(book.best_bid(), Some((PriceMicros(540_000), 100_000_000)));
        assert_eq!(book.best_ask(), Some((PriceMicros(560_000), 200_000_000)));

        // Apply NO bid delta of -50 contracts at $0.44 -> YES ask at $0.56 becomes 150 contracts
        let delta = L2DeltaEvent {
            venue_id: VenueId::Kalshi,
            instrument_iid: 10,
            market_uid: 5,
            exchange_ts_ms: 1_005,
            sending_ts_ms: 1_005,
            recv_ts_ns: 1_005_000,
            ingest_ts_ns: 1_005_100,
            collector_session: 1,
            collector_leg: 0,
            sid: 1,
            seq: 2,
            side: BookSide::KalshiNo,
            is_delta: true,
            price_micros: PriceMicros(440_000),
            size_micros: SizeMicros(-50_000_000),
            resulting_size_micros: 150_000_000,
            best_bid_micros: PriceMicros::ZERO,
            best_ask_micros: PriceMicros::ZERO,
            is_bbo_verified: true,
            event_hash: String::new(),
            dedup_key: 123,
        };
        let res = book.apply_delta(&delta, true).unwrap();
        assert_eq!(res, 150_000_000);
        assert_eq!(book.best_ask(), Some((PriceMicros(560_000), 150_000_000)));
    }

    #[test]
    fn test_polymarket_worst_first_snapshot_and_bbo_verification() {
        let mut book = FlatOrderBook::new(VenueId::Polymarket, 77, 20);
        // Polymarket sends worst-first on the wire: 0.48, 0.49, 0.50 for bids; 0.54, 0.53, 0.52 for asks
        book.apply_polymarket_snapshot(
            1,
            2_000,
            2_000_000,
            &[
                (PriceMicros(480_000), 30_000_000),
                (PriceMicros(490_000), 20_000_000),
                (PriceMicros(500_000), 15_000_000),
            ],
            &[
                (PriceMicros(540_000), 10_000_000),
                (PriceMicros(530_000), 60_000_000),
                (PriceMicros(520_000), 25_000_000),
            ],
        );

        assert_eq!(book.best_bid(), Some((PriceMicros(500_000), 15_000_000)));
        assert_eq!(book.best_ask(), Some((PriceMicros(520_000), 25_000_000)));
    }

    #[test]
    fn test_kalshi_sequence_gap_level_deletion_and_state_hash_determinism() {
        let mut book_a = FlatOrderBook::new(VenueId::Kalshi, 10, 5);
        let mut book_b = FlatOrderBook::new(VenueId::Kalshi, 10, 5);

        // Insert same levels in opposite order on book_a and book_b -> state hashes must be identical
        book_a.apply_kalshi_snapshot(
            10,
            1_000,
            1_000_000,
            &[
                (PriceMicros(510_000), 10_000_000),
                (PriceMicros(540_000), 50_000_000),
            ],
            &[
                (PriceMicros(430_000), 30_000_000),
                (PriceMicros(440_000), 20_000_000),
            ],
        );
        book_b.apply_kalshi_snapshot(
            10,
            1_000,
            1_000_000,
            &[
                (PriceMicros(540_000), 50_000_000),
                (PriceMicros(510_000), 10_000_000),
            ],
            &[
                (PriceMicros(440_000), 20_000_000),
                (PriceMicros(430_000), 30_000_000),
            ],
        );
        assert_eq!(book_a.compute_state_hash(), book_b.compute_state_hash());

        // Delete the best YES bid ($0.54) by applying a delta of -50.00 contracts (seq 11)
        let delete_delta = L2DeltaEvent {
            venue_id: VenueId::Kalshi,
            instrument_iid: 10,
            market_uid: 5,
            exchange_ts_ms: 1_001,
            sending_ts_ms: 1_001,
            recv_ts_ns: 1_001_000,
            ingest_ts_ns: 1_001_100,
            collector_session: 1,
            collector_leg: 0,
            sid: 2,
            seq: 11,
            side: BookSide::KalshiYes,
            is_delta: true,
            price_micros: PriceMicros(540_000),
            size_micros: SizeMicros(-50_000_000),
            resulting_size_micros: 0,
            best_bid_micros: PriceMicros::ZERO,
            best_ask_micros: PriceMicros::ZERO,
            is_bbo_verified: true,
            event_hash: String::new(),
            dedup_key: 1,
        };
        assert_eq!(book_a.apply_delta(&delete_delta, true).unwrap(), 0);
        // Best bid must now fall back to $0.51
        assert_eq!(book_a.best_bid(), Some((PriceMicros(510_000), 10_000_000)));

        // Applying seq 13 after seq 11 with enforce_seq = true must return SequenceGap and mark book unsynced
        let mut gap_delta = delete_delta.clone();
        gap_delta.seq = 13;
        let err = book_a.apply_delta(&gap_delta, true).unwrap_err();
        assert_eq!(
            err,
            BookError::SequenceGap {
                sid: 2,
                expected_seq: 12,
                observed_seq: 13
            }
        );
        assert!(!book_a.is_synced);
    }
}

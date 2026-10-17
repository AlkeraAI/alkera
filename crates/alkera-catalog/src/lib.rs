use alkera_schema::{PriceMicros, SchemaError, VenueId};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("schema error: {0}")]
    Schema(#[from] SchemaError),
    #[error("invalid stringified JSON array in Gamma market metadata: {0}")]
    InvalidGammaJsonArray(String),
    #[error("mismatched Polymarket outcomes ({0}) vs clobTokenIds ({1})")]
    MismatchedPolymarketOutcomes(usize, usize),
    #[error("instrument not found for iid {0}")]
    InstrumentNotFound(u32),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ActivityTier {
    HotWs = 1,
    WarmWs = 2,
    DormantWatched = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PriorityClass {
    Critical,
    High,
    Standard,
    Bulk,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentMeta {
    pub instrument_iid: u32,
    pub venue_id: VenueId,
    pub market_uid: u32,
    pub native_market_id: String,
    pub native_instrument_id: String,
    pub outcome_index: u16,
    pub outcome_label: String,
    pub complement_iid: Option<u32>,
    pub tick_size_micros: PriceMicros,
    pub activity_tier: ActivityTier,
    pub is_active: bool,
    pub exchange_index: Option<String>,
}

/// Payload emitted by the PostgreSQL `trg_instrument_upserted` trigger on `LISTEN catalog_instrument_updates`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstrumentUpsertNotification {
    pub instrument_iid: u32,
    pub venue_id: u8,
    pub market_uid: u32,
    pub native_instrument_id: String,
    pub outcome_index: u16,
    pub complement_iid: Option<u32>,
    pub activity_tier: u8,
    pub is_active: bool,
}

/// Lock-free bi-directional surrogate ID interner (`instrument_iid <-> native_instrument_id`).
/// Readers on the hot market-data path perform wait-free `O(1)` indexing via `ArcSwap<Vec<Option<InstrumentMeta>>>`.
pub struct InstrumentInterner {
    native_to_iid: DashMap<(VenueId, String), u32>,
    iid_table: ArcSwap<Vec<Option<InstrumentMeta>>>,
    next_iid: AtomicU32,
    next_market_uid: AtomicU32,
    write_lock: Mutex<()>,
}

impl Default for InstrumentInterner {
    fn default() -> Self {
        Self::new()
    }
}

impl InstrumentInterner {
    pub fn new() -> Self {
        Self {
            native_to_iid: DashMap::with_capacity(65_536),
            iid_table: ArcSwap::from_pointee(vec![None; 1024]),
            next_iid: AtomicU32::new(1),
            next_market_uid: AtomicU32::new(1),
            write_lock: Mutex::new(()),
        }
    }

    /// Wait-free `O(1)` lookup by surrogate `instrument_iid` on the hot collector/merger path.
    #[inline]
    pub fn get_by_iid(&self, instrument_iid: u32) -> Option<InstrumentMeta> {
        let guard = self.iid_table.load();
        guard
            .get(instrument_iid as usize)
            .and_then(|slot| slot.clone())
    }

    /// Fast concurrent hash lookup from `(VenueId, native_instrument_id)` to `instrument_iid`.
    #[inline]
    pub fn get_iid(&self, venue_id: VenueId, native_instrument_id: &str) -> Option<u32> {
        self.native_to_iid
            .get(&(venue_id, native_instrument_id.to_string()))
            .map(|r| *r.value())
    }

    /// Upserts an `InstrumentMeta` into the lock-free `ArcSwap` table and `DashMap` index.
    pub fn upsert(&self, meta: InstrumentMeta) {
        let _guard = self.write_lock.lock();
        let iid = meta.instrument_iid;
        self.native_to_iid
            .insert((meta.venue_id, meta.native_instrument_id.clone()), iid);

        let current = self.iid_table.load_full();
        let mut next_vec = (*current).clone();
        if (iid as usize) >= next_vec.len() {
            next_vec.resize((iid as usize + 1).next_power_of_two(), None);
        }
        next_vec[iid as usize] = Some(meta);
        self.iid_table.store(Arc::new(next_vec));

        let mut cur_max = self.next_iid.load(Ordering::Relaxed);
        while iid >= cur_max {
            match self.next_iid.compare_exchange_weak(
                cur_max,
                iid + 1,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur_max = actual,
            }
        }
    }

    /// Registers a Kalshi binary/scalar market and atomically allocates `<ticker>:YES` and `<ticker>:NO`
    /// surrogate `instrument_iid`s linked via `complement_iid`.
    pub fn register_kalshi_market(
        &self,
        market_ticker: &str,
        tick_size_micros: PriceMicros,
        exchange_index: Option<String>,
        activity_tier: ActivityTier,
    ) -> (u32, u32, u32) {
        let yes_native = format!("{market_ticker}:YES");
        let no_native = format!("{market_ticker}:NO");
        if let (Some(yes_iid), Some(no_iid)) = (
            self.get_iid(VenueId::Kalshi, &yes_native),
            self.get_iid(VenueId::Kalshi, &no_native),
        ) {
            let market_uid = self
                .get_by_iid(yes_iid)
                .map(|m| m.market_uid)
                .unwrap_or(0);
            return (market_uid, yes_iid, no_iid);
        }

        let market_uid = self.next_market_uid.fetch_add(1, Ordering::SeqCst);
        let yes_iid = self.next_iid.fetch_add(1, Ordering::SeqCst);
        let no_iid = self.next_iid.fetch_add(1, Ordering::SeqCst);

        let yes_meta = InstrumentMeta {
            instrument_iid: yes_iid,
            venue_id: VenueId::Kalshi,
            market_uid,
            native_market_id: market_ticker.to_string(),
            native_instrument_id: yes_native,
            outcome_index: 0,
            outcome_label: "Yes".to_string(),
            complement_iid: Some(no_iid),
            tick_size_micros,
            activity_tier,
            is_active: true,
            exchange_index: exchange_index.clone(),
        };

        let no_meta = InstrumentMeta {
            instrument_iid: no_iid,
            venue_id: VenueId::Kalshi,
            market_uid,
            native_market_id: market_ticker.to_string(),
            native_instrument_id: no_native,
            outcome_index: 1,
            outcome_label: "No".to_string(),
            complement_iid: Some(yes_iid),
            tick_size_micros,
            activity_tier,
            is_active: true,
            exchange_index,
        };

        self.upsert(yes_meta);
        self.upsert(no_meta);
        (market_uid, yes_iid, no_iid)
    }

    /// Registers a Polymarket market from Gamma API fields, safely decoding stringified JSON arrays
    /// (`outcomes: "[\"Chargers\", \"Chiefs\"]"` and `clobTokenIds: "[\"2597...\", \"5816...\"]"`).
    pub fn register_polymarket_gamma_market(
        &self,
        gamma_market_id: &str,
        outcomes_json_str: &str,
        clob_token_ids_json_str: &str,
        tick_size_str: &str,
        activity_tier: ActivityTier,
    ) -> Result<(u32, Vec<u32>), CatalogError> {
        let outcomes: Vec<String> = serde_json::from_str(outcomes_json_str)
            .map_err(|e| CatalogError::InvalidGammaJsonArray(e.to_string()))?;
        let token_ids: Vec<String> = serde_json::from_str(clob_token_ids_json_str)
            .map_err(|e| CatalogError::InvalidGammaJsonArray(e.to_string()))?;

        if outcomes.is_empty() || outcomes.len() != token_ids.len() {
            return Err(CatalogError::MismatchedPolymarketOutcomes(
                outcomes.len(),
                token_ids.len(),
            ));
        }

        let tick_size_micros = PriceMicros::parse_ascii(tick_size_str)?;
        let market_uid = self.next_market_uid.fetch_add(1, Ordering::SeqCst);

        let mut allocated_iids = Vec::with_capacity(token_ids.len());
        for token_id in &token_ids {
            if let Some(existing) = self.get_iid(VenueId::Polymarket, token_id) {
                allocated_iids.push(existing);
            } else {
                allocated_iids.push(self.next_iid.fetch_add(1, Ordering::SeqCst));
            }
        }

        for (idx, (label, token_id)) in outcomes.into_iter().zip(token_ids.into_iter()).enumerate() {
            let iid = allocated_iids[idx];
            let complement_iid = if allocated_iids.len() == 2 {
                Some(allocated_iids[1 - idx])
            } else {
                None
            };
            self.upsert(InstrumentMeta {
                instrument_iid: iid,
                venue_id: VenueId::Polymarket,
                market_uid,
                native_market_id: gamma_market_id.to_string(),
                native_instrument_id: token_id,
                outcome_index: idx as u16,
                outcome_label: label,
                complement_iid,
                tick_size_micros,
                activity_tier,
                is_active: true,
                exchange_index: None,
            });
        }

        Ok((market_uid, allocated_iids))
    }

    /// Promotes a dormant long-tail instrument (`ActivityTier::DormantWatched`) to `ActivityTier::HotWs`
    /// when the global `new_market` or RTDS `activity` watcher observes live order/trade activity.
    pub fn promote_to_hot(&self, instrument_iid: u32) -> Result<InstrumentMeta, CatalogError> {
        let mut meta = self
            .get_by_iid(instrument_iid)
            .ok_or(CatalogError::InstrumentNotFound(instrument_iid))?;
        meta.activity_tier = ActivityTier::HotWs;
        self.upsert(meta.clone());
        Ok(meta)
    }

    /// Applies a PostgreSQL `LISTEN catalog_instrument_updates` payload in real time.
    pub fn apply_pg_notification(&self, payload_json: &str) -> Result<u32, CatalogError> {
        let notif: InstrumentUpsertNotification = serde_json::from_str(payload_json)?;
        let venue_id = match notif.venue_id {
            1 => VenueId::Kalshi,
            _ => VenueId::Polymarket,
        };
        let tier = match notif.activity_tier {
            1 => ActivityTier::HotWs,
            2 => ActivityTier::WarmWs,
            _ => ActivityTier::DormantWatched,
        };
        let existing = self.get_by_iid(notif.instrument_iid);
        let meta = InstrumentMeta {
            instrument_iid: notif.instrument_iid,
            venue_id,
            market_uid: notif.market_uid,
            native_market_id: existing
                .as_ref()
                .map(|m| m.native_market_id.clone())
                .unwrap_or_else(|| notif.native_instrument_id.clone()),
            native_instrument_id: notif.native_instrument_id,
            outcome_index: notif.outcome_index,
            outcome_label: existing
                .as_ref()
                .map(|m| m.outcome_label.clone())
                .unwrap_or_else(|| "Outcome".to_string()),
            complement_iid: notif.complement_iid,
            tick_size_micros: existing
                .map(|m| m.tick_size_micros)
                .unwrap_or(PriceMicros::ONE_CENT),
            activity_tier: tier,
            is_active: notif.is_active,
            exchange_index: None,
        };
        self.upsert(meta);
        Ok(notif.instrument_iid)
    }
}

/// Sharded WebSocket subscription planner enforcing Kalshi and Polymarket per-socket and per-frame ceilings.
#[derive(Clone, Debug)]
pub struct ShardPlannerConfig {
    pub kalshi_max_markets_per_socket: usize,
    pub kalshi_max_markets_per_sid: usize,
    pub polymarket_max_assets_per_socket: usize,
    pub polymarket_max_assets_per_frame: usize,
}

impl Default for ShardPlannerConfig {
    fn default() -> Self {
        Self {
            kalshi_max_markets_per_socket: 5_000,
            kalshi_max_markets_per_sid: 500,
            polymarket_max_assets_per_socket: 500,
            polymarket_max_assets_per_frame: 250,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketShardPlan {
    pub venue_id: VenueId,
    pub priority: PriorityClass,
    pub socket_id: usize,
    /// Each inner `Vec<String>` is one subscription frame / `sid` batch respecting venue limits.
    pub subscription_frames: Vec<Vec<String>>,
}

pub struct ShardedSubscriptionRouter {
    config: ShardPlannerConfig,
}

impl ShardedSubscriptionRouter {
    pub fn new(config: ShardPlannerConfig) -> Self {
        Self { config }
    }

    /// Assigns instruments to priority-isolated WebSocket connection shards and slices them
    /// into protocol-compliant subscription frames (max 500 tickers/sid on Kalshi; max 500 assets/socket
    /// and 250 assets/frame on Polymarket).
    pub fn build_shard_plan(
        &self,
        venue_id: VenueId,
        priority: PriorityClass,
        native_ids: &[String],
    ) -> Vec<SocketShardPlan> {
        if native_ids.is_empty() {
            return Vec::new();
        }

        let (max_per_socket, max_per_frame) = match venue_id {
            VenueId::Kalshi => (
                self.config.kalshi_max_markets_per_socket,
                self.config.kalshi_max_markets_per_sid,
            ),
            VenueId::Polymarket => (
                self.config.polymarket_max_assets_per_socket,
                self.config.polymarket_max_assets_per_frame,
            ),
        };

        let num_sockets = native_ids.len().div_ceil(max_per_socket).max(1);
        let mut buckets: BTreeMap<usize, Vec<String>> = BTreeMap::new();

        // Sort deterministically by consistent hash ring token so rebalances minimize movement,
        // while strictly capping each socket at `max_per_socket` (Bounded-Load Consistent Hashing).
        let mut hashed: Vec<(u64, &String)> = native_ids
            .iter()
            .map(|id| (xxh3_64(id.as_bytes()), id))
            .collect();
        hashed.sort_unstable_by_key(|(h, _)| *h);

        for (_, id) in hashed {
            let preferred = (xxh3_64(id.as_bytes()) as usize) % num_sockets;
            let mut chosen = preferred;
            for offset in 0..num_sockets {
                let candidate = (preferred + offset) % num_sockets;
                let len = buckets.get(&candidate).map(|v| v.len()).unwrap_or(0);
                if len < max_per_socket {
                    chosen = candidate;
                    break;
                }
            }
            buckets.entry(chosen).or_default().push(id.clone());
        }

        buckets
            .into_iter()
            .map(|(socket_id, ids)| {
                let frames = ids
                    .chunks(max_per_frame)
                    .map(|chunk| chunk.to_vec())
                    .collect();
                SocketShardPlan {
                    venue_id,
                    priority,
                    socket_id,
                    subscription_frames: frames,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interner_kalshi_and_polymarket_registration() {
        let interner = InstrumentInterner::new();
        let (k_mkt, yes_iid, no_iid) = interner.register_kalshi_market(
            "KXBTC-26OCT17-T95000",
            PriceMicros::ONE_CENT,
            Some("shard-0".to_string()),
            ActivityTier::HotWs,
        );
        assert_eq!(k_mkt, 1);
        assert_eq!(
            interner.get_by_iid(yes_iid).unwrap().complement_iid,
            Some(no_iid)
        );
        assert_eq!(
            interner.get_by_iid(no_iid).unwrap().complement_iid,
            Some(yes_iid)
        );

        let (p_mkt, poly_iids) = interner
            .register_polymarket_gamma_market(
                "4329891",
                "[\"Chargers\", \"Chiefs\"]",
                "[\"25975613657668487250673927756557799100593582472155421789800705293576427824049\", \"58167714289864717825087991545150213770744040987591942526721915426213133665357\"]",
                "0.01",
                ActivityTier::DormantWatched,
            )
            .unwrap();
        assert_eq!(p_mkt, 2);
        assert_eq!(poly_iids.len(), 2);

        let promoted = interner.promote_to_hot(poly_iids[1]).unwrap();
        assert_eq!(promoted.activity_tier, ActivityTier::HotWs);
        assert_eq!(promoted.outcome_label, "Chiefs");
    }

    #[test]
    fn test_polymarket_500_per_socket_and_250_per_frame_sharding() {
        let router = ShardedSubscriptionRouter::new(ShardPlannerConfig::default());
        let assets: Vec<String> = (0..1_200).map(|i| format!("token_{i:070}")).collect();
        let plans = router.build_shard_plan(VenueId::Polymarket, PriorityClass::Critical, &assets);

        // 1,200 assets with max 500 per socket -> 3 sockets
        assert_eq!(plans.len(), 3);
        let total_assigned: usize = plans
            .iter()
            .flat_map(|p| p.subscription_frames.iter())
            .map(|f| f.len())
            .sum();
        assert_eq!(total_assigned, 1_200);

        for plan in &plans {
            let socket_assets: usize = plan.subscription_frames.iter().map(|f| f.len()).sum();
            assert!(socket_assets <= 500);
            for frame in &plan.subscription_frames {
                assert!(frame.len() <= 250);
            }
        }
    }

    #[test]
    fn test_kalshi_idempotent_registration_pg_notify_and_large_shard_plan() {
        let interner = InstrumentInterner::new();
        let (mkt_1, yes_1, no_1) = interner.register_kalshi_market(
            "KXCPI-26OCT-T03",
            PriceMicros::ONE_CENT,
            Some("shard-1".to_string()),
            ActivityTier::HotWs,
        );
        // Re-registering the same Kalshi market must be idempotent
        let (mkt_2, yes_2, no_2) = interner.register_kalshi_market(
            "KXCPI-26OCT-T03",
            PriceMicros::ONE_CENT,
            Some("shard-1".to_string()),
            ActivityTier::HotWs,
        );
        assert_eq!((mkt_1, yes_1, no_1), (mkt_2, yes_2, no_2));

        // Test PostgreSQL LISTEN/NOTIFY outbox update
        let pg_json = serde_json::json!({
            "instrument_iid": yes_1,
            "venue_id": 1,
            "market_uid": mkt_1,
            "native_instrument_id": "KXCPI-26OCT-T03:YES",
            "outcome_index": 0,
            "complement_iid": no_1,
            "activity_tier": 2,
            "is_active": false
        })
        .to_string();
        interner.apply_pg_notification(&pg_json).unwrap();
        let updated = interner.get_by_iid(yes_1).unwrap();
        assert_eq!(updated.activity_tier, ActivityTier::WarmWs);
        assert!(!updated.is_active);

        // Test Kalshi sharding for 12,000 open markets (max 5,000/socket, max 500/sid frame)
        let router = ShardedSubscriptionRouter::new(ShardPlannerConfig::default());
        let tickers: Vec<String> = (0..12_000).map(|i| format!("KXTEST-26OCT-M{i:05}")).collect();
        let k_plans = router.build_shard_plan(VenueId::Kalshi, PriorityClass::Critical, &tickers);
        assert_eq!(k_plans.len(), 3);
        let total_kalshi: usize = k_plans
            .iter()
            .flat_map(|p| p.subscription_frames.iter())
            .map(|f| f.len())
            .sum();
        assert_eq!(total_kalshi, 12_000);
        for plan in &k_plans {
            let socket_total: usize = plan.subscription_frames.iter().map(|f| f.len()).sum();
            assert!(socket_total <= 5_000);
            for frame in &plan.subscription_frames {
                assert!(frame.len() <= 500);
            }
        }
    }
}

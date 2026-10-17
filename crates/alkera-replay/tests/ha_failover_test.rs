use alkera_catalog::{ActivityTier, InstrumentInterner};
use alkera_merger::{ActiveActiveMerger, MergerOutcome};
use alkera_replay::{ContinuityProof, PointInTimeReconstructor};
use alkera_schema::{
    BookSide, CheckpointReason, L2CheckpointEvent, L2DeltaEvent, MarketDataEnvelope, PriceMicros,
    SizeMicros, VenueId,
};
use alkera_sink_clickhouse::{ClickHouseDoubleBufferSink, ClickHouseTable};
use alkera_sink_parquet::{read_l2_deltas_parquet, write_l2_deltas_parquet};
use alkera_wal::MmapRingWal;
use tempfile::NamedTempFile;

#[test]
fn test_full_pipeline_chaos_failover_wal_compaction_and_reproducibility() {
    // 1. Register Kalshi & Polymarket instruments in the lock-free catalog interner
    let interner = InstrumentInterner::new();
    let (k_mkt_uid, k_yes_iid, _k_no_iid) = interner.register_kalshi_market(
        "KXFED-26NOV-T425",
        PriceMicros::ONE_CENT,
        Some("shard-0".to_string()),
        ActivityTier::HotWs,
    );

    // 2. Open two independent local NVMe WAL rings (Leg A in AZ1, Leg B in AZ2)
    let wal_a_file = NamedTempFile::new().unwrap();
    let wal_b_file = NamedTempFile::new().unwrap();
    let mut wal_a = MmapRingWal::open(wal_a_file.path(), 262_144).unwrap();
    let mut wal_b = MmapRingWal::open(wal_b_file.path(), 262_144).unwrap();

    // Use a 50-event periodic checkpoint interval (50_000_000 ns) so periodic checkpoints fire mid-stream
    let checkpoint_interval_ns = 50_000_000;
    let mut merger = ActiveActiveMerger::new(10_000_000_000, 100_000, checkpoint_interval_ns);
    let mut ch_sink = ClickHouseDoubleBufferSink::new(0, 500);

    let mut checkpoints: Vec<L2CheckpointEvent> = Vec::new();
    let mut deltas: Vec<L2DeltaEvent> = Vec::new();
    let mut gaps = Vec::new();

    // 3. Emit initial subscription snapshot at t = 1_000_000_000 ns
    let init_cp = L2CheckpointEvent {
        venue_id: VenueId::Kalshi,
        instrument_iid: k_yes_iid,
        market_uid: k_mkt_uid,
        exchange_ts_ms: 1_792_273_000_000,
        recv_ts_ns: 1_000_000_000,
        collector_session: 100,
        seq: 1,
        checkpoint_reason: CheckpointReason::SubscribeInit,
        book_hash: "init_kalshi".to_string(),
        bids_price_micros: vec![540_000, 530_000],
        bids_size_micros: vec![100_000_000, 200_000_000],
        asks_price_micros: vec![560_000, 570_000],
        asks_size_micros: vec![150_000_000, 250_000_000],
        dedup_key: 1,
    };
    wal_a
        .append(&MarketDataEnvelope::L2Checkpoint(init_cp.clone()))
        .unwrap();
    wal_b
        .append(&MarketDataEnvelope::L2Checkpoint(init_cp))
        .unwrap();

    // 4. Generate 200 L2 updates:
    //    - Leg A crashes between seq 60..90
    //    - Leg B drops between seq 130..150
    //    Because at least one leg is alive at every sequence number, 0 ticks are lost!
    for seq in 2..=200_u64 {
        let is_yes_bid = seq % 2 == 0;
        let side = if is_yes_bid {
            BookSide::KalshiYes
        } else {
            BookSide::KalshiNo
        };
        // YES bid @ $0.54 or NO bid @ $0.44 (which maps to YES ask @ $0.56)
        let price = if is_yes_bid { 540_000 } else { 440_000 };
        let delta_size = 5_000_000_i64; // +5.00 contracts each step
        let resulting_est = (100_000_000 + seq * 5_000_000) as u64;

        let dedup = L2DeltaEvent::compute_dedup_key(
            VenueId::Kalshi,
            k_yes_iid,
            k_mkt_uid,
            1_792_273_000_000 + seq,
            side,
            true,
            PriceMicros(price),
            SizeMicros(delta_size),
            resulting_est,
            "",
        );

        if !(60..=90).contains(&seq) {
            let env_a = MarketDataEnvelope::L2Delta(L2DeltaEvent {
                venue_id: VenueId::Kalshi,
                instrument_iid: k_yes_iid,
                market_uid: k_mkt_uid,
                exchange_ts_ms: 1_792_273_000_000 + seq,
                sending_ts_ms: 1_792_273_000_000 + seq,
                recv_ts_ns: 1_000_000_000 + seq * 1_000_000,
                ingest_ts_ns: 1_000_000_100 + seq * 1_000_000,
                collector_session: 100,
                collector_leg: 0,
                sid: 1,
                seq,
                side,
                is_delta: true,
                price_micros: PriceMicros(price),
                size_micros: SizeMicros(delta_size),
                resulting_size_micros: resulting_est,
                best_bid_micros: PriceMicros::ZERO,
                best_ask_micros: PriceMicros::ZERO,
                is_bbo_verified: true,
                event_hash: String::new(),
                dedup_key: dedup,
            });
            wal_a.append(&env_a).unwrap();
        }

        if !(130..=150).contains(&seq) {
            let env_b = MarketDataEnvelope::L2Delta(L2DeltaEvent {
                venue_id: VenueId::Kalshi,
                instrument_iid: k_yes_iid,
                market_uid: k_mkt_uid,
                exchange_ts_ms: 1_792_273_000_000 + seq,
                sending_ts_ms: 1_792_273_000_000 + seq,
                recv_ts_ns: 1_000_200_000 + seq * 1_000_000,
                ingest_ts_ns: 1_000_200_100 + seq * 1_000_000,
                collector_session: 200,
                collector_leg: 1,
                sid: 1,
                seq,
                side,
                is_delta: true,
                price_micros: PriceMicros(price),
                size_micros: SizeMicros(delta_size),
                resulting_size_micros: resulting_est,
                best_bid_micros: PriceMicros::ZERO,
                best_ask_micros: PriceMicros::ZERO,
                is_bbo_verified: true,
                event_hash: String::new(),
                dedup_key: dedup,
            });
            wal_b.append(&env_b).unwrap();
        }
    }

    // 5. Replay both WALs in arrival timestamp order into the Active-Active Merger
    let mut combined_wal_frames = wal_a.replay_uncommitted().unwrap();
    combined_wal_frames.extend(wal_b.replay_uncommitted().unwrap());
    combined_wal_frames.sort_by_key(|f| f.recv_ts_ns);

    let mut ch_offset = 1_u64;
    for frame in combined_wal_frames {
        match frame.envelope {
            MarketDataEnvelope::L2Checkpoint(cp) => {
                if let MergerOutcome::EmittedPrimary(env) = merger.ingest_l2_checkpoint(cp) {
                    if let MarketDataEnvelope::L2Checkpoint(ref c) = env {
                        checkpoints.push(c.clone());
                    }
                    ch_sink.push(ch_offset, env);
                    ch_offset += 1;
                }
            }
            MarketDataEnvelope::L2Delta(d) => {
                for outcome in merger.ingest_l2_delta(d) {
                    match outcome {
                        MergerOutcome::EmittedPrimary(env) => {
                            if let MarketDataEnvelope::L2Delta(ref dl) = env {
                                deltas.push(dl.clone());
                            }
                            ch_sink.push(ch_offset, env);
                            ch_offset += 1;
                        }
                        MergerOutcome::EmittedWithPeriodicCheckpoint {
                            primary,
                            checkpoint,
                        } => {
                            if let MarketDataEnvelope::L2Delta(ref dl) = primary {
                                deltas.push(dl.clone());
                            }
                            checkpoints.push(checkpoint.clone());
                            ch_sink.push(ch_offset, primary);
                            ch_offset += 1;
                            ch_sink.push(ch_offset, MarketDataEnvelope::L2Checkpoint(checkpoint));
                            ch_offset += 1;
                        }
                        MergerOutcome::GapRecorded(gap) => {
                            gaps.push(gap.clone());
                            ch_sink.push(ch_offset, MarketDataEnvelope::CaptureGap(gap));
                            ch_offset += 1;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    // Verify 100% of the 199 deltas (seq 2..=200) were captured and both single-leg outages were healed by the twin leg
    assert_eq!(deltas.len(), 199);
    assert_eq!(merger.metrics.gaps_healed_by_twin_total, 2);
    assert_eq!(merger.metrics.gaps_requiring_resync_total, 0);
    assert!(checkpoints.len() >= 4, "expected initial + periodic checkpoints");

    // 6. Verify ClickHouse double-buffered block generation
    let ch_blocks = ch_sink.swap_and_build_blocks();
    assert!(ch_blocks
        .iter()
        .any(|b| b.table == ClickHouseTable::L2Deltas && b.row_count == 199));

    // 7. Write deltas to Parquet and verify checkpoint-to-checkpoint transitions + final book state
    let parquet_tmp = NamedTempFile::new().unwrap();
    write_l2_deltas_parquet(parquet_tmp.path(), deltas).unwrap();
    let cold_deltas = read_l2_deltas_parquet(parquet_tmp.path()).unwrap();

    for pair in checkpoints.windows(2) {
        let verified = PointInTimeReconstructor::verify_checkpoint_transition(
            &pair[0],
            &pair[1],
            &cold_deltas,
            &gaps,
        )
        .unwrap();
        assert!(verified, "checkpoint transition failed verification");
    }

    let final_reconstructed = PointInTimeReconstructor::reconstruct_at_recv_ns(
        k_yes_iid,
        2_000_000_000,
        &checkpoints,
        &cold_deltas,
        &gaps,
    )
    .unwrap();

    assert!(matches!(
        final_reconstructed.continuity,
        ContinuityProof::Clean | ContinuityProof::HealedByTwinLeg { .. }
    ));
    let live_book = merger.canonical_book(k_yes_iid).unwrap();
    assert_eq!(final_reconstructed.state_hash, live_book.compute_state_hash());
}

#[test]
fn test_kalshi_wire_json_end_to_end_pipeline() {
    use alkera_collector::{CollectorRuntimeConfig, KalshiWireNormalizer};

    let interner = InstrumentInterner::new();
    let cfg_leg_a = CollectorRuntimeConfig {
        collector_leg: 0,
        collector_session: 1001,
        ..CollectorRuntimeConfig::default()
    };
    let cfg_leg_b = CollectorRuntimeConfig {
        collector_leg: 1,
        collector_session: 1002,
        ..CollectorRuntimeConfig::default()
    };

    let mut norm_a = KalshiWireNormalizer::new(cfg_leg_a);
    let mut norm_b = KalshiWireNormalizer::new(cfg_leg_b);
    let mut merger = ActiveActiveMerger::new(10_000_000_000, 100_000, 900_000_000_000);

    let snapshot_frame = r#"{
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "msg": {
            "market_ticker": "KXHIGHNY-26OCT17-T65",
            "yes_dollars_fp": [["0.6000", "100.00"], ["0.5900", "50.00"]],
            "no_dollars_fp": [["0.3800", "80.00"]]
        }
    }"#;

    let delta_frame = r#"{
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 2,
        "msg": {
            "market_ticker": "KXHIGHNY-26OCT17-T65",
            "price_dollars": "0.6000",
            "delta_fp": "25.00",
            "side": "yes",
            "ts_ms": 1792274000100
        }
    }"#;

    let mut checkpoints = Vec::new();
    let mut deltas = Vec::new();

    // Parse snapshot on Leg A and feed into merger
    for env in norm_a
        .parse_ws_frame(snapshot_frame, 1_792_274_000_000_000_000, &interner)
        .unwrap()
    {
        if let MarketDataEnvelope::L2Checkpoint(cp) = env {
            if let MergerOutcome::EmittedPrimary(MarketDataEnvelope::L2Checkpoint(c)) =
                merger.ingest_l2_checkpoint(cp)
            {
                checkpoints.push(c);
            }
        }
    }

    // Parse delta on both Leg A and Leg B (Leg B arrives 1.5ms later)
    for env in norm_a
        .parse_ws_frame(delta_frame, 1_792_274_000_100_000_000, &interner)
        .unwrap()
    {
        if let MarketDataEnvelope::L2Delta(d) = env {
            for out in merger.ingest_l2_delta(d) {
                if let MergerOutcome::EmittedPrimary(MarketDataEnvelope::L2Delta(emitted)) = out {
                    deltas.push(emitted);
                }
            }
        }
    }
    for env in norm_b
        .parse_ws_frame(delta_frame, 1_792_274_000_101_500_000, &interner)
        .unwrap()
    {
        if let MarketDataEnvelope::L2Delta(d) = env {
            let out = merger.ingest_l2_delta(d);
            assert!(matches!(
                out.as_slice(),
                [MergerOutcome::SuppressedTwinDuplicate {
                    skew_ns: 1_500_000,
                    ..
                }]
            ));
        }
    }

    assert_eq!(checkpoints.len(), 1);
    assert_eq!(deltas.len(), 1);

    let yes_iid = interner
        .get_iid(VenueId::Kalshi, "KXHIGHNY-26OCT17-T65:YES")
        .unwrap();
    let reconstructed = PointInTimeReconstructor::reconstruct_at_recv_ns(
        yes_iid,
        1_792_274_000_200_000_000,
        &checkpoints,
        &deltas,
        &[],
    )
    .unwrap();

    // Best YES bid @ $0.60 should now be 100.00 + 25.00 = 125.00 contracts (125_000_000 micros)
    // Best YES ask (from NO bid @ $0.38) should be $0.62 with 80.00 contracts (80_000_000 micros)
    assert_eq!(
        reconstructed.book.best_bid(),
        Some((PriceMicros(600_000), 125_000_000))
    );
    assert_eq!(
        reconstructed.book.best_ask(),
        Some((PriceMicros(620_000), 80_000_000))
    );
}

#[test]
fn test_live_kalshi_rest_and_ws_v2_capture_into_storage_pipeline() {
    use alkera_collector::{
        CollectorRuntimeConfig, KalshiCredentials, KalshiLiveClient, KalshiWireNormalizer,
    };

    let env_path = std::path::Path::new(
        "/cursor/stores/bc-2ce999e0-7f56-421c-9444-f12c4cf2154d/internal/kalshi-credentials.env",
    );
    if !env_path.exists() {
        return;
    }

    let creds = KalshiCredentials::from_dotenv_file(env_path).unwrap();
    let live_client = KalshiLiveClient::new(creds);

    // Capture 3 live open markets and 4 WebSocket frames (1 subscribed ack + 3 orderbook_snapshot frames)
    let report = live_client.capture_live_ws_sample(3, 4).unwrap();
    println!(
        "Live Kalshi capture succeeded on {} ({} / {}): discovered_tickers={:?}, frames_received={}",
        report.environment,
        report.rest_host,
        report.ws_host,
        report.discovered_tickers,
        report.raw_ws_frames.len()
    );
    assert!(!report.discovered_tickers.is_empty());
    assert!(report.raw_ws_frames.len() >= 2);

    // Pipe the live WebSocket frames through Leg A and Leg B normalizers -> WAL -> Merger -> ClickHouse Sink -> Parquet -> Replay
    let interner = InstrumentInterner::new();
    let mut norm_a = KalshiWireNormalizer::new(CollectorRuntimeConfig {
        collector_leg: 0,
        collector_session: 5001,
        ..CollectorRuntimeConfig::default()
    });
    let mut norm_b = KalshiWireNormalizer::new(CollectorRuntimeConfig {
        collector_leg: 1,
        collector_session: 5002,
        ..CollectorRuntimeConfig::default()
    });

    let wal_tmp = NamedTempFile::new().unwrap();
    let mut wal = MmapRingWal::open(wal_tmp.path(), 262_144).unwrap();
    let mut merger = ActiveActiveMerger::new(10_000_000_000, 100_000, 900_000_000_000);
    let mut ch_sink = ClickHouseDoubleBufferSink::new(0, 100);

    let mut checkpoints = Vec::new();
    let mut deltas = Vec::new();

    for (idx, frame_json) in report.raw_ws_frames.iter().enumerate() {
        let base_ts_ns = 1_792_274_500_000_000_000 + (idx as u64) * 1_000_000;
        let envs_a = norm_a.parse_ws_frame(frame_json, base_ts_ns, &interner).unwrap();
        let envs_b = norm_b
            .parse_ws_frame(frame_json, base_ts_ns + 500_000, &interner)
            .unwrap();

        for env in envs_a {
            wal.append(&env).unwrap();
        }
        for env in envs_b {
            wal.append(&env).unwrap();
        }
    }

    let replayed_wal = wal.replay_uncommitted().unwrap();
    assert!(!replayed_wal.is_empty());

    for (offset, wal_frame) in replayed_wal.into_iter().enumerate() {
        match wal_frame.envelope {
            MarketDataEnvelope::L2Checkpoint(cp) => {
                if let MergerOutcome::EmittedPrimary(env) = merger.ingest_l2_checkpoint(cp) {
                    if let MarketDataEnvelope::L2Checkpoint(ref c) = env {
                        checkpoints.push(c.clone());
                    }
                    ch_sink.push(offset as u64 + 1, env);
                }
            }
            MarketDataEnvelope::L2Delta(d) => {
                for out in merger.ingest_l2_delta(d) {
                    if let MergerOutcome::EmittedPrimary(env) = out {
                        if let MarketDataEnvelope::L2Delta(ref dl) = env {
                            deltas.push(dl.clone());
                        }
                        ch_sink.push(offset as u64 + 1, env);
                    }
                }
            }
            _ => {}
        }
    }

    assert!(
        !checkpoints.is_empty(),
        "expected at least one live Kalshi orderbook_snapshot checkpoint"
    );
    assert!(
        merger.metrics.twin_duplicates_suppressed_total >= 1,
        "expected Leg B duplicate snapshots to be suppressed"
    );

    let ch_blocks = ch_sink.swap_and_build_blocks();
    assert!(!ch_blocks.is_empty());

    // Reconstruct the first live market's orderbook from its captured checkpoint
    let first_cp = &checkpoints[0];
    let reconstructed = PointInTimeReconstructor::reconstruct_at_recv_ns(
        first_cp.instrument_iid,
        first_cp.recv_ts_ns + 10_000_000,
        &checkpoints,
        &deltas,
        &[],
    )
    .unwrap();
    assert_eq!(reconstructed.continuity, ContinuityProof::Clean);
}

use alkera_catalog::{ActivityTier, InstrumentInterner};
use alkera_schema::{
    BookSide, CheckpointReason, L2CheckpointEvent, L2DeltaEvent, LifecycleEvent,
    MarketDataEnvelope, PriceMicros, SchemaError, SizeMicros, TakerSide, TickerEvent, TradeEvent,
    VenueId,
};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;

pub const DEFAULT_KALSHI_REST_URL: &str = "https://external-api.kalshi.com/trade-api/v2";
pub const DEFAULT_KALSHI_WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const DEFAULT_KALSHI_WS_SIGN_PATH: &str = "/trade-api/ws/v2";
pub const DEFAULT_LOCALSTACK_ENDPOINT: &str = "http://localhost:4566";
pub const DEFAULT_AWS_REGION: &str = "us-east-2";

#[derive(Debug, Error)]
pub enum CollectorError {
    #[error("missing required Kalshi API key environment variable `{0}`")]
    MissingEnvVar(String),
    #[error("failed to read Kalshi private key from path `{path}`: {source}")]
    PrivateKeyRead {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid Kalshi Ed25519 private key PEM or hex seed: {0}")]
    InvalidPrivateKey(String),
    #[error("venue `{0}` is disabled in collector runtime configuration (Kalshi-only mode active)")]
    VenueDisabled(String),
    #[error("JSON parse error on Kalshi wire frame: {0}")]
    Json(#[from] serde_json::Error),
    #[error("schema conversion error: {0}")]
    Schema(#[from] SchemaError),
    #[error("Kalshi WebSocket error frame (code {code}): {msg}")]
    ExchangeErrorFrame { code: i64, msg: String },
    #[error("live transport error: {0}")]
    LiveTransport(String),
}

/// Runtime configuration for the collector and LocalStack/AWS storage layer.
/// Defaults to **Kalshi-only** (`kalshi_enabled: true`, `polymarket_enabled: false`) and LocalStack in `us-east-2`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorRuntimeConfig {
    pub kalshi_enabled: bool,
    pub polymarket_enabled: bool,
    pub kalshi_rest_url: String,
    pub kalshi_ws_url: String,
    pub collector_leg: u8,
    pub collector_session: u64,
    pub aws_region: String,
    pub aws_s3_endpoint: String,
    pub aws_s3_bucket: String,
    pub use_localstack: bool,
}

impl Default for CollectorRuntimeConfig {
    fn default() -> Self {
        Self {
            kalshi_enabled: true,
            polymarket_enabled: false,
            kalshi_rest_url: DEFAULT_KALSHI_REST_URL.to_string(),
            kalshi_ws_url: DEFAULT_KALSHI_WS_URL.to_string(),
            collector_leg: 0,
            collector_session: 1,
            aws_region: DEFAULT_AWS_REGION.to_string(),
            aws_s3_endpoint: DEFAULT_LOCALSTACK_ENDPOINT.to_string(),
            aws_s3_bucket: "alkera-parquet-lakehouse".to_string(),
            use_localstack: true,
        }
    }
}

impl CollectorRuntimeConfig {
    pub fn ensure_venue_enabled(&self, venue: VenueId) -> Result<(), CollectorError> {
        match venue {
            VenueId::Kalshi if self.kalshi_enabled => Ok(()),
            VenueId::Polymarket if self.polymarket_enabled => Ok(()),
            other => Err(CollectorError::VenueDisabled(other.as_str().to_string())),
        }
    }
}

/// Authenticated Kalshi API v2 header triplet required on both REST requests and the WebSocket `/trade-api/ws/v2` handshake.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KalshiSignedHeaders {
    pub access_key: String,
    pub access_timestamp_ms: String,
    pub access_signature: String,
}

impl KalshiSignedHeaders {
    pub fn as_header_pairs(&self) -> [(&'static str, &str); 3] {
        [
            ("KALSHI-ACCESS-KEY", self.access_key.as_str()),
            ("KALSHI-ACCESS-TIMESTAMP", self.access_timestamp_ms.as_str()),
            ("KALSHI-ACCESS-SIGNATURE", self.access_signature.as_str()),
        ]
    }
}

/// Secret JSON structure stored in LocalStack / AWS Secrets Manager (`alkera/kalshi/collector-leg-a`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KalshiSecretPayload {
    #[serde(rename = "KALSHI_API_KEY_ID")]
    pub api_key_id: String,
    #[serde(rename = "KALSHI_PRIVATE_KEY_PEM")]
    pub private_key_pem: String,
}

/// Kalshi API v2 Ed25519 credential signer.
/// Loads credentials from environment variables (`KALSHI_API_KEY_ID` + `KALSHI_PRIVATE_KEY_PEM` or `KALSHI_PRIVATE_KEY_PATH`),
/// leg-specific env vars (`KALSHI_CAPTURE_A_API_KEY_ID`, `KALSHI_CAPTURE_B_API_KEY_ID`), or AWS/LocalStack Secrets Manager JSON.
#[derive(Clone)]
pub struct KalshiCredentials {
    api_key_id: String,
    signing_key: SigningKey,
}

impl KalshiCredentials {
    /// Creates `KalshiCredentials` from an API key UUID and either a PKCS#8 Ed25519 PEM string or a 32-byte hex seed.
    pub fn from_pem_or_seed(api_key_id: impl Into<String>, key_material: &str) -> Result<Self, CollectorError> {
        let api_key_id = api_key_id.into();
        let trimmed = key_material.trim();
        if trimmed.is_empty() {
            return Err(CollectorError::InvalidPrivateKey(
                "empty private key material".to_string(),
            ));
        }

        let signing_key = if trimmed.starts_with("-----BEGIN") {
            let normalized_pem = trimmed.replace("\\n", "\n");
            SigningKey::from_pkcs8_pem(&normalized_pem)
                .map_err(|e| CollectorError::InvalidPrivateKey(e.to_string()))?
        } else {
            let hex_clean = trimmed.strip_prefix("0x").unwrap_or(trimmed);
            let bytes = hex::decode(hex_clean)
                .map_err(|e| CollectorError::InvalidPrivateKey(e.to_string()))?;
            let seed: [u8; 32] = bytes.try_into().map_err(|_| {
                CollectorError::InvalidPrivateKey("expected 32-byte Ed25519 seed".to_string())
            })?;
            SigningKey::from_bytes(&seed)
        };

        Ok(Self {
            api_key_id,
            signing_key,
        })
    }

    /// Loads Kalshi credentials from standard or leg-specific environment variables:
    /// - Key ID: `KALSHI_CAPTURE_{A|B}_API_KEY_ID` -> `KALSHI_API_KEY_ID`
    /// - Private Key: `KALSHI_CAPTURE_{A|B}_PRIVATE_KEY_PEM` -> `KALSHI_PRIVATE_KEY_PEM` -> `KALSHI_CAPTURE_{A|B}_PRIVATE_KEY_PATH` -> `KALSHI_PRIVATE_KEY_PATH`
    pub fn from_env_for_leg(leg: u8) -> Result<Self, CollectorError> {
        let leg_tag = if leg == 0 { "A" } else { "B" };
        let leg_key_var = format!("KALSHI_CAPTURE_{leg_tag}_API_KEY_ID");
        let api_key_id = std::env::var(&leg_key_var)
            .or_else(|_| std::env::var("KALSHI_API_KEY_ID"))
            .map_err(|_| CollectorError::MissingEnvVar("KALSHI_API_KEY_ID".to_string()))?;

        let leg_pem_var = format!("KALSHI_CAPTURE_{leg_tag}_PRIVATE_KEY_PEM");
        if let Ok(inline_pem) =
            std::env::var(&leg_pem_var).or_else(|_| std::env::var("KALSHI_PRIVATE_KEY_PEM"))
        {
            if !inline_pem.trim().is_empty() {
                return Self::from_pem_or_seed(api_key_id, &inline_pem);
            }
        }

        let leg_path_var = format!("KALSHI_CAPTURE_{leg_tag}_PRIVATE_KEY_PATH");
        let key_path = std::env::var(&leg_path_var)
            .or_else(|_| std::env::var("KALSHI_PRIVATE_KEY_PATH"))
            .map_err(|_| {
                CollectorError::MissingEnvVar(
                    "KALSHI_PRIVATE_KEY_PEM or KALSHI_PRIVATE_KEY_PATH".to_string(),
                )
            })?;

        Self::from_key_file(api_key_id, Path::new(&key_path))
    }

    /// Loads Kalshi credentials from a PEM file on disk.
    pub fn from_key_file(api_key_id: impl Into<String>, path: &Path) -> Result<Self, CollectorError> {
        let content = std::fs::read_to_string(path).map_err(|source| {
            CollectorError::PrivateKeyRead {
                path: path.display().to_string(),
                source,
            }
        })?;
        Self::from_pem_or_seed(api_key_id, &content)
    }

    /// Loads Kalshi credentials from a `.env`-formatted file (`KEY="VALUE"` lines).
    pub fn from_dotenv_file(path: &Path) -> Result<Self, CollectorError> {
        let content = std::fs::read_to_string(path).map_err(|source| {
            CollectorError::PrivateKeyRead {
                path: path.display().to_string(),
                source,
            }
        })?;
        let mut vars: HashMap<String, String> = HashMap::new();
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = trimmed.split_once('=') {
                let key = k.trim().to_string();
                let val = v
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_string();
                vars.insert(key, val);
            }
        }

        let api_key_id = vars
            .get("KALSHI_API_KEY_ID")
            .cloned()
            .ok_or_else(|| CollectorError::MissingEnvVar("KALSHI_API_KEY_ID".to_string()))?;

        if let Some(pem) = vars.get("KALSHI_PRIVATE_KEY_PEM") {
            if !pem.trim().is_empty() {
                return Self::from_pem_or_seed(api_key_id, pem);
            }
        }
        if let Some(key_path) = vars.get("KALSHI_PRIVATE_KEY_PATH") {
            if !key_path.trim().is_empty() {
                return Self::from_key_file(api_key_id, Path::new(key_path));
            }
        }

        Err(CollectorError::MissingEnvVar(
            "KALSHI_PRIVATE_KEY_PEM or KALSHI_PRIVATE_KEY_PATH".to_string(),
        ))
    }

    /// Parses a LocalStack / AWS Secrets Manager secret JSON string.
    pub fn from_secrets_manager_json(secret_json: &str) -> Result<Self, CollectorError> {
        let payload: KalshiSecretPayload = serde_json::from_str(secret_json)?;
        Self::from_pem_or_seed(payload.api_key_id, &payload.private_key_pem)
    }

    #[inline]
    pub fn api_key_id(&self) -> &str {
        &self.api_key_id
    }

    #[inline]
    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// Constructs the exact canonical string signed by Kalshi v2:
    /// `"{timestamp_ms}{UPPER(method)}{path_without_query}"`
    /// Per Kalshi documentation, query parameters (`?limit=100&status=open`) MUST be stripped before signing.
    pub fn canonical_sign_payload(timestamp_ms: u64, method: &str, path_or_uri: &str) -> String {
        let path_only = path_or_uri
            .split_once('?')
            .map(|(p, _)| p)
            .unwrap_or(path_or_uri);
        format!("{timestamp_ms}{}{}", method.trim().to_ascii_uppercase(), path_only)
    }

    /// Signs a Kalshi REST or WebSocket request for the given `timestamp_ms`, HTTP `method`, and `path`.
    pub fn sign_request(
        &self,
        timestamp_ms: u64,
        method: &str,
        path_or_uri: &str,
    ) -> KalshiSignedHeaders {
        let payload = Self::canonical_sign_payload(timestamp_ms, method, path_or_uri);
        let signature = self.signing_key.sign(payload.as_bytes());
        KalshiSignedHeaders {
            access_key: self.api_key_id.clone(),
            access_timestamp_ms: timestamp_ms.to_string(),
            access_signature: BASE64_STANDARD.encode(signature.to_bytes()),
        }
    }

    /// Signs the Kalshi WebSocket v2 handshake (`GET /trade-api/ws/v2`).
    pub fn sign_ws_handshake(&self, timestamp_ms: u64) -> KalshiSignedHeaders {
        self.sign_request(timestamp_ms, "GET", DEFAULT_KALSHI_WS_SIGN_PATH)
    }
}

/// Kalshi WebSocket v2 command frames (`subscribe`, `unsubscribe`, `update_subscription`).
pub struct KalshiWsCommands;

impl KalshiWsCommands {
    /// Builds the unfiltered global subscription frame for socket 0 (`trade`, `ticker`, `market_lifecycle_v2`, `event_lifecycle`, `multivariate_market_lifecycle`).
    pub fn subscribe_global_channels(cmd_id: u64) -> String {
        serde_json::json!({
            "id": cmd_id,
            "cmd": "subscribe",
            "params": {
                "channels": [
                    "trade",
                    "ticker",
                    "market_lifecycle_v2",
                    "event_lifecycle",
                    "multivariate_market_lifecycle"
                ]
            }
        })
        .to_string()
    }

    /// Builds a sharded `orderbook_delta` subscription command for up to 500 `market_tickers`.
    pub fn subscribe_orderbook_shard(cmd_id: u64, market_tickers: &[String]) -> String {
        serde_json::json!({
            "id": cmd_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": market_tickers
            }
        })
        .to_string()
    }

    /// Builds an in-place `update_subscription` (`action = "get_snapshot"`) command to heal a sequence gap
    /// on subscription `sid` without dropping the live delta stream.
    pub fn request_in_place_snapshot(cmd_id: u64, sid: u32, market_tickers: &[String]) -> String {
        serde_json::json!({
            "id": cmd_id,
            "cmd": "update_subscription",
            "params": {
                "sid": sid,
                "action": "get_snapshot",
                "market_tickers": market_tickers
            }
        })
        .to_string()
    }
}

#[derive(Deserialize)]
struct RawKalshiWsEnvelope<'a> {
    #[serde(rename = "type")]
    msg_type: &'a str,
    #[serde(default)]
    sid: u32,
    #[serde(default)]
    seq: u64,
    msg: serde_json::Value,
}

#[derive(Deserialize)]
struct RawKalshiSnapshotMsg {
    market_ticker: String,
    #[serde(default)]
    yes_dollars_fp: Vec<(String, String)>,
    #[serde(default)]
    no_dollars_fp: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct RawKalshiDeltaMsg {
    market_ticker: String,
    price_dollars: String,
    delta_fp: String,
    side: String,
    #[serde(default)]
    client_order_id: Option<String>,
    #[serde(default)]
    ts_ms: Option<u64>,
}

#[derive(Deserialize)]
struct RawKalshiTradeMsg {
    trade_id: String,
    market_ticker: String,
    yes_price_dollars: String,
    no_price_dollars: String,
    count_fp: String,
    taker_side: String,
    #[serde(default)]
    is_block_trade: bool,
    ts_ms: u64,
}

#[derive(Deserialize)]
struct RawKalshiTickerMsg {
    market_ticker: String,
    #[serde(default)]
    price_dollars: Option<String>,
    #[serde(default)]
    yes_bid_dollars: Option<String>,
    #[serde(default)]
    yes_ask_dollars: Option<String>,
    #[serde(default)]
    yes_bid_size_fp: Option<String>,
    #[serde(default)]
    yes_ask_size_fp: Option<String>,
    #[serde(default)]
    volume_fp: Option<String>,
    #[serde(default)]
    open_interest_fp: Option<String>,
    #[serde(default)]
    dollar_volume: u64,
    #[serde(default)]
    dollar_open_interest: u64,
    ts_ms: u64,
}

/// Stateful Kalshi WebSocket v2 wire frame parser & normalizer.
/// Tracks per-`sid` sequence numbers (`seq`), resolves `market_ticker` to surrogate `(market_uid, instrument_iid)`
/// via `InstrumentInterner` (auto-registering unseen tickers on the fly if needed), and emits canonical `MarketDataEnvelope`s.
pub struct KalshiWireNormalizer {
    config: CollectorRuntimeConfig,
    sid_last_seq: HashMap<u32, u64>,
}

impl KalshiWireNormalizer {
    pub fn new(config: CollectorRuntimeConfig) -> Self {
        Self {
            config,
            sid_last_seq: HashMap::new(),
        }
    }

    /// Parses a single Kalshi WebSocket JSON frame into `0..N` canonical `MarketDataEnvelope`s.
    pub fn parse_ws_frame(
        &mut self,
        raw_json: &str,
        recv_ts_ns: u64,
        interner: &InstrumentInterner,
    ) -> Result<Vec<MarketDataEnvelope>, CollectorError> {
        self.config.ensure_venue_enabled(VenueId::Kalshi)?;
        let env: RawKalshiWsEnvelope<'_> = serde_json::from_str(raw_json)?;

        match env.msg_type {
            "subscribed" => Ok(Vec::new()),
            "error" => {
                let code = env.msg.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
                let msg = env
                    .msg
                    .get("msg")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error")
                    .to_string();
                Err(CollectorError::ExchangeErrorFrame { code, msg })
            }
            "orderbook_snapshot" => {
                let snap: RawKalshiSnapshotMsg = serde_json::from_value(env.msg)?;
                let (market_uid, yes_iid, _) = interner.register_kalshi_market(
                    &snap.market_ticker,
                    PriceMicros::ONE_CENT,
                    None,
                    ActivityTier::HotWs,
                );

                let mut bids_levels: Vec<(u32, u64)> =
                    Vec::with_capacity(snap.yes_dollars_fp.len());
                for (p_str, c_str) in &snap.yes_dollars_fp {
                    let price = PriceMicros::parse_ascii(p_str)?.0;
                    let size = SizeMicros::parse_ascii(c_str)?.0.max(0) as u64;
                    if size > 0 {
                        bids_levels.push((price, size));
                    }
                }
                // Sort bids descending (highest bid first)
                bids_levels.sort_unstable_by(|a, b| b.0.cmp(&a.0));

                let mut asks_levels: Vec<(u32, u64)> = Vec::with_capacity(snap.no_dollars_fp.len());
                for (no_p_str, c_str) in &snap.no_dollars_fp {
                    let no_price = PriceMicros::parse_ascii(no_p_str)?;
                    let yes_ask_price = no_price.complement().0;
                    let size = SizeMicros::parse_ascii(c_str)?.0.max(0) as u64;
                    if size > 0 {
                        asks_levels.push((yes_ask_price, size));
                    }
                }
                // Sort asks ascending (lowest ask first)
                asks_levels.sort_unstable_by(|a, b| a.0.cmp(&b.0));

                let bids_price_micros: Vec<u32> = bids_levels.iter().map(|&(p, _)| p).collect();
                let bids_size_micros: Vec<u64> = bids_levels.iter().map(|&(_, s)| s).collect();
                let asks_price_micros: Vec<u32> = asks_levels.iter().map(|&(p, _)| p).collect();
                let asks_size_micros: Vec<u64> = asks_levels.iter().map(|&(_, s)| s).collect();

                if env.sid > 0 && env.seq > 0 {
                    self.sid_last_seq.insert(env.sid, env.seq);
                }

                let exchange_ts_ms = recv_ts_ns / 1_000_000;
                let book_hash = format!("kalshi_sid{}_seq{}", env.sid, env.seq);
                let dedup_key = L2CheckpointEvent::compute_dedup_key(
                    VenueId::Kalshi,
                    yes_iid,
                    exchange_ts_ms,
                    CheckpointReason::SubscribeInit,
                    &book_hash,
                );

                Ok(vec![MarketDataEnvelope::L2Checkpoint(L2CheckpointEvent {
                    venue_id: VenueId::Kalshi,
                    instrument_iid: yes_iid,
                    market_uid,
                    exchange_ts_ms,
                    recv_ts_ns,
                    collector_session: self.config.collector_session,
                    seq: env.seq,
                    checkpoint_reason: CheckpointReason::SubscribeInit,
                    book_hash,
                    bids_price_micros,
                    bids_size_micros,
                    asks_price_micros,
                    asks_size_micros,
                    dedup_key,
                })])
            }
            "orderbook_delta" => {
                let d: RawKalshiDeltaMsg = serde_json::from_value(env.msg)?;
                let (market_uid, yes_iid, _) = interner.register_kalshi_market(
                    &d.market_ticker,
                    PriceMicros::ONE_CENT,
                    None,
                    ActivityTier::HotWs,
                );

                let side = if d.side.eq_ignore_ascii_case("yes") {
                    BookSide::KalshiYes
                } else {
                    BookSide::KalshiNo
                };
                let price_micros = PriceMicros::parse_ascii(&d.price_dollars)?;
                let size_micros = SizeMicros::parse_ascii(&d.delta_fp)?;
                let exchange_ts_ms = d.ts_ms.unwrap_or(recv_ts_ns / 1_000_000);
                let event_hash = d.client_order_id.unwrap_or_default();

                if env.sid > 0 && env.seq > 0 {
                    self.sid_last_seq.insert(env.sid, env.seq);
                }

                let dedup_key = L2DeltaEvent::compute_dedup_key(
                    VenueId::Kalshi,
                    yes_iid,
                    market_uid,
                    exchange_ts_ms,
                    side,
                    true,
                    price_micros,
                    size_micros,
                    0,
                    &event_hash,
                );

                Ok(vec![MarketDataEnvelope::L2Delta(L2DeltaEvent {
                    venue_id: VenueId::Kalshi,
                    instrument_iid: yes_iid,
                    market_uid,
                    exchange_ts_ms,
                    sending_ts_ms: exchange_ts_ms,
                    recv_ts_ns,
                    ingest_ts_ns: recv_ts_ns,
                    collector_session: self.config.collector_session,
                    collector_leg: self.config.collector_leg,
                    sid: env.sid,
                    seq: env.seq,
                    side,
                    is_delta: true,
                    price_micros,
                    size_micros,
                    resulting_size_micros: 0,
                    best_bid_micros: PriceMicros::ZERO,
                    best_ask_micros: PriceMicros::ZERO,
                    is_bbo_verified: true,
                    event_hash,
                    dedup_key,
                })])
            }
            "trade" => {
                let t: RawKalshiTradeMsg = serde_json::from_value(env.msg)?;
                let (market_uid, yes_iid, _) = interner.register_kalshi_market(
                    &t.market_ticker,
                    PriceMicros::ONE_CENT,
                    None,
                    ActivityTier::HotWs,
                );
                let price_micros = PriceMicros::parse_ascii(&t.yes_price_dollars)?;
                let complement_price_micros = PriceMicros::parse_ascii(&t.no_price_dollars)?;
                let size_micros = SizeMicros::parse_ascii(&t.count_fp)?.0.max(0) as u64;
                let taker_side = if t.taker_side.eq_ignore_ascii_case("yes") {
                    TakerSide::Yes
                } else {
                    TakerSide::No
                };
                let dedup_key = TradeEvent::compute_dedup_key(
                    VenueId::Kalshi,
                    yes_iid,
                    t.ts_ms,
                    &t.trade_id,
                    "",
                    price_micros,
                    size_micros,
                    taker_side,
                );

                let (leg_a_ns, leg_b_ns) = if self.config.collector_leg == 0 {
                    (recv_ts_ns, 0)
                } else {
                    (0, recv_ts_ns)
                };

                Ok(vec![MarketDataEnvelope::Trade(TradeEvent {
                    venue_id: VenueId::Kalshi,
                    instrument_iid: yes_iid,
                    market_uid,
                    exchange_ts_ms: t.ts_ms,
                    sending_ts_ms: t.ts_ms,
                    recv_ts_ns,
                    leg_a_recv_ns: leg_a_ns,
                    leg_b_recv_ns: leg_b_ns,
                    trade_id: t.trade_id,
                    tx_hash: String::new(),
                    price_micros,
                    complement_price_micros,
                    size_micros,
                    taker_side,
                    fee_rate_bps: 0,
                    is_block_trade: t.is_block_trade,
                    dedup_key,
                })])
            }
            "ticker" => {
                let tk: RawKalshiTickerMsg = serde_json::from_value(env.msg)?;
                let (market_uid, yes_iid, _) = interner.register_kalshi_market(
                    &tk.market_ticker,
                    PriceMicros::ONE_CENT,
                    None,
                    ActivityTier::HotWs,
                );
                let parse_opt_price = |opt: Option<String>| -> Result<PriceMicros, SchemaError> {
                    match opt {
                        Some(s) if !s.is_empty() => PriceMicros::parse_ascii(&s),
                        _ => Ok(PriceMicros::ZERO),
                    }
                };
                let parse_opt_size = |opt: Option<String>| -> Result<u64, SchemaError> {
                    match opt {
                        Some(s) if !s.is_empty() => Ok(SizeMicros::parse_ascii(&s)?.0.max(0) as u64),
                        _ => Ok(0),
                    }
                };

                let last_price_micros = parse_opt_price(tk.price_dollars)?;
                let best_bid_micros = parse_opt_price(tk.yes_bid_dollars)?;
                let best_ask_micros = parse_opt_price(tk.yes_ask_dollars)?;
                let best_bid_size_micros = parse_opt_size(tk.yes_bid_size_fp)?;
                let best_ask_size_micros = parse_opt_size(tk.yes_ask_size_fp)?;
                let volume_micros = parse_opt_size(tk.volume_fp)?;
                let open_interest_micros = parse_opt_size(tk.open_interest_fp)?;

                let dedup_key = xxhash_rust::xxh3::xxh3_64(
                    format!(
                        "{}:{}:{}:{}:{}:{}:{}",
                        yes_iid,
                        tk.ts_ms,
                        last_price_micros.0,
                        best_bid_micros.0,
                        best_ask_micros.0,
                        volume_micros,
                        open_interest_micros
                    )
                    .as_bytes(),
                );

                Ok(vec![MarketDataEnvelope::Ticker(TickerEvent {
                    venue_id: VenueId::Kalshi,
                    instrument_iid: yes_iid,
                    market_uid,
                    exchange_ts_ms: tk.ts_ms,
                    recv_ts_ns,
                    last_price_micros,
                    best_bid_micros,
                    best_ask_micros,
                    best_bid_size_micros,
                    best_ask_size_micros,
                    volume_micros,
                    open_interest_micros,
                    dollar_volume: tk.dollar_volume,
                    dollar_open_interest: tk.dollar_open_interest,
                    dedup_key,
                })])
            }
            "market_lifecycle_v2" | "multivariate_market_lifecycle" => {
                let ticker = env
                    .msg
                    .get("market_ticker")
                    .and_then(|v| v.as_str())
                    .unwrap_or("UNKNOWN");
                let event_type = env
                    .msg
                    .get("event_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let result = env
                    .msg
                    .get("result")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let (market_uid, yes_iid, _) = interner.register_kalshi_market(
                    ticker,
                    PriceMicros::ONE_CENT,
                    None,
                    ActivityTier::HotWs,
                );
                let payload_json = env.msg.to_string();
                let exchange_ts_ms = recv_ts_ns / 1_000_000;
                let dedup_key = xxhash_rust::xxh3::xxh3_64(
                    format!("{market_uid}:{event_type}:{payload_json}").as_bytes(),
                );

                Ok(vec![MarketDataEnvelope::Lifecycle(LifecycleEvent {
                    venue_id: VenueId::Kalshi,
                    market_uid,
                    instrument_iid: yes_iid,
                    exchange_ts_ms,
                    recv_ts_ns,
                    lifecycle_type: event_type,
                    old_tick_size_micros: PriceMicros::ZERO,
                    new_tick_size_micros: PriceMicros::ZERO,
                    result,
                    settlement_value_micros: 0,
                    payload_json,
                    dedup_key,
                })])
            }
            _ => Ok(Vec::new()),
        }
    }
}

/// Live read-only Kalshi REST v2 + WebSocket v2 capture client.
/// Automatically probes production (`external-api.kalshi.com` / `external-api-ws.kalshi.com`)
/// and demo (`external-api.demo.kalshi.co` / `external-api-ws.demo.kalshi.co`) endpoints using
/// Ed25519 PKCS#8 request signatures (`timestamp_ms + "GET" + path`).
pub struct KalshiLiveClient {
    creds: KalshiCredentials,
}

#[derive(Clone, Debug)]
pub struct LiveKalshiCaptureReport {
    pub environment: String,
    pub rest_host: String,
    pub ws_host: String,
    pub discovered_tickers: Vec<String>,
    pub raw_ws_frames: Vec<String>,
}

impl KalshiLiveClient {
    pub fn new(creds: KalshiCredentials) -> Self {
        Self { creds }
    }

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Queries `GET /trade-api/v2/markets?status=open&limit={limit}` with Ed25519 headers,
    /// checking production first and demo second, and returns `(environment, rest_host, ws_host, tickers)`.
    pub fn discover_open_markets(
        &self,
        limit: usize,
    ) -> Result<(String, String, String, Vec<String>), CollectorError> {
        let candidates = [
            (
                "production",
                "external-api.kalshi.com",
                "external-api-ws.kalshi.com",
            ),
            (
                "demo",
                "external-api.demo.kalshi.co",
                "external-api-ws.demo.kalshi.co",
            ),
        ];

        let sign_path = "/trade-api/v2/markets";
        let query_path = format!("{sign_path}?status=open&limit={limit}");

        let mut last_err = String::new();
        for (env_name, rest_host, ws_host) in candidates {
            let ts_ms = Self::now_ms();
            let headers = self.creds.sign_request(ts_ms, "GET", &query_path);
            let url = format!("https://{rest_host}{query_path}");

            let output = std::process::Command::new("curl")
                .args([
                    "-sS",
                    "--max-time",
                    "8",
                    "-H",
                    &format!("KALSHI-ACCESS-KEY: {}", headers.access_key),
                    "-H",
                    &format!("KALSHI-ACCESS-TIMESTAMP: {}", headers.access_timestamp_ms),
                    "-H",
                    &format!("KALSHI-ACCESS-SIGNATURE: {}", headers.access_signature),
                    &url,
                ])
                .output()
                .map_err(|e| CollectorError::LiveTransport(e.to_string()))?;

            if !output.status.success() {
                last_err = String::from_utf8_lossy(&output.stderr).to_string();
                continue;
            }

            let body: serde_json::Value = match serde_json::from_slice(&output.stdout) {
                Ok(v) => v,
                Err(e) => {
                    last_err = e.to_string();
                    continue;
                }
            };

            if let Some(markets) = body.get("markets").and_then(|m| m.as_array()) {
                let tickers: Vec<String> = markets
                    .iter()
                    .filter_map(|m| m.get("ticker").and_then(|t| t.as_str()).map(String::from))
                    .collect();
                if !tickers.is_empty() {
                    return Ok((
                        env_name.to_string(),
                        rest_host.to_string(),
                        ws_host.to_string(),
                        tickers,
                    ));
                }
            }
            last_err = String::from_utf8_lossy(&output.stdout).to_string();
        }

        Err(CollectorError::LiveTransport(format!(
            "failed to discover open markets on Kalshi prod or demo: {last_err}"
        )))
    }

    /// Performs a live authenticated WebSocket v2 handshake (`GET /trade-api/ws/v2`),
    /// subscribes to `orderbook_delta` for the discovered `market_tickers` plus `ticker` and `trade`,
    /// and captures the initial `subscribed`, `orderbook_snapshot`, and live delta/ticker frames.
    pub fn capture_live_ws_sample(
        &self,
        max_tickers: usize,
        max_frames: usize,
    ) -> Result<LiveKalshiCaptureReport, CollectorError> {
        use std::io::{Read, Write};
        use std::process::{Command, Stdio};

        let (environment, rest_host, ws_host, discovered_tickers) =
            self.discover_open_markets(max_tickers)?;

        let ts_ms = Self::now_ms();
        let ws_headers = self.creds.sign_ws_handshake(ts_ms);

        let mut child = Command::new("openssl")
            .args([
                "s_client",
                "-quiet",
                "-connect",
                &format!("{ws_host}:443"),
                "-servername",
                &ws_host,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| CollectorError::LiveTransport(e.to_string()))?;

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| CollectorError::LiveTransport("missing openssl stdin".to_string()))?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| CollectorError::LiveTransport("missing openssl stdout".to_string()))?;

        // Send RFC-6455 HTTP/1.1 Upgrade request with Kalshi Ed25519 auth headers
        let upgrade_req = format!(
            "GET /trade-api/ws/v2 HTTP/1.1\r\n\
             Host: {ws_host}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Version: 13\r\n\
             KALSHI-ACCESS-KEY: {}\r\n\
             KALSHI-ACCESS-TIMESTAMP: {}\r\n\
             KALSHI-ACCESS-SIGNATURE: {}\r\n\r\n",
            ws_headers.access_key, ws_headers.access_timestamp_ms, ws_headers.access_signature
        );
        stdin
            .write_all(upgrade_req.as_bytes())
            .map_err(|e| CollectorError::LiveTransport(e.to_string()))?;
        stdin
            .flush()
            .map_err(|e| CollectorError::LiveTransport(e.to_string()))?;

        // Read HTTP 101 Switching Protocols header
        let mut raw_buf = Vec::with_capacity(65_536);
        let mut chunk = [0_u8; 4096];
        let header_end = loop {
            let n = stdout
                .read(&mut chunk)
                .map_err(|e| CollectorError::LiveTransport(e.to_string()))?;
            if n == 0 {
                let _ = child.kill();
                return Err(CollectorError::LiveTransport(
                    "EOF before WebSocket HTTP 101 upgrade response".to_string(),
                ));
            }
            raw_buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = raw_buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };

        let http_header_str = String::from_utf8_lossy(&raw_buf[..header_end]).to_string();
        if !http_header_str.contains("101") {
            let _ = child.kill();
            return Err(CollectorError::LiveTransport(format!(
                "WebSocket handshake rejected: {http_header_str}"
            )));
        }

        let mut ws_stream_buf = raw_buf.split_off(header_end);

        // Send masked RFC-6455 text frame subscribing to `orderbook_delta` on the discovered tickers
        let sub_cmd = KalshiWsCommands::subscribe_orderbook_shard(1, &discovered_tickers);
        let masked_frame = Self::encode_masked_ws_text_frame(&sub_cmd);
        stdin
            .write_all(&masked_frame)
            .map_err(|e| CollectorError::LiveTransport(e.to_string()))?;
        stdin
            .flush()
            .map_err(|e| CollectorError::LiveTransport(e.to_string()))?;

        let mut raw_ws_frames = Vec::new();
        while raw_ws_frames.len() < max_frames {
            while let Some((opcode, payload, consumed)) = Self::try_decode_ws_frame(&ws_stream_buf)
            {
                ws_stream_buf.drain(..consumed);
                if opcode == 0x1 {
                    // Text JSON frame
                    if let Ok(text) = String::from_utf8(payload) {
                        raw_ws_frames.push(text);
                        if raw_ws_frames.len() >= max_frames {
                            break;
                        }
                    }
                } else if opcode == 0x9 {
                    // Control Ping -> reply Pong
                    let pong = Self::encode_masked_ws_frame(0xA, &payload);
                    let _ = stdin.write_all(&pong);
                    let _ = stdin.flush();
                } else if opcode == 0x8 {
                    // Close frame
                    break;
                }
            }
            if raw_ws_frames.len() >= max_frames {
                break;
            }
            let n = stdout
                .read(&mut chunk)
                .map_err(|e| CollectorError::LiveTransport(e.to_string()))?;
            if n == 0 {
                break;
            }
            ws_stream_buf.extend_from_slice(&chunk[..n]);
        }

        let _ = child.kill();
        let _ = child.wait();

        Ok(LiveKalshiCaptureReport {
            environment,
            rest_host,
            ws_host,
            discovered_tickers,
            raw_ws_frames,
        })
    }

    fn encode_masked_ws_text_frame(text: &str) -> Vec<u8> {
        Self::encode_masked_ws_frame(0x1, text.as_bytes())
    }

    fn encode_masked_ws_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
        let mask_key: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];
        let mut frame = Vec::with_capacity(14 + payload.len());
        frame.push(0x80 | (opcode & 0x0f));
        if payload.len() < 126 {
            frame.push(0x80 | (payload.len() as u8));
        } else if payload.len() <= u16::MAX as usize {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        } else {
            frame.push(0x80 | 127);
            frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
        frame.extend_from_slice(&mask_key);
        for (i, &b) in payload.iter().enumerate() {
            frame.push(b ^ mask_key[i % 4]);
        }
        frame
    }

    fn try_decode_ws_frame(buf: &[u8]) -> Option<(u8, Vec<u8>, usize)> {
        if buf.len() < 2 {
            return None;
        }
        let opcode = buf[0] & 0x0f;
        let masked = (buf[1] & 0x80) != 0;
        let mut len = (buf[1] & 0x7f) as usize;
        let mut offset = 2;

        if len == 126 {
            if buf.len() < 4 {
                return None;
            }
            len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
            offset = 4;
        } else if len == 127 {
            if buf.len() < 10 {
                return None;
            }
            len = u64::from_be_bytes(buf[2..10].try_into().ok()?) as usize;
            offset = 10;
        }

        let mask_key = if masked {
            if buf.len() < offset + 4 {
                return None;
            }
            let k = [
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ];
            offset += 4;
            Some(k)
        } else {
            None
        };

        if buf.len() < offset + len {
            return None;
        }

        let mut payload = buf[offset..offset + len].to_vec();
        if let Some(k) = mask_key {
            for (i, b) in payload.iter_mut().enumerate() {
                *b ^= k[i % 4];
            }
        }
        Some((opcode, payload, offset + len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Verifier;

    const TEST_SEED_HEX: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";

    #[test]
    fn test_kalshi_only_default_runtime_config() {
        let cfg = CollectorRuntimeConfig::default();
        assert!(cfg.kalshi_enabled);
        assert!(!cfg.polymarket_enabled);
        assert!(cfg.use_localstack);
        assert_eq!(cfg.aws_region, "us-east-2");
        assert!(cfg.ensure_venue_enabled(VenueId::Kalshi).is_ok());
        assert!(matches!(
            cfg.ensure_venue_enabled(VenueId::Polymarket),
            Err(CollectorError::VenueDisabled(_))
        ));
    }

    #[test]
    fn test_kalshi_ed25519_signing_strips_query_params_and_verifies() {
        let creds =
            KalshiCredentials::from_pem_or_seed("kalshi-key-uuid-001", TEST_SEED_HEX).unwrap();
        let ts_ms = 1_792_273_500_123_u64;

        // Signing a REST GET with query parameters must sign only `/trade-api/v2/markets`
        let headers = creds.sign_request(
            ts_ms,
            "get",
            "/trade-api/v2/markets?status=open&limit=1000&cursor=abc",
        );
        assert_eq!(headers.access_key, "kalshi-key-uuid-001");
        assert_eq!(headers.access_timestamp_ms, "1792273500123");

        let sig_bytes = BASE64_STANDARD.decode(&headers.access_signature).unwrap();
        let signature = ed25519_dalek::Signature::from_slice(&sig_bytes).unwrap();
        let expected_payload = b"1792273500123GET/trade-api/v2/markets";
        assert!(creds
            .verifying_key()
            .verify(expected_payload, &signature)
            .is_ok());

        // WebSocket handshake signature check
        let ws_headers = creds.sign_ws_handshake(ts_ms);
        let ws_sig_bytes = BASE64_STANDARD
            .decode(&ws_headers.access_signature)
            .unwrap();
        let ws_sig = ed25519_dalek::Signature::from_slice(&ws_sig_bytes).unwrap();
        assert!(creds
            .verifying_key()
            .verify(b"1792273500123GET/trade-api/ws/v2", &ws_sig)
            .is_ok());
    }

    #[test]
    fn test_kalshi_secrets_manager_json_loader() {
        let secret_json = serde_json::json!({
            "KALSHI_API_KEY_ID": "leg-a-key-id",
            "KALSHI_PRIVATE_KEY_PEM": TEST_SEED_HEX
        })
        .to_string();
        let creds = KalshiCredentials::from_secrets_manager_json(&secret_json).unwrap();
        assert_eq!(creds.api_key_id(), "leg-a-key-id");
    }

    #[test]
    fn test_kalshi_ws_wire_frames_snapshot_delta_trade_ticker_lifecycle() {
        let interner = InstrumentInterner::new();
        let mut normalizer = KalshiWireNormalizer::new(CollectorRuntimeConfig::default());

        // 1. Parse `orderbook_snapshot`
        let snap_json = r#"{
            "type": "orderbook_snapshot",
            "sid": 2,
            "seq": 10,
            "msg": {
                "market_ticker": "KXFED-26NOV-T425",
                "market_id": "2b8f5b68-4f9f-9f02-9a2d-1b8ac1a10000",
                "yes_dollars_fp": [["0.5300", "200.00"], ["0.5400", "100.00"]],
                "no_dollars_fp": [["0.4400", "150.00"], ["0.4300", "250.00"]]
            }
        }"#;
        let envs = normalizer
            .parse_ws_frame(snap_json, 1_792_273_000_000_000_000, &interner)
            .unwrap();
        assert_eq!(envs.len(), 1);
        match &envs[0] {
            MarketDataEnvelope::L2Checkpoint(cp) => {
                // Bids sorted descending: 0.54 ($540,000 micros) first, then 0.53 ($530,000 micros)
                assert_eq!(cp.bids_price_micros, vec![540_000, 530_000]);
                assert_eq!(cp.bids_size_micros, vec![100_000_000, 200_000_000]);
                // NO bids at 0.44 and 0.43 project to YES asks at 0.56 and 0.57 sorted ascending
                assert_eq!(cp.asks_price_micros, vec![560_000, 570_000]);
                assert_eq!(cp.asks_size_micros, vec![150_000_000, 250_000_000]);
            }
            other => panic!("expected L2Checkpoint, got {other:?}"),
        }

        // 2. Parse `orderbook_delta`
        let delta_json = r#"{
            "type": "orderbook_delta",
            "sid": 2,
            "seq": 11,
            "msg": {
                "market_ticker": "KXFED-26NOV-T425",
                "price_dollars": "0.5400",
                "delta_fp": "-25.50",
                "side": "yes",
                "ts_ms": 1792273000500
            }
        }"#;
        let delta_envs = normalizer
            .parse_ws_frame(delta_json, 1_792_273_000_500_000_000, &interner)
            .unwrap();
        assert_eq!(delta_envs.len(), 1);
        match &delta_envs[0] {
            MarketDataEnvelope::L2Delta(d) => {
                assert_eq!(d.side, BookSide::KalshiYes);
                assert_eq!(d.price_micros, PriceMicros(540_000));
                assert_eq!(d.size_micros, SizeMicros(-25_500_000));
                assert_eq!(d.seq, 11);
            }
            other => panic!("expected L2Delta, got {other:?}"),
        }

        // 3. Parse `trade`
        let trade_json = r#"{
            "type": "trade",
            "sid": 1,
            "msg": {
                "trade_id": "d91bc2a2-1111-2222-3333-444455556666",
                "market_ticker": "KXFED-26NOV-T425",
                "yes_price_dollars": "0.5600",
                "no_price_dollars": "0.4400",
                "count_fp": "12.75",
                "taker_side": "yes",
                "is_block_trade": false,
                "ts_ms": 1792273000600
            }
        }"#;
        let trade_envs = normalizer
            .parse_ws_frame(trade_json, 1_792_273_000_600_000_000, &interner)
            .unwrap();
        assert_eq!(trade_envs.len(), 1);
        match &trade_envs[0] {
            MarketDataEnvelope::Trade(tr) => {
                assert_eq!(tr.price_micros, PriceMicros(560_000));
                assert_eq!(tr.complement_price_micros, PriceMicros(440_000));
                assert_eq!(tr.size_micros, 12_750_000);
                assert_eq!(tr.taker_side, TakerSide::Yes);
            }
            other => panic!("expected Trade, got {other:?}"),
        }

        // 4. Parse `ticker` and `market_lifecycle_v2`
        let ticker_json = r#"{
            "type": "ticker",
            "sid": 1,
            "msg": {
                "market_ticker": "KXFED-26NOV-T425",
                "price_dollars": "0.5600",
                "yes_bid_dollars": "0.5400",
                "yes_ask_dollars": "0.5600",
                "yes_bid_size_fp": "74.50",
                "yes_ask_size_fp": "137.25",
                "volume_fp": "15420.00",
                "open_interest_fp": "8900.00",
                "dollar_volume": 8500,
                "dollar_open_interest": 4900,
                "ts_ms": 1792273000700
            }
        }"#;
        let ticker_envs = normalizer
            .parse_ws_frame(ticker_json, 1_792_273_000_700_000_000, &interner)
            .unwrap();
        assert!(matches!(
            ticker_envs.as_slice(),
            [MarketDataEnvelope::Ticker(_)]
        ));

        let lifecycle_json = r#"{
            "type": "market_lifecycle_v2",
            "sid": 1,
            "msg": {
                "event_type": "determined",
                "market_ticker": "KXFED-26NOV-T425",
                "result": "yes"
            }
        }"#;
        let lc_envs = normalizer
            .parse_ws_frame(lifecycle_json, 1_792_273_000_800_000_000, &interner)
            .unwrap();
        assert!(matches!(
            lc_envs.as_slice(),
            [MarketDataEnvelope::Lifecycle(_)]
        ));
    }

    #[test]
    fn test_load_provided_kalshi_pkcs8_credentials_and_sign_ws_handshake() {
        let env_path = std::path::Path::new(
            "/cursor/stores/bc-2ce999e0-7f56-421c-9444-f12c4cf2154d/internal/kalshi-credentials.env",
        );
        let pem_path = std::path::Path::new(
            "/cursor/stores/bc-2ce999e0-7f56-421c-9444-f12c4cf2154d/internal/kalshi_private_key.pem",
        );
        if !env_path.exists() {
            return;
        }

        let creds_from_env_file = KalshiCredentials::from_dotenv_file(env_path).unwrap();
        let creds_from_pem_file =
            KalshiCredentials::from_key_file(creds_from_env_file.api_key_id(), pem_path).unwrap();

        // Verify both loading methods yield the exact same Ed25519 public verifying key
        assert_eq!(
            creds_from_env_file.verifying_key(),
            creds_from_pem_file.verifying_key()
        );

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let ws_headers = creds_from_env_file.sign_ws_handshake(now_ms);
        let sig_bytes = BASE64_STANDARD
            .decode(&ws_headers.access_signature)
            .unwrap();
        let sig = ed25519_dalek::Signature::from_slice(&sig_bytes).unwrap();
        let expected_msg = format!("{now_ms}GET/trade-api/ws/v2");
        assert!(creds_from_env_file
            .verifying_key()
            .verify(expected_msg.as_bytes(), &sig)
            .is_ok());
    }
}

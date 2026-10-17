use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::fmt;
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SchemaError {
    #[error("empty numeric string")]
    EmptyNumericString,
    #[error("invalid character in fixed-point string: {0}")]
    InvalidCharacter(String),
    #[error("too many fractional digits (max 6): {0}")]
    TooManyFractionalDigits(String),
    #[error("numeric overflow in fixed-point conversion: {0}")]
    Overflow(String),
    #[error("price out of valid prediction market bounds: {0}")]
    PriceOutOfBounds(u32),
    #[error("zero-copy rkyv deserialization failed: {0}")]
    RkyvValidationFailed(String),
}

/// Venue identifier matching PostgreSQL `catalog.venues` and ClickHouse `venue_id`.
#[repr(u8)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub enum VenueId {
    Kalshi = 1,
    Polymarket = 2,
}

impl VenueId {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Kalshi => "kalshi",
            Self::Polymarket => "polymarket",
        }
    }
}

/// Fixed-point price in micro-dollars (`1_000_000` = `$1.000000`).
/// Eliminates IEEE-754 floating-point drift across Kalshi cent/deci-cent and Polymarket ticks.
#[repr(transparent)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq, PartialOrd, Ord))]
pub struct PriceMicros(pub u32);

impl PriceMicros {
    pub const ZERO: Self = Self(0);
    pub const ONE_DOLLAR: Self = Self(1_000_000);
    pub const ONE_CENT: Self = Self(10_000);
    pub const ONE_DECI_CENT: Self = Self(1_000);

    /// Parses an ASCII fixed-point dollar string (e.g., `"0.56"`, `"0.5625"`, `"1"`) into micro-dollars
    /// without any floating-point conversion.
    pub fn parse_ascii(input: &str) -> Result<Self, SchemaError> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(SchemaError::EmptyNumericString);
        }
        if trimmed.starts_with('-') {
            return Err(SchemaError::InvalidCharacter(trimmed.to_string()));
        }
        let unsigned = trimmed.strip_prefix('+').unwrap_or(trimmed);
        let (int_part, frac_part) = match unsigned.split_once('.') {
            Some((i, f)) => (i, f),
            None => (unsigned, ""),
        };

        if frac_part.len() > 6 {
            return Err(SchemaError::TooManyFractionalDigits(trimmed.to_string()));
        }

        let int_val: u64 = if int_part.is_empty() {
            0
        } else {
            int_part
                .parse::<u64>()
                .map_err(|_| SchemaError::InvalidCharacter(trimmed.to_string()))?
        };

        let mut frac_val: u64 = 0;
        for &b in frac_part.as_bytes() {
            if !b.is_ascii_digit() {
                return Err(SchemaError::InvalidCharacter(trimmed.to_string()));
            }
            frac_val = frac_val * 10 + (b - b'0') as u64;
        }
        let scale = 10_u64.pow((6 - frac_part.len()) as u32);
        let total = int_val
            .checked_mul(1_000_000)
            .and_then(|v| v.checked_add(frac_val * scale))
            .ok_or_else(|| SchemaError::Overflow(trimmed.to_string()))?;

        let micros =
            u32::try_from(total).map_err(|_| SchemaError::Overflow(trimmed.to_string()))?;
        Ok(Self(micros))
    }

    /// Computes the binary complement price (`$1.000000 - price`), used to project
    /// Kalshi `NO` bids at price `p` into canonical `YES` asks at `$1.00 - p`.
    #[inline]
    pub const fn complement(self) -> Self {
        Self(Self::ONE_DOLLAR.0.saturating_sub(self.0))
    }

    /// Formats back to a 6-decimal fixed-point string (`"0.542500"`).
    pub fn to_fixed_string(self) -> String {
        format!("{}.{:06}", self.0 / 1_000_000, self.0 % 1_000_000)
    }
}

impl fmt::Display for PriceMicros {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{:06}", self.0 / 1_000_000, self.0 % 1_000_000)
    }
}

/// Fixed-point contract or token share quantity in micro-units (`1_000_000` = `1.000000`).
/// Signed (`i64`) to represent both Kalshi signed relative deltas (`delta_fp`, e.g., `"-54.00"`)
/// and Polymarket absolute level quantities (`size`, e.g., `"219.217767"`).
#[repr(transparent)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq, PartialOrd, Ord))]
pub struct SizeMicros(pub i64);

impl SizeMicros {
    pub const ZERO: Self = Self(0);
    pub const ONE_UNIT: Self = Self(1_000_000);

    /// Parses an ASCII fixed-point signed or unsigned quantity string (up to 6 decimal places)
    /// into micro-units without IEEE-754 floating-point conversion.
    pub fn parse_ascii(input: &str) -> Result<Self, SchemaError> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(SchemaError::EmptyNumericString);
        }
        let (is_negative, unsigned) = if let Some(rest) = trimmed.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = trimmed.strip_prefix('+') {
            (false, rest)
        } else {
            (false, trimmed)
        };

        if unsigned.is_empty() {
            return Err(SchemaError::InvalidCharacter(trimmed.to_string()));
        }

        let (int_part, frac_part) = match unsigned.split_once('.') {
            Some((i, f)) => (i, f),
            None => (unsigned, ""),
        };

        if frac_part.len() > 6 {
            return Err(SchemaError::TooManyFractionalDigits(trimmed.to_string()));
        }

        let int_val: i128 = if int_part.is_empty() {
            0
        } else {
            int_part
                .parse::<i128>()
                .map_err(|_| SchemaError::InvalidCharacter(trimmed.to_string()))?
        };

        let mut frac_val: i128 = 0;
        for &b in frac_part.as_bytes() {
            if !b.is_ascii_digit() {
                return Err(SchemaError::InvalidCharacter(trimmed.to_string()));
            }
            frac_val = frac_val * 10 + (b - b'0') as i128;
        }
        let scale = 10_i128.pow((6 - frac_part.len()) as u32);
        let mag = int_val
            .checked_mul(1_000_000)
            .and_then(|v| v.checked_add(frac_val * scale))
            .ok_or_else(|| SchemaError::Overflow(trimmed.to_string()))?;

        let signed = if is_negative { -mag } else { mag };
        let micros =
            i64::try_from(signed).map_err(|_| SchemaError::Overflow(trimmed.to_string()))?;
        Ok(Self(micros))
    }

    #[inline]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for SizeMicros {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.0 < 0 { "-" } else { "" };
        let abs = self.0.unsigned_abs();
        write!(f, "{}{}.{:06}", sign, abs / 1_000_000, abs % 1_000_000)
    }
}

/// Orderbook side representation preserving Kalshi native YES/NO bids alongside canonical BID/ASK.
#[repr(u8)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub enum BookSide {
    Bid = 1,
    Ask = 2,
    KalshiYes = 3,
    KalshiNo = 4,
}

impl BookSide {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Taker trade side representation.
#[repr(u8)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub enum TakerSide {
    Buy = 1,
    Sell = 2,
    Yes = 3,
    No = 4,
}

impl TakerSide {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Reason an L2 orderbook checkpoint row was persisted.
#[repr(u8)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub enum CheckpointReason {
    SubscribeInit = 1,
    Periodic15m = 2,
    GapResync = 3,
    PostTradeVerified = 4,
    RestSweep = 5,
}

impl CheckpointReason {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Anomaly / gap classification recorded in `market_data.capture_gaps`.
#[repr(u8)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub enum GapType {
    SeqGap = 1,
    BboMismatch = 2,
    HashMismatch = 3,
    WatchdogTimeout = 4,
    SocketEof = 5,
}

impl GapType {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Canonical L2 orderbook delta event corresponding to `market_data.l2_deltas`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub struct L2DeltaEvent {
    pub venue_id: VenueId,
    pub instrument_iid: u32,
    pub market_uid: u32,
    pub exchange_ts_ms: u64,
    pub sending_ts_ms: u64,
    pub recv_ts_ns: u64,
    pub ingest_ts_ns: u64,
    pub collector_session: u64,
    pub collector_leg: u8,
    pub sid: u32,
    pub seq: u64,
    pub side: BookSide,
    pub is_delta: bool,
    pub price_micros: PriceMicros,
    pub size_micros: SizeMicros,
    pub resulting_size_micros: u64,
    pub best_bid_micros: PriceMicros,
    pub best_ask_micros: PriceMicros,
    pub is_bbo_verified: bool,
    pub event_hash: String,
    pub dedup_key: u64,
}

impl L2DeltaEvent {
    /// Computes the deterministic content-based deduplication key across Active-Active collector legs.
    /// Note that connection-local `sid`, `collector_leg`, and `recv_ts_ns` are intentionally excluded
    /// so Leg A and Leg B produce the exact same `dedup_key` for an identical exchange state update.
    pub fn compute_dedup_key(
        venue_id: VenueId,
        instrument_iid: u32,
        market_uid: u32,
        exchange_ts_ms: u64,
        side: BookSide,
        is_delta: bool,
        price_micros: PriceMicros,
        size_micros: SizeMicros,
        resulting_size_micros: u64,
        event_hash: &str,
    ) -> u64 {
        let mut buf = Vec::with_capacity(48 + event_hash.len());
        buf.push(venue_id.as_u8());
        buf.extend_from_slice(&instrument_iid.to_le_bytes());
        buf.extend_from_slice(&market_uid.to_le_bytes());
        buf.extend_from_slice(&exchange_ts_ms.to_le_bytes());
        buf.push(side.as_u8());
        buf.push(u8::from(is_delta));
        buf.extend_from_slice(&price_micros.0.to_le_bytes());
        buf.extend_from_slice(&size_micros.0.to_le_bytes());
        buf.extend_from_slice(&resulting_size_micros.to_le_bytes());
        buf.extend_from_slice(event_hash.as_bytes());
        xxh3_64(&buf)
    }
}

/// Canonical L2 orderbook checkpoint event corresponding to `market_data.l2_checkpoints`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub struct L2CheckpointEvent {
    pub venue_id: VenueId,
    pub instrument_iid: u32,
    pub market_uid: u32,
    pub exchange_ts_ms: u64,
    pub recv_ts_ns: u64,
    pub collector_session: u64,
    pub seq: u64,
    pub checkpoint_reason: CheckpointReason,
    pub book_hash: String,
    pub bids_price_micros: Vec<u32>,
    pub bids_size_micros: Vec<u64>,
    pub asks_price_micros: Vec<u32>,
    pub asks_size_micros: Vec<u64>,
    pub dedup_key: u64,
}

impl L2CheckpointEvent {
    pub fn compute_dedup_key(
        venue_id: VenueId,
        instrument_iid: u32,
        exchange_ts_ms: u64,
        checkpoint_reason: CheckpointReason,
        book_hash: &str,
    ) -> u64 {
        let mut buf = Vec::with_capacity(24 + book_hash.len());
        buf.push(venue_id.as_u8());
        buf.extend_from_slice(&instrument_iid.to_le_bytes());
        buf.extend_from_slice(&exchange_ts_ms.to_le_bytes());
        buf.push(checkpoint_reason.as_u8());
        buf.extend_from_slice(book_hash.as_bytes());
        xxh3_64(&buf)
    }
}

/// Canonical public trade event corresponding to `market_data.trades`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub struct TradeEvent {
    pub venue_id: VenueId,
    pub instrument_iid: u32,
    pub market_uid: u32,
    pub exchange_ts_ms: u64,
    pub sending_ts_ms: u64,
    pub recv_ts_ns: u64,
    pub leg_a_recv_ns: u64,
    pub leg_b_recv_ns: u64,
    pub trade_id: String,
    pub tx_hash: String,
    pub price_micros: PriceMicros,
    pub complement_price_micros: PriceMicros,
    pub size_micros: u64,
    pub taker_side: TakerSide,
    pub fee_rate_bps: u16,
    pub is_block_trade: bool,
    pub dedup_key: u64,
}

impl TradeEvent {
    pub fn compute_dedup_key(
        venue_id: VenueId,
        instrument_iid: u32,
        exchange_ts_ms: u64,
        trade_id: &str,
        tx_hash: &str,
        price_micros: PriceMicros,
        size_micros: u64,
        taker_side: TakerSide,
    ) -> u64 {
        let mut buf = Vec::with_capacity(32 + trade_id.len() + tx_hash.len());
        buf.push(venue_id.as_u8());
        buf.extend_from_slice(&instrument_iid.to_le_bytes());
        buf.extend_from_slice(&exchange_ts_ms.to_le_bytes());
        buf.extend_from_slice(&price_micros.0.to_le_bytes());
        buf.extend_from_slice(&size_micros.to_le_bytes());
        buf.push(taker_side.as_u8());
        buf.extend_from_slice(trade_id.as_bytes());
        buf.push(0xff);
        buf.extend_from_slice(tx_hash.as_bytes());
        xxh3_64(&buf)
    }
}

/// Top-of-book ticker / open-interest event corresponding to `market_data.tickers`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub struct TickerEvent {
    pub venue_id: VenueId,
    pub instrument_iid: u32,
    pub market_uid: u32,
    pub exchange_ts_ms: u64,
    pub recv_ts_ns: u64,
    pub last_price_micros: PriceMicros,
    pub best_bid_micros: PriceMicros,
    pub best_ask_micros: PriceMicros,
    pub best_bid_size_micros: u64,
    pub best_ask_size_micros: u64,
    pub volume_micros: u64,
    pub open_interest_micros: u64,
    pub dollar_volume: u64,
    pub dollar_open_interest: u64,
    pub dedup_key: u64,
}

/// Market lifecycle transition event corresponding to `market_data.lifecycle_events`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub struct LifecycleEvent {
    pub venue_id: VenueId,
    pub market_uid: u32,
    pub instrument_iid: u32,
    pub exchange_ts_ms: u64,
    pub recv_ts_ns: u64,
    pub lifecycle_type: String,
    pub old_tick_size_micros: PriceMicros,
    pub new_tick_size_micros: PriceMicros,
    pub result: String,
    pub settlement_value_micros: i64,
    pub payload_json: String,
    pub dedup_key: u64,
}

/// Underlying oracle / settlement index event corresponding to `market_data.oracle_ticks`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub struct OracleTickEvent {
    pub venue_id: VenueId,
    pub source: String,
    pub symbol: String,
    pub exchange_ts_ms: u64,
    pub recv_ts_ns: u64,
    pub seq: u64,
    pub dropped_count: u32,
    pub price_micros: i64,
    pub confidence_micros: u64,
    pub dedup_key: u64,
}

/// Collector anomaly / sequence gap audit record corresponding to `market_data.capture_gaps`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub struct CaptureGapEvent {
    pub venue_id: VenueId,
    pub instrument_iid: u32,
    pub collector_session: u64,
    pub collector_leg: u8,
    pub shard_id: u16,
    pub gap_type: GapType,
    pub gap_start_ns: u64,
    pub gap_resolved_ns: u64,
    pub expected_seq: u64,
    pub observed_seq: u64,
    pub healed_by_twin_leg: bool,
    pub recovery_checkpoint_seq: u64,
}

/// Unified zero-copy wire envelope persisted to `alkera-wal` and Redpanda topics.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Archive,
    Serialize,
    Deserialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub enum MarketDataEnvelope {
    L2Delta(L2DeltaEvent),
    L2Checkpoint(L2CheckpointEvent),
    Trade(TradeEvent),
    Ticker(TickerEvent),
    Lifecycle(LifecycleEvent),
    Oracle(OracleTickEvent),
    CaptureGap(CaptureGapEvent),
}

impl MarketDataEnvelope {
    /// Serializes this envelope into zero-copy `rkyv` aligned bytes.
    pub fn to_rkyv_bytes(&self) -> Result<Vec<u8>, SchemaError> {
        rkyv::to_bytes::<_, 1024>(self)
            .map(|aligned| aligned.into_vec())
            .map_err(|e| SchemaError::RkyvValidationFailed(e.to_string()))
    }

    /// Validates and deserializes a `MarketDataEnvelope` from raw `rkyv` bytes.
    /// Automatically uses zero-copy validation when `bytes` is 8-byte aligned, or copies into
    /// an `AlignedVec` if the caller passes an unaligned slice.
    pub fn from_rkyv_bytes(bytes: &[u8]) -> Result<Self, SchemaError> {
        if (bytes.as_ptr() as usize) % 8 == 0 {
            rkyv::from_bytes::<Self>(bytes)
                .map_err(|e| SchemaError::RkyvValidationFailed(e.to_string()))
        } else {
            let mut aligned = rkyv::AlignedVec::with_capacity(bytes.len());
            aligned.extend_from_slice(bytes);
            rkyv::from_bytes::<Self>(&aligned)
                .map_err(|e| SchemaError::RkyvValidationFailed(e.to_string()))
        }
    }

    /// Returns the primary `dedup_key` for deduplicated stream types.
    pub fn dedup_key(&self) -> Option<u64> {
        match self {
            Self::L2Delta(e) => Some(e.dedup_key),
            Self::L2Checkpoint(e) => Some(e.dedup_key),
            Self::Trade(e) => Some(e.dedup_key),
            Self::Ticker(e) => Some(e.dedup_key),
            Self::Lifecycle(e) => Some(e.dedup_key),
            Self::Oracle(e) => Some(e.dedup_key),
            Self::CaptureGap(_) => None,
        }
    }

    /// Returns the receive timestamp in nanoseconds (`recv_ts_ns`).
    pub fn recv_ts_ns(&self) -> u64 {
        match self {
            Self::L2Delta(e) => e.recv_ts_ns,
            Self::L2Checkpoint(e) => e.recv_ts_ns,
            Self::Trade(e) => e.recv_ts_ns,
            Self::Ticker(e) => e.recv_ts_ns,
            Self::Lifecycle(e) => e.recv_ts_ns,
            Self::Oracle(e) => e.recv_ts_ns,
            Self::CaptureGap(e) => e.gap_start_ns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_micros_exact_ascii_parsing() {
        assert_eq!(PriceMicros::parse_ascii("0.56").unwrap(), PriceMicros(560_000));
        assert_eq!(
            PriceMicros::parse_ascii("0.5625").unwrap(),
            PriceMicros(562_500)
        );
        assert_eq!(
            PriceMicros::parse_ascii("0.001").unwrap(),
            PriceMicros(1_000)
        );
        assert_eq!(
            PriceMicros::parse_ascii("1.000000").unwrap(),
            PriceMicros::ONE_DOLLAR
        );
        assert_eq!(
            PriceMicros(780_000).complement(),
            PriceMicros(220_000)
        );
    }

    #[test]
    fn test_size_micros_signed_and_unsigned_parsing() {
        assert_eq!(
            SizeMicros::parse_ascii("-54.00").unwrap(),
            SizeMicros(-54_000_000)
        );
        assert_eq!(
            SizeMicros::parse_ascii("219.217767").unwrap(),
            SizeMicros(219_217_767)
        );
        assert_eq!(SizeMicros::parse_ascii("0").unwrap(), SizeMicros::ZERO);
    }

    #[test]
    fn test_rkyv_roundtrip_and_leg_independent_dedup_key() {
        let dedup = L2DeltaEvent::compute_dedup_key(
            VenueId::Kalshi,
            42,
            10,
            1_792_273_000_123,
            BookSide::KalshiYes,
            true,
            PriceMicros(540_000),
            SizeMicros(-25_000_000),
            100_000_000,
            "",
        );

        let leg_a = MarketDataEnvelope::L2Delta(L2DeltaEvent {
            venue_id: VenueId::Kalshi,
            instrument_iid: 42,
            market_uid: 10,
            exchange_ts_ms: 1_792_273_000_123,
            sending_ts_ms: 1_792_273_000_124,
            recv_ts_ns: 1_792_273_000_125_100_000,
            ingest_ts_ns: 1_792_273_000_125_150_000,
            collector_session: 1001,
            collector_leg: 0,
            sid: 7,
            seq: 99,
            side: BookSide::KalshiYes,
            is_delta: true,
            price_micros: PriceMicros(540_000),
            size_micros: SizeMicros(-25_000_000),
            resulting_size_micros: 100_000_000,
            best_bid_micros: PriceMicros(540_000),
            best_ask_micros: PriceMicros(550_000),
            is_bbo_verified: true,
            event_hash: String::new(),
            dedup_key: dedup,
        });

        let bytes = leg_a.to_rkyv_bytes().unwrap();
        let decoded = MarketDataEnvelope::from_rkyv_bytes(&bytes).unwrap();
        assert_eq!(leg_a, decoded);
        assert_eq!(decoded.dedup_key(), Some(dedup));

        // Verify unaligned slice deserialization (e.g., offset by 1 byte) still succeeds
        let mut unaligned_buf = vec![0_u8; bytes.len() + 1];
        unaligned_buf[1..].copy_from_slice(&bytes);
        let decoded_unaligned = MarketDataEnvelope::from_rkyv_bytes(&unaligned_buf[1..]).unwrap();
        assert_eq!(leg_a, decoded_unaligned);
    }

    #[test]
    fn test_fixed_point_edge_cases_and_invalid_inputs() {
        assert_eq!(
            PriceMicros::parse_ascii(""),
            Err(SchemaError::EmptyNumericString)
        );
        assert!(matches!(
            PriceMicros::parse_ascii("-0.10"),
            Err(SchemaError::InvalidCharacter(_))
        ));
        assert!(matches!(
            PriceMicros::parse_ascii("0.1234567"),
            Err(SchemaError::TooManyFractionalDigits(_))
        ));
        assert!(matches!(
            PriceMicros::parse_ascii("0.55abc"),
            Err(SchemaError::InvalidCharacter(_))
        ));
        assert_eq!(
            PriceMicros::parse_ascii("  +0.0001  ").unwrap(),
            PriceMicros(100)
        );
        assert_eq!(PriceMicros::ONE_DOLLAR.complement(), PriceMicros::ZERO);
        assert_eq!(PriceMicros::ZERO.complement(), PriceMicros::ONE_DOLLAR);

        assert_eq!(
            SizeMicros::parse_ascii("-0.01").unwrap(),
            SizeMicros(-10_000)
        );
        assert_eq!(
            SizeMicros::parse_ascii("+100000.50").unwrap(),
            SizeMicros(100_000_500_000)
        );
        assert!(matches!(
            SizeMicros::parse_ascii("-"),
            Err(SchemaError::InvalidCharacter(_))
        ));
        assert!(matches!(
            SizeMicros::parse_ascii("1.0000001"),
            Err(SchemaError::TooManyFractionalDigits(_))
        ));
    }
}

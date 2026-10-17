-- =============================================================================
-- Alkera HA Data Layer: PostgreSQL Relational Catalog & Discovery Schema
-- Migration: 001_catalog_schema.sql
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS catalog;

-- 1. Venues Reference Table
CREATE TABLE IF NOT EXISTS catalog.venues (
    venue_id            SMALLINT PRIMARY KEY CHECK (venue_id IN (1, 2)),
    code                TEXT NOT NULL UNIQUE, -- 'kalshi', 'polymarket'
    display_name        TEXT NOT NULL,
    primary_region      TEXT NOT NULL,        -- 'us-east-2', 'eu-west-2'
    fallback_region     TEXT,                 -- 'us-east-1', 'eu-west-1'
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO catalog.venues (venue_id, code, display_name, primary_region, fallback_region)
VALUES
    (1, 'kalshi', 'Kalshi CFTC Event Contracts Exchange', 'us-east-2', 'us-east-1'),
    (2, 'polymarket', 'Polymarket Global CLOB', 'eu-west-2', 'eu-west-1')
ON CONFLICT (venue_id) DO NOTHING;

-- 2. Series (Recurring event templates, e.g., KXHIGHNY, KXCPI, FED)
CREATE TABLE IF NOT EXISTS catalog.series (
    series_uid          SERIAL PRIMARY KEY,
    venue_id            SMALLINT NOT NULL REFERENCES catalog.venues(venue_id),
    native_series_id    TEXT NOT NULL,
    title               TEXT NOT NULL,
    category            TEXT,
    tags                TEXT[] NOT NULL DEFAULT '{}',
    frequency           TEXT,
    fee_type            TEXT,                 -- 'quadratic', 'quadratic_with_maker_fees', 'flat'
    fee_multiplier      NUMERIC(10, 6),
    exchange_index      TEXT,                 -- Kalshi matching engine shard index
    raw_metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (venue_id, native_series_id)
);
CREATE INDEX IF NOT EXISTS idx_series_venue_category ON catalog.series(venue_id, category);

-- 3. Events (Real-world fixtures / occurrences grouping 1..N markets)
CREATE TABLE IF NOT EXISTS catalog.events (
    event_uid           SERIAL PRIMARY KEY,
    venue_id            SMALLINT NOT NULL REFERENCES catalog.venues(venue_id),
    series_uid          INTEGER REFERENCES catalog.series(series_uid),
    native_event_id     TEXT NOT NULL,        -- Kalshi event_ticker or Polymarket Gamma event id
    slug                TEXT,
    title               TEXT NOT NULL,
    sub_title           TEXT,
    mutually_exclusive  BOOLEAN NOT NULL DEFAULT false, -- Kalshi mutually_exclusive / Polymarket negRisk
    neg_risk_market_id  TEXT,                 -- Polymarket negRiskMarketID (0x...)
    collateral_return   TEXT,                 -- 'MECNET', 'DIRECNET', 'binary', 'CTF'
    strike_date         TIMESTAMPTZ,
    strike_period       TEXT,
    fee_type_override   TEXT,
    fee_multiplier_override NUMERIC(10, 6),
    exchange_index      TEXT,
    raw_metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (venue_id, native_event_id)
);
CREATE INDEX IF NOT EXISTS idx_events_venue_slug ON catalog.events(venue_id, slug);
CREATE INDEX IF NOT EXISTS idx_events_series_uid ON catalog.events(series_uid);

-- 4. Markets (Individual binary/scalar/categorical questions or conditions)
CREATE TABLE IF NOT EXISTS catalog.markets (
    market_uid                  SERIAL PRIMARY KEY,
    venue_id                    SMALLINT NOT NULL REFERENCES catalog.venues(venue_id),
    event_uid                   INTEGER NOT NULL REFERENCES catalog.events(event_uid),
    native_market_id            TEXT NOT NULL, -- Kalshi market_ticker or Polymarket Gamma market id
    market_uuid                 UUID,          -- Kalshi market_id UUID
    condition_id                TEXT,          -- Polymarket 0x... 32-byte hex conditionId
    question_id                 TEXT,          -- Polymarket UMA questionID
    slug                        TEXT,
    protocol_version            TEXT NOT NULL DEFAULT 'v2', -- 'kalshi_v2', 'poly_ctf_v1', 'poly_v2'
    market_type                 TEXT NOT NULL, -- 'binary', 'scalar', 'categorical', 'mve_combo', 'perp'
    status                      TEXT NOT NULL, -- 'unopened', 'open', 'active', 'paused', 'closed', 'determined', 'settled'
    title                       TEXT NOT NULL,
    yes_sub_title               TEXT,
    no_sub_title                TEXT,
    rules_primary               TEXT,
    rules_secondary             TEXT,
    price_level_structure       TEXT,          -- 'linear_cent', 'deci_cent', 'tapered_deci_cent', 'dynamic_tick'
    price_ranges                JSONB,         -- Array of {start, end, step}
    tick_size_micros            INTEGER NOT NULL DEFAULT 10000,   -- $0.01 = 10,000 micro-dollars
    min_order_size_micros       BIGINT NOT NULL DEFAULT 1000000,  -- 1.00 = 1,000,000 micro-units
    notional_value_micros       BIGINT NOT NULL DEFAULT 1000000,  -- $1.00 = 1,000,000 micro-dollars
    fractional_trading_enabled  BOOLEAN NOT NULL DEFAULT false,
    neg_risk                    BOOLEAN NOT NULL DEFAULT false,
    can_close_early             BOOLEAN NOT NULL DEFAULT false,
    is_provisional              BOOLEAN NOT NULL DEFAULT false,
    strike_type                 TEXT,
    floor_strike                DOUBLE PRECISION,
    cap_strike                  DOUBLE PRECISION,
    open_time                   TIMESTAMPTZ,
    close_time                  TIMESTAMPTZ,
    expected_expiration_time    TIMESTAMPTZ,
    latest_expiration_time      TIMESTAMPTZ,
    settled_time                TIMESTAMPTZ,
    result                      TEXT,
    settlement_value_micros     BIGINT,
    exchange_index              TEXT,
    raw_metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (venue_id, native_market_id)
);
CREATE INDEX IF NOT EXISTS idx_markets_condition_id ON catalog.markets(condition_id) WHERE condition_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_markets_status ON catalog.markets(venue_id, status);
CREATE INDEX IF NOT EXISTS idx_markets_event_uid ON catalog.markets(event_uid);

-- 5. Tradeable Instruments (Dense UInt32 surrogate key used across all hot/cold tick tables)
CREATE TABLE IF NOT EXISTS catalog.instruments (
    instrument_iid          SERIAL PRIMARY KEY,
    venue_id                SMALLINT NOT NULL REFERENCES catalog.venues(venue_id),
    market_uid              INTEGER NOT NULL REFERENCES catalog.markets(market_uid) ON DELETE CASCADE,
    native_instrument_id    TEXT NOT NULL, -- Kalshi: "<ticker>:YES" / "<ticker>:NO"; Poly: 77-digit clobTokenId
    position_id_v2          TEXT,          -- Polymarket Protocol V2 positionId if applicable
    outcome_index           SMALLINT NOT NULL, -- 0=YES/Outcome0, 1=NO/Outcome1
    outcome_label           TEXT NOT NULL,     -- 'Yes', 'No', 'Chiefs', 'Chargers', etc.
    complement_iid          INTEGER REFERENCES catalog.instruments(instrument_iid),
    activity_tier           SMALLINT NOT NULL DEFAULT 1 CHECK (activity_tier IN (1, 2, 3)), -- 1=hot_ws, 2=warm_ws, 3=dormant_watched
    is_active               BOOLEAN NOT NULL DEFAULT true,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (venue_id, native_instrument_id)
);
CREATE INDEX IF NOT EXISTS idx_instruments_market_uid ON catalog.instruments(market_uid);
CREATE INDEX IF NOT EXISTS idx_instruments_active_tier ON catalog.instruments(venue_id, is_active, activity_tier);

-- 6. Real-Time Outbox Trigger for Lock-Free Collector Interner Sync
CREATE OR REPLACE FUNCTION catalog.notify_instrument_upserted()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'catalog_instrument_updates',
        json_build_object(
            'instrument_iid', NEW.instrument_iid,
            'venue_id', NEW.venue_id,
            'market_uid', NEW.market_uid,
            'native_instrument_id', NEW.native_instrument_id,
            'outcome_index', NEW.outcome_index,
            'complement_iid', NEW.complement_iid,
            'activity_tier', NEW.activity_tier,
            'is_active', NEW.is_active
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_instrument_upserted ON catalog.instruments;
CREATE TRIGGER trg_instrument_upserted
AFTER INSERT OR UPDATE ON catalog.instruments
FOR EACH ROW EXECUTE FUNCTION catalog.notify_instrument_upserted();

-- =============================================================================
-- Alkera HA Data Layer: Combos, Segments & Cross-Venue Equivalence Graph
-- Migration: 002_cross_venue_and_combos.sql
-- =============================================================================

-- 1. Kalshi Multivariate (Combo) Market Legs
CREATE TABLE IF NOT EXISTS catalog.mve_combo_legs (
    combo_market_uid        INTEGER NOT NULL REFERENCES catalog.markets(market_uid) ON DELETE CASCADE,
    leg_index               SMALLINT NOT NULL,
    leg_event_ticker        TEXT NOT NULL,
    leg_market_uid          INTEGER REFERENCES catalog.markets(market_uid),
    leg_market_ticker       TEXT NOT NULL,
    leg_side                TEXT NOT NULL CHECK (leg_side IN ('yes', 'no')),
    yes_settlement_micros   BIGINT,
    PRIMARY KEY (combo_market_uid, leg_index)
);
CREATE INDEX IF NOT EXISTS idx_mve_combo_legs_leg_market ON catalog.mve_combo_legs(leg_market_uid);

-- 2. User-Defined Market Segments & Dynamic Shard Memberships
CREATE TABLE IF NOT EXISTS catalog.market_segments (
    segment_id              TEXT PRIMARY KEY,
    name                    TEXT NOT NULL,
    priority_class          TEXT NOT NULL CHECK (priority_class IN ('critical', 'high', 'standard', 'bulk')),
    enabled                 BOOLEAN NOT NULL DEFAULT true,
    filter_rules            JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS catalog.instrument_segment_memberships (
    segment_id              TEXT NOT NULL REFERENCES catalog.market_segments(segment_id) ON DELETE CASCADE,
    instrument_iid          INTEGER NOT NULL REFERENCES catalog.instruments(instrument_iid) ON DELETE CASCADE,
    assigned_shard_id       INTEGER NOT NULL,
    assigned_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (segment_id, instrument_iid)
);
CREATE INDEX IF NOT EXISTS idx_segment_memberships_shard ON catalog.instrument_segment_memberships(segment_id, assigned_shard_id);

-- 3. Cross-Venue Canonical Markets & Instrument Mappings
CREATE TABLE IF NOT EXISTS catalog.canonical_markets (
    canonical_market_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_title         TEXT NOT NULL,
    category                TEXT NOT NULL,
    resolution_date         TIMESTAMPTZ,
    rules_equivalence       TEXT NOT NULL CHECK (rules_equivalence IN ('exact', 'oracle_variant', 'time_offset')),
    notes                   TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS catalog.instrument_mappings (
    canonical_market_id     UUID NOT NULL REFERENCES catalog.canonical_markets(canonical_market_id) ON DELETE CASCADE,
    instrument_iid          INTEGER NOT NULL REFERENCES catalog.instruments(instrument_iid) ON DELETE CASCADE,
    polarity                SMALLINT NOT NULL DEFAULT 1 CHECK (polarity IN (-1, 1)), -- +1 direct, -1 inverted
    confidence_score        NUMERIC(5, 4) NOT NULL DEFAULT 1.0000,
    verified_by             TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (canonical_market_id, instrument_iid)
);

-- 4. Market Metadata Audit History (SCD Type 2 Lifecycle Log)
CREATE TABLE IF NOT EXISTS catalog.market_lifecycle_audit (
    audit_id                BIGSERIAL PRIMARY KEY,
    market_uid              INTEGER NOT NULL REFERENCES catalog.markets(market_uid) ON DELETE CASCADE,
    lifecycle_event_type    TEXT NOT NULL, -- 'created', 'activated', 'deactivated', 'tick_size_change', 'determined', 'settled'
    previous_status         TEXT,
    new_status              TEXT,
    old_tick_size_micros    INTEGER,
    new_tick_size_micros    INTEGER,
    exchange_ts             TIMESTAMPTZ,
    recv_ts_ns              BIGINT NOT NULL,
    payload                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_lifecycle_audit_market ON catalog.market_lifecycle_audit(market_uid, recorded_at DESC);

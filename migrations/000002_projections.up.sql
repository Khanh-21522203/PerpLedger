-- 000002_projections.up.sql
-- Creates the projections schema: derived read-models rebuilt from event log.
-- Per doc §11: projections are eventually consistent and can be rebuilt at any time.

CREATE SCHEMA IF NOT EXISTS projections;

-- Balance projections: one row per (account_path, asset_id).
CREATE TABLE IF NOT EXISTS projections.balances (
    account_path  TEXT NOT NULL,
    asset_id      SMALLINT NOT NULL,
    balance       BIGINT NOT NULL DEFAULT 0,
    last_sequence BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (account_path, asset_id)
);

-- Position projections: one row per (user_id, market_id).
CREATE TABLE IF NOT EXISTS projections.positions (
    user_id            UUID NOT NULL,
    market_id          TEXT NOT NULL,
    side               SMALLINT NOT NULL DEFAULT 0,
    size               BIGINT NOT NULL DEFAULT 0,
    avg_entry_price    BIGINT NOT NULL DEFAULT 0,
    realized_pnl       BIGINT NOT NULL DEFAULT 0,
    last_funding_epoch BIGINT NOT NULL DEFAULT 0,
    liquidation_state  SMALLINT NOT NULL DEFAULT 0,
    version            BIGINT NOT NULL DEFAULT 0,
    last_sequence      BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, market_id)
);

-- Funding history: one row per (user_id, market_id, epoch_id).
CREATE TABLE IF NOT EXISTS projections.funding_history (
    id            BIGSERIAL PRIMARY KEY,
    user_id       UUID NOT NULL,
    market_id     TEXT NOT NULL,
    epoch_id      BIGINT NOT NULL,
    funding_rate  BIGINT NOT NULL,
    position_size BIGINT NOT NULL,
    mark_price    BIGINT NOT NULL,
    payment       BIGINT NOT NULL,
    timestamp     BIGINT NOT NULL,
    UNIQUE (user_id, market_id, epoch_id)
);

CREATE INDEX IF NOT EXISTS idx_funding_hist_user
    ON projections.funding_history (user_id, market_id, epoch_id DESC);

-- Liquidation history: one row per liquidation_id.
CREATE TABLE IF NOT EXISTS projections.liquidation_history (
    liquidation_id TEXT PRIMARY KEY,
    user_id        UUID NOT NULL,
    market_id      TEXT NOT NULL,
    triggered_at   BIGINT NOT NULL,
    initial_size   BIGINT NOT NULL,
    remaining_size BIGINT NOT NULL,
    state          SMALLINT NOT NULL,
    deficit        BIGINT NOT NULL DEFAULT 0,
    last_sequence  BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_liq_hist_user
    ON projections.liquidation_history (user_id, market_id);

-- Watermark: tracks the last sequence applied by each projection worker.
CREATE TABLE IF NOT EXISTS projections.watermark (
    worker_id     TEXT PRIMARY KEY,
    last_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Generic metadata key-value store for projections.
CREATE TABLE IF NOT EXISTS projections.metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

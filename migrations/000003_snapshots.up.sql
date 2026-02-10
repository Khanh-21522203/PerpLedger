-- 000003_snapshots.up.sql
-- Creates the snapshots table within event_log schema.
-- Per doc §11: snapshots capture full in-memory state for warm/cold restart recovery.

CREATE TABLE IF NOT EXISTS event_log.snapshots (
    sequence   BIGINT PRIMARY KEY,
    data       JSONB NOT NULL,
    state_hash BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    verified   BOOLEAN NOT NULL DEFAULT FALSE
);

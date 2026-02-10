-- 000001_event_log.up.sql
-- Creates the event_log schema: append-only event store + double-entry journal.
-- Per doc §11 (Storage and Snapshots): event_log.events is the source of truth.

CREATE SCHEMA IF NOT EXISTS event_log;

-- Append-only event log. Each row is one processed event with its state hash.
CREATE TABLE IF NOT EXISTS event_log.events (
    sequence        BIGINT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    market_id       TEXT,
    payload         JSONB NOT NULL,
    state_hash      BYTEA NOT NULL,
    prev_hash       BYTEA NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    source_sequence BIGINT NOT NULL
);

-- Two-tier idempotency: cold-path unique index for Postgres dedup lookup.
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idem
    ON event_log.events (event_type, idempotency_key);

-- Double-entry journal. Every balance mutation is a debit/credit pair.
CREATE TABLE IF NOT EXISTS event_log.journal (
    journal_id     TEXT PRIMARY KEY,
    batch_id       TEXT NOT NULL,
    event_ref      TEXT NOT NULL,
    sequence       BIGINT NOT NULL,
    debit_account  TEXT NOT NULL,
    credit_account TEXT NOT NULL,
    asset_id       SMALLINT NOT NULL,
    amount         BIGINT NOT NULL,
    journal_type   INTEGER NOT NULL,
    timestamp      BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_journal_batch ON event_log.journal (batch_id);
CREATE INDEX IF NOT EXISTS idx_journal_seq   ON event_log.journal (sequence);

-- Immutability trigger: prevent UPDATE/DELETE on the event log.
CREATE OR REPLACE FUNCTION event_log.prevent_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'event_log.events is append-only: % not allowed', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS no_update_events ON event_log.events;
CREATE TRIGGER no_update_events
    BEFORE UPDATE OR DELETE ON event_log.events
    FOR EACH ROW EXECUTE FUNCTION event_log.prevent_mutation();

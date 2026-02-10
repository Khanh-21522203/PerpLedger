-- 000001_event_log.down.sql
-- Rolls back the event_log schema.

DROP TRIGGER IF EXISTS no_update_events ON event_log.events;
DROP FUNCTION IF EXISTS event_log.prevent_mutation();
DROP INDEX IF EXISTS event_log.idx_journal_seq;
DROP INDEX IF EXISTS event_log.idx_journal_batch;
DROP INDEX IF EXISTS event_log.idx_events_idem;
DROP TABLE IF EXISTS event_log.journal;
DROP TABLE IF EXISTS event_log.events;
DROP SCHEMA IF EXISTS event_log;

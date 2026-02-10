-- 000003_snapshots.down.sql
-- Rolls back the snapshots table.

DROP TABLE IF EXISTS event_log.snapshots;

-- 000002_projections.down.sql
-- Rolls back the projections schema.

DROP INDEX IF EXISTS projections.idx_liq_hist_user;
DROP INDEX IF EXISTS projections.idx_funding_hist_user;
DROP TABLE IF EXISTS projections.metadata;
DROP TABLE IF EXISTS projections.watermark;
DROP TABLE IF EXISTS projections.liquidation_history;
DROP TABLE IF EXISTS projections.funding_history;
DROP TABLE IF EXISTS projections.positions;
DROP TABLE IF EXISTS projections.balances;
DROP SCHEMA IF EXISTS projections;

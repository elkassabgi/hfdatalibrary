-- Task #134 / R430 class: handlePublicStats (and /v1/admin/stats) full-scanned
-- download_log on every hit — COUNT(*), SUM(bytes_served), GROUP BY ticker and
-- GROUP BY version each read the whole ~1.4M-row table, measured at ~440M rows
-- read/day on 2026-08-17 (the same query-shape class that billed $82/day on the
-- econ catalog). These counters are maintained incrementally at the three
-- download_log insert sites (bumpStatsCounters in api/src/index.js), so the
-- stats endpoints read a handful of rows instead.
--
-- Run order: execute this BEFORE deploying the code that reads the tables, then
-- re-run it once AFTER the deploy — the backfills are atomic exact recomputes
-- (INSERT ... SELECT over download_log), so the second pass absorbs any rows
-- logged between backfill and deploy without double-counting.
--
--   npx wrangler d1 execute hfdatalibrary-db --remote --file=migrate_stats_counters.sql

CREATE TABLE IF NOT EXISTS stats_totals (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  downloads INTEGER NOT NULL DEFAULT 0,
  bytes INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS stats_ticker_counts (
  ticker TEXT NOT NULL PRIMARY KEY,
  downloads INTEGER NOT NULL DEFAULT 0,
  bytes INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS stats_version_counts (
  version TEXT NOT NULL PRIMARY KEY,
  downloads INTEGER NOT NULL DEFAULT 0
);

-- Exact backfill. COALESCE guards legacy NULL ticker/version rows against the
-- NOT NULL keys; '' groups them under one bucket exactly as GROUP BY grouped
-- the NULLs.
INSERT OR REPLACE INTO stats_totals (id, downloads, bytes)
  SELECT 1, COUNT(*), COALESCE(SUM(bytes_served), 0) FROM download_log;

DELETE FROM stats_ticker_counts;
INSERT INTO stats_ticker_counts (ticker, downloads, bytes)
  SELECT COALESCE(ticker, ''), COUNT(*), COALESCE(SUM(bytes_served), 0)
  FROM download_log GROUP BY COALESCE(ticker, '');

DELETE FROM stats_version_counts;
INSERT INTO stats_version_counts (version, downloads)
  SELECT COALESCE(version, ''), COUNT(*)
  FROM download_log GROUP BY COALESCE(version, '');

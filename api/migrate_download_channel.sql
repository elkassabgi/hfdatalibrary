-- Migration: download-channel tracking (api / web / mcp) for the admin dashboard.
-- Author: Ahmed Elkassabgi
--
-- ORDER MATTERS: run this migration BEFORE deploying the worker that writes the
-- `channel` columns, or the INSERTs will fail (the worker fault-isolates the
-- download_log insert, but download_tokens would still error):
--   npx wrangler d1 execute hfdatalibrary-db --remote --file api/migrate_download_channel.sql
-- Safe to run AHEAD of the deploy: the old worker uses named-column INSERTs and
-- both new columns are nullable. ONE-SHOT: ALTER TABLE ADD COLUMN is not
-- idempotent — a re-run fails with "duplicate column name" (harmless; the
-- index/backfill statements below are guarded and can re-run).
--
-- Channel is derived at download time from signals in the request:
--   via=mcp query param        -> 'mcp'  (links handed out by the elkassabgidata MCP)
--   token issued via session   -> 'web'  (the site's search-and-download flow)
--   token issued via X-API-Key -> 'api'  (signed-token flow is ALSO the documented
--                                         API path for timeframes/CSV)
--   direct authenticated call  -> 'api'

ALTER TABLE download_log ADD COLUMN channel TEXT;
CREATE INDEX IF NOT EXISTS idx_download_log_channel ON download_log(channel);

-- The channel a download token was ISSUED through (web session vs API key vs
-- mcp). handleDownload logs the token's issue-channel, not "token = web".
ALTER TABLE download_tokens ADD COLUMN channel TEXT;

-- Backfill the UNAMBIGUOUS historical rows: these endpoints were always pure API.
UPDATE download_log SET channel = 'api'
  WHERE channel IS NULL AND endpoint IN ('/v1/bars', '/v1/variables', '/v1/quality');

-- NOTE: historical '/v1/download' rows are intentionally left NULL. They mix
-- website + programmatic token-flow (API) + MCP traffic and no channel marker
-- was persisted, so they cannot be split retroactively. The stats query
-- COALESCEs NULL -> 'web' for display, so the pre-tracking "Website" bucket is
-- an UPPER BOUND (it absorbs old API-token and MCP downloads). The UI captions
-- these rows as pre-tracking; the exact split starts at channel_tracked_since.

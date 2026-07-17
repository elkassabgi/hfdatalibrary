-- M1 additive migration (authored against the TRUE live schema 2026-07-17).
-- D1 has no atomic multi-statement DDL and no DROP COLUMN — each statement is
-- applied SEPARATELY with verify-after-each. All additions are nullable / IF NOT
-- EXISTS so legacy rows and the running M0 worker are unaffected (zero downtime).

-- (1) sessions.kind — NULL/legacy == 'web'. Validator predicate keys on this.
ALTER TABLE sessions ADD COLUMN kind TEXT;

-- (2) sessions.audience — caller origin for family_access tokens (revocation/audit).
ALTER TABLE sessions ADD COLUMN audience TEXT;

-- (3) Client registry (extensibility linchpin + security root). Owner-only writes.
CREATE TABLE IF NOT EXISTS sso_clients (
  origin         TEXT PRIMARY KEY,
  brand_name     TEXT,
  logo_url       TEXT,
  theme_json     TEXT,
  redirect_exact TEXT,
  status         TEXT NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active','suspended')),
  created_at     INTEGER
);

-- (4) One-time cross-domain codes. 60s TTL, hashed at rest, single-use enforced
--     atomically (UPDATE ... WHERE used=0 AND expires_at>now, act on changes==1).
CREATE TABLE IF NOT EXISTS sso_codes (
  code_hash      TEXT PRIMARY KEY,
  user_id        INTEGER NOT NULL,
  client_origin  TEXT NOT NULL,
  state          TEXT,
  code_challenge TEXT,
  consent_token  TEXT,
  used           INTEGER NOT NULL DEFAULT 0,
  expires_at     TEXT NOT NULL,
  created_at     TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (user_id) REFERENCES users(id)
);

-- (5) Purge helper for the M4 cron (expired-code sweep).
CREATE INDEX IF NOT EXISTS idx_sso_codes_expires ON sso_codes(expires_at);

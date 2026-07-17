-- M2 additive migration (family SSO issuer). Authored against the live schema +
-- the M1 migration. D1 has no atomic multi-statement DDL — apply each statement
-- SEPARATELY with verify-after-each. All additions are IF NOT EXISTS and inert
-- until the M2 code reads them (edl_at is a sessions row with kind='family_access',
-- so it needs no table). Rehearsed on a local copy of the live schema first.

-- (1) Family refresh tokens (edl_rt). Single-use + reuse-detection + 24h absolute
--     cap. Hashed at rest as token_hash = sha256Hex(raw edl_rt) -- no raw column.
CREATE TABLE IF NOT EXISTS sso_refresh_tokens (
  token_hash          TEXT PRIMARY KEY,
  user_id             INTEGER NOT NULL,
  audience            TEXT NOT NULL,
  chain_id            TEXT NOT NULL,
  parent_hash         TEXT,
  child_hash          TEXT,
  access_hash         TEXT,
  generation          INTEGER NOT NULL DEFAULT 0,
  used                INTEGER NOT NULL DEFAULT 0,
  used_at             TEXT,
  revoked             INTEGER NOT NULL DEFAULT 0,
  grace_until         TEXT,
  absolute_expires_at TEXT NOT NULL,
  expires_at          TEXT NOT NULL,
  created_at          TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_srt_chain ON sso_refresh_tokens(chain_id);

CREATE INDEX IF NOT EXISTS idx_srt_expiry ON sso_refresh_tokens(absolute_expires_at);

-- (2) OAuth broker state. Provider-bound, single-use, short TTL.
CREATE TABLE IF NOT EXISTS sso_oauth_state (
  state                  TEXT PRIMARY KEY,
  provider               TEXT NOT NULL,
  client_origin          TEXT NOT NULL,
  family_state           TEXT,
  family_code_challenge  TEXT,
  provider_code_verifier TEXT,
  nonce                  TEXT,
  link_user_id           INTEGER,
  used                   INTEGER NOT NULL DEFAULT 0,
  expires_at             TEXT NOT NULL,
  created_at             TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_sso_oauth_state_expires ON sso_oauth_state(expires_at);

-- (3) Speeds family_access lookups + revoke-by-kind sweeps.
CREATE INDEX IF NOT EXISTS idx_sessions_kind ON sessions(kind);

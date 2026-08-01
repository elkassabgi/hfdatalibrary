-- Security hardening tables
CREATE TABLE IF NOT EXISTS rate_limits (
  key TEXT PRIMARY KEY,
  count INTEGER DEFAULT 0,
  window_start TEXT DEFAULT (datetime('now'))
);
-- Rows here are dead once their window passes, and until 2026-07-31 nothing deleted them,
-- so the table only grew — in the same D1 that fails WRITES (logins, registrations) when it
-- fills. index.js prunes it from the cron and opportunistically from checkRateLimit; both
-- filter on window_start, so it needs an index or the sweep scans the whole table.
CREATE INDEX IF NOT EXISTS idx_rate_limits_window ON rate_limits(window_start);

CREATE TABLE IF NOT EXISTS admin_audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  admin_user_id INTEGER NOT NULL,
  admin_email TEXT NOT NULL,
  action TEXT NOT NULL,
  target_user_id INTEGER,
  target_email TEXT,
  details TEXT,
  ip_address TEXT,
  timestamp TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_audit_admin ON admin_audit_log(admin_user_id);
CREATE INDEX IF NOT EXISTS idx_audit_time ON admin_audit_log(timestamp);

-- Download tokens (for signed URLs)
CREATE TABLE IF NOT EXISTS download_tokens (
  token TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  ticker TEXT NOT NULL,
  version TEXT NOT NULL,
  format TEXT NOT NULL,
  channel TEXT,            -- 'web' | 'api' | 'mcp': how the token was issued
  expires_at TEXT NOT NULL,
  used INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_dl_tokens_user ON download_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_dl_tokens_expires ON download_tokens(expires_at);

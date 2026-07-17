CREATE TABLE admin_audit_log (
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
CREATE TABLE download_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  api_key TEXT,
  ticker TEXT,
  version TEXT,
  endpoint TEXT,
  ip_address TEXT,
  bytes_served INTEGER DEFAULT 0,
  timestamp TEXT DEFAULT (datetime('now'))
, channel TEXT);
CREATE TABLE download_tokens (
  token TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  ticker TEXT NOT NULL,
  version TEXT NOT NULL,
  format TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  used INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
, channel TEXT);
CREATE TABLE econ_download_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  series_id TEXT NOT NULL,
  ip TEXT,
  ts TEXT DEFAULT (datetime('now'))
, channel TEXT, bytes INTEGER DEFAULT 0);
CREATE TABLE login_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  ip_address TEXT,
  user_agent TEXT,
  country TEXT,
  success INTEGER DEFAULT 1,
  timestamp TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE TABLE newsletter_campaigns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subject TEXT NOT NULL,
  body_html TEXT NOT NULL,
  sent_at TEXT DEFAULT (datetime('now')),
  sent_by_user_id INTEGER,
  recipients_count INTEGER DEFAULT 0,
  success_count INTEGER DEFAULT 0,
  failed_count INTEGER DEFAULT 0,
  FOREIGN KEY (sent_by_user_id) REFERENCES users(id)
);
CREATE TABLE oauth_state (state TEXT PRIMARY KEY, user_id INTEGER, provider TEXT NOT NULL, expires_at TEXT NOT NULL, created_at TEXT DEFAULT (datetime('now')));
CREATE TABLE password_resets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  token TEXT NOT NULL UNIQUE,
  expires_at TEXT NOT NULL,
  used INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE TABLE rate_limits (
  key TEXT PRIMARY KEY,
  count INTEGER DEFAULT 0,
  window_start TEXT DEFAULT (datetime('now'))
);
CREATE TABLE sessions (
  id TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  ip_address TEXT,
  user_agent TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  expires_at TEXT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE TABLE totp_pending (token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, expires_at TEXT NOT NULL, ip_address TEXT, user_agent TEXT);
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  institution TEXT NOT NULL,
  country TEXT NOT NULL,
  role TEXT NOT NULL,
  api_key TEXT NOT NULL UNIQUE,
  is_active INTEGER DEFAULT 1,
  is_admin INTEGER DEFAULT 0,
  email_verified INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now')),
  last_login_at TEXT,
  last_login_ip TEXT,
  last_login_ua TEXT,
  login_count INTEGER DEFAULT 0,
  download_count INTEGER DEFAULT 0,
  total_bytes_downloaded INTEGER DEFAULT 0,
  notes TEXT DEFAULT ''
, newsletter_subscribed INTEGER DEFAULT 0, unsubscribe_token TEXT, is_vip INTEGER DEFAULT 0, api_key_expires_at TEXT, totp_secret TEXT, totp_enabled INTEGER DEFAULT 0, orcid_id TEXT, google_id TEXT, orcid_profile_json TEXT, profile_complete INTEGER DEFAULT 0, hide_institution INTEGER DEFAULT 0);
CREATE INDEX idx_audit_admin ON admin_audit_log(admin_user_id);
CREATE INDEX idx_audit_time ON admin_audit_log(timestamp);
CREATE INDEX idx_dl_tokens_expires ON download_tokens(expires_at);
CREATE INDEX idx_dl_tokens_user ON download_tokens(user_id);
CREATE INDEX idx_download_log_channel ON download_log(channel);
CREATE INDEX idx_download_log_time ON download_log(timestamp);
CREATE INDEX idx_download_log_user ON download_log(user_id);
CREATE INDEX idx_econ_dl_ts ON econ_download_log(ts);
CREATE INDEX idx_econ_dl_user_ts ON econ_download_log(user_id, ts);
CREATE INDEX idx_login_history_user ON login_history(user_id);
CREATE INDEX idx_password_resets_token ON password_resets(token);
CREATE INDEX idx_sessions_expires ON sessions(expires_at);
CREATE INDEX idx_sessions_user ON sessions(user_id);
CREATE INDEX idx_users_api_key ON users(api_key);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_newsletter ON users(newsletter_subscribed);
CREATE INDEX idx_users_unsubscribe ON users(unsubscribe_token);

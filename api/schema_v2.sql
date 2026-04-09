-- HF Data Library — D1 Schema v2
-- Full auth system with admin controls
-- Author: Ahmed Elkassabgi

-- Drop old tables
DROP TABLE IF EXISTS usage_log;
DROP TABLE IF EXISTS users;

-- Users table
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
);

-- Sessions table
CREATE TABLE sessions (
  id TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  ip_address TEXT,
  user_agent TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  expires_at TEXT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Login history
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

-- Download log
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
);

-- Password reset tokens
CREATE TABLE password_resets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  token TEXT NOT NULL UNIQUE,
  expires_at TEXT NOT NULL,
  used INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Indexes
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_api_key ON users(api_key);
CREATE INDEX idx_sessions_user ON sessions(user_id);
CREATE INDEX idx_sessions_expires ON sessions(expires_at);
CREATE INDEX idx_login_history_user ON login_history(user_id);
CREATE INDEX idx_download_log_user ON download_log(user_id);
CREATE INDEX idx_download_log_time ON download_log(timestamp);
CREATE INDEX idx_password_resets_token ON password_resets(token);

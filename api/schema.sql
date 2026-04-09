-- HF Data Library — D1 Schema
-- Author: Ahmed Elkassabgi

-- Users table
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  institution TEXT NOT NULL,
  role TEXT NOT NULL,
  api_key TEXT NOT NULL UNIQUE,
  created_at TEXT DEFAULT (datetime('now')),
  last_used_at TEXT,
  request_count INTEGER DEFAULT 0,
  is_active INTEGER DEFAULT 1
);

-- API usage log
CREATE TABLE IF NOT EXISTS usage_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  api_key TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  ticker TEXT,
  version TEXT,
  timestamp TEXT DEFAULT (datetime('now')),
  response_code INTEGER,
  bytes_served INTEGER DEFAULT 0
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_users_api_key ON users(api_key);
CREATE INDEX IF NOT EXISTS idx_usage_api_key ON usage_log(api_key);
CREATE INDEX IF NOT EXISTS idx_usage_timestamp ON usage_log(timestamp);

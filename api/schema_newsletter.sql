-- HF Data Library — Newsletter schema additions
-- Author: Ahmed Elkassabgi

-- Add newsletter fields to users
ALTER TABLE users ADD COLUMN newsletter_subscribed INTEGER DEFAULT 0;
ALTER TABLE users ADD COLUMN unsubscribe_token TEXT;

-- Newsletter campaigns table
CREATE TABLE IF NOT EXISTS newsletter_campaigns (
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

-- Index
CREATE INDEX IF NOT EXISTS idx_users_newsletter ON users(newsletter_subscribed);
CREATE INDEX IF NOT EXISTS idx_users_unsubscribe ON users(unsubscribe_token);

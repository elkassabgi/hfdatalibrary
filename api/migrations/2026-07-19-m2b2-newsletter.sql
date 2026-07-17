-- M2b-2 newsletter selection (the register-form bubble). Additive; per-list detail.
-- users.newsletter_subscribed stays the single gate the HF digest reads
-- (index.js) and the Resend webhook writes — UNTOUCHED. handleAccountsRegister
-- sets newsletter_subscribed = (HF box ticked ? 1 : 0); non-HF lists live only
-- here and feed future Econ/IP senders. Apply each statement separately.
CREATE TABLE IF NOT EXISTS newsletter_prefs (
  user_id    INTEGER NOT NULL,
  list_key   TEXT NOT NULL,              -- 'hf' | 'econ' | 'ip' | 'family'
  subscribed INTEGER NOT NULL DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (user_id, list_key),
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_newsletter_prefs_list ON newsletter_prefs(list_key, subscribed);

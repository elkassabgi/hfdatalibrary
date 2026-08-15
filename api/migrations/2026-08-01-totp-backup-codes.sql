-- Recovery codes for two-factor authentication.
--
-- Until today 2FA here was effectively decorative: enrolment was reachable without a verified
-- email, and both OAuth doors ignored totp_enabled entirely, so a "protected" account could be
-- entered with no code at all. Those holes were closed on 2026-08-01 — which is exactly what
-- makes the missing recovery path urgent rather than theoretical. The factor is now genuinely
-- enforced on every door, and there is no way back in: no backup codes, no admin reset, and
-- /2fa/disable itself demands a working code. A lost or wiped phone was a permanent lockout of
-- the account AND of every family site that authenticates through it.
--
-- Ten single-use codes, issued once when 2FA is enabled and shown once. Stored as SHA-256, not
-- plaintext: they are login credentials and a database copy must not be usable. A fast hash is
-- appropriate here where it would not be for passwords — these are 32 bits of CSPRNG output per
-- code with no human-chosen structure to guess, they are single-use, and the same per-account
-- 2FA rate limit that guards TOTP guesses guards these.
--
-- used_at rather than a bare flag so a support conversation can tell "spent last March" from
-- "never touched", which is the difference between a normal recovery and a compromise.
CREATE TABLE IF NOT EXISTS totp_backup_codes (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id    INTEGER NOT NULL,
  code_hash  TEXT NOT NULL,
  used       INTEGER NOT NULL DEFAULT 0,
  used_at    TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Verification looks a code up by (user, hash); the partial predicate keeps that a point read.
CREATE INDEX IF NOT EXISTS idx_totp_backup_user_hash ON totp_backup_codes(user_id, code_hash);

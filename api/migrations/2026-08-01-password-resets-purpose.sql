-- password_resets holds TWO different credentials and had no way to tell them apart.
--
-- Four writers mint EMAIL-VERIFICATION tokens (handleRegister, handleResendVerification,
-- handleAccountResendVerification, handleAccountsRegister) and one mints a PASSWORD-RESET token
-- (handleResetRequest). Both readers — handleVerifyEmail and handleReset — selected on
-- `token = ? AND used = 0 AND expires_at > now` and nothing else, so either token satisfied
-- either consumer.
--
-- The direction that matters: a verification link is emailed on sign-up and on every resend,
-- lives 24 HOURS (a reset token lives 1), and is the least-guarded URL the service sends. Anyone
-- who obtains one — a forwarded welcome email, a shared screen, a mailbox someone else can read,
-- a link scanner that follows URLs — could POST it to /v1/auth/reset and set a new password.
-- A token issued to prove "you can read this mailbox" was silently also a token that says
-- "replace this account's password", which is a much larger claim.
--
-- NULL means "minted before this column existed". The application treats that asymmetrically and
-- deliberately: handleVerifyEmail still accepts NULL (using an old token to confirm an email
-- address is harmless), while handleReset requires purpose = 'reset' and refuses NULL outright.
-- Fail closed on the dangerous consumer, stay permissive on the benign one. Measured immediately
-- before this migration: exactly 1 unused, unexpired row existed, so the cost of that strictness
-- is at most one person clicking "forgot password" a second time; the cost of a grace period is
-- that the takeover stays open for another 24 hours.
--
-- No DEFAULT and no backfill on purpose. A default would silently label the historic rows as
-- something they may not be, and every writer sets the value explicitly from here on, so the
-- only NULLs that can ever exist are the pre-migration ones.
ALTER TABLE password_resets ADD COLUMN purpose TEXT;

-- The readers look rows up by token; this keeps that lookup a point read once purpose is also
-- in the predicate, rather than degrading to a scan as the table grows between cron sweeps.
CREATE INDEX IF NOT EXISTS idx_password_resets_token_purpose
  ON password_resets(token, purpose);

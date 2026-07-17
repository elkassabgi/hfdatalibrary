"""M1 regression tests (AUTH_SSO_PLAN §7) for the getSessionUser rewrite.

Runs the EXACT SQL the worker uses against a local SQLite built from the true
live schema + the M1 migration, and asserts the security-critical invariants:

  1. A family_access-kind session is REJECTED (the `kind IS NULL OR kind='web'`
     predicate is what stops a family token authenticating a full/web session).
  2. A legacy (kind NULL) and an explicit 'web' session are ACCEPTED unchanged.
  3. user.id resolves to the USER id (the id-collision footgun is fixed).
  4. user.user_id is PRESERVED (read by ~20 handlers) and equals the user id.
  5. session_id exposes the real session token (distinct from user.id).

Self-contained; no network, no live DB. Run: python api/test/test_m1_getsessionuser.py
Exit 0 = all pass. Mirrors the schema files committed under api/.
"""
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
API = os.path.dirname(HERE)

# The exact query from getSessionUser (index.js §7). Keep in sync with the worker.
GET_SESSION_USER_SQL = (
    "SELECT u.*, s.user_id AS user_id, s.id AS session_id, "
    "s.kind AS session_kind, s.audience AS session_audience, "
    "s.expires_at AS session_expires_at "
    "FROM sessions s JOIN users u ON s.user_id = u.id "
    "WHERE s.id = ? AND s.expires_at > datetime('now') "
    "AND (s.kind IS NULL OR s.kind = 'web')"
)


def build_db():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(open(os.path.join(API, "schema_live_20260717.sql"), encoding="utf-8").read())
    # apply the M1 migration (adds sessions.kind/audience + sso tables)
    raw = open(os.path.join(API, "migrations", "2026-07-17-m1-sso.sql"), encoding="utf-8").read()
    for stmt in [s for s in raw.split(";")
                 if s.strip() and any(not l.strip().startswith("--") and l.strip() for l in s.splitlines())]:
        con.execute(stmt)
    con.execute("INSERT INTO users (id,name,email,password_hash,institution,country,role,api_key,is_active,is_admin) "
                "VALUES (42,'Tester','t@example.com','ph','UCA','US','user','KEY42',1,0)")
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind) VALUES ('sess_web_legacy',42,datetime('now','+1 day'),NULL)")
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind) VALUES ('sess_web_explicit',42,datetime('now','+1 day'),'web')")
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind) VALUES ('sess_family',42,datetime('now','+1 day'),'family_access')")
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind) VALUES ('sess_idp',42,datetime('now','+1 day'),'idp_master')")
    con.commit()
    return con


def getSessionUser(con, session_id):
    """Faithful port: query + post-assertion (kind must be null/web)."""
    row = con.execute(GET_SESSION_USER_SQL, (session_id,)).fetchone()
    if row is None:
        return None
    if not row["is_active"]:
        return None
    if row["session_kind"] is not None and row["session_kind"] != "web":
        return None  # defense-in-depth assertion (mirrors index.js)
    return row


def main():
    con = build_db()
    failures = []

    def check(name, cond):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        if not cond:
            failures.append(name)

    # 1. family_access + idp_master rejected
    check("family_access session is REJECTED", getSessionUser(con, "sess_family") is None)
    check("idp_master session is REJECTED", getSessionUser(con, "sess_idp") is None)

    # 2. legacy(null) + explicit web accepted
    legacy = getSessionUser(con, "sess_web_legacy")
    web = getSessionUser(con, "sess_web_explicit")
    check("legacy (kind NULL) session is ACCEPTED", legacy is not None)
    check("explicit 'web' session is ACCEPTED", web is not None)

    # 3/4/5. field invariants on the accepted legacy session
    if legacy is not None:
        check("user.id == USER id 42 (footgun fixed)", legacy["id"] == 42)
        check("user.user_id preserved == 42", legacy["user_id"] == 42)
        check("session_id is the real session token", legacy["session_id"] == "sess_web_legacy")
        check("session_kind is NULL for legacy", legacy["session_kind"] is None)
        check("api_key still present on full session", legacy["api_key"] == "KEY42")

    # expired session rejected regardless of kind
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind) VALUES ('sess_expired',42,datetime('now','-1 hour'),'web')")
    con.commit()
    check("expired web session is REJECTED", getSessionUser(con, "sess_expired") is None)

    print()
    if failures:
        print(f"FAILED: {len(failures)} assertion(s): {failures}")
        sys.exit(1)
    print("ALL M1 getSessionUser REGRESSION TESTS PASSED")


if __name__ == "__main__":
    main()

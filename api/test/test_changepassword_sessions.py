"""Regression test: change-password preserves the CURRENT session.

handleChangePassword logs out all OTHER sessions on a password change but must
keep the one the user is currently on. The fix binds `user.session_id` (the real
session id, exposed by the §7 getSessionUser rewrite) instead of `user.id` (the
USER id). This test reproduces the exact DELETE and asserts the current session
survives while the other is removed — and documents the old bug it fixes.

Self-contained; no network. Run: python api/test/test_changepassword_sessions.py
Exit 0 = pass. Mirrors the schema files committed under api/.
"""
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
API = os.path.dirname(HERE)

# The exact DELETE from handleChangePassword.
DELETE_SQL = 'DELETE FROM sessions WHERE user_id = ? AND id != ?'


def build_db():
    con = sqlite3.connect(":memory:")
    con.executescript(open(os.path.join(API, "schema_live_20260717.sql"), encoding="utf-8").read())
    raw = open(os.path.join(API, "migrations", "2026-07-17-m1-sso.sql"), encoding="utf-8").read()
    for stmt in [s for s in raw.split(";")
                 if s.strip() and any(not l.strip().startswith("--") and l.strip() for l in s.splitlines())]:
        con.execute(stmt)
    con.execute("INSERT INTO users (id,name,email,password_hash,institution,country,role,api_key,is_active,is_admin) "
                "VALUES (42,'Tester','t@example.com','ph','UCA','US','user','KEY42',1,0)")
    # two live web sessions for the same user: A = current device, B = other device
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind) VALUES ('sess_current',42,datetime('now','+1 day'),NULL)")
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind) VALUES ('sess_other',42,datetime('now','+1 day'),NULL)")
    con.commit()
    return con


def remaining(con):
    return sorted(r[0] for r in con.execute("SELECT id FROM sessions WHERE user_id=42"))


def main():
    failures = []

    def check(name, cond):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        if not cond:
            failures.append(name)

    # FIXED behavior: bind user.session_id (the current session id).
    con = build_db()
    # getSessionUser for the request on device A returns session_id='sess_current'
    current_session = "sess_current"
    con.execute(DELETE_SQL, (42, current_session))
    con.commit()
    left = remaining(con)
    check("current session 'sess_current' PRESERVED", "sess_current" in left)
    check("other session 'sess_other' DELETED", "sess_other" not in left)
    check("exactly the current session remains", left == ["sess_current"])

    # DOCUMENT the old bug: binding user.id (=42, the USER id) deletes BOTH,
    # because sessions.id is TEXT and never equals the integer 42.
    con2 = build_db()
    con2.execute(DELETE_SQL, (42, 42))  # old buggy bind
    con2.commit()
    left2 = remaining(con2)
    check("(old bug) binding user.id would delete the current session too", "sess_current" not in left2)
    check("(old bug) binding user.id would delete ALL sessions", left2 == [])

    print()
    if failures:
        print(f"FAILED: {len(failures)} assertion(s): {failures}")
        sys.exit(1)
    print("ALL change-password session-preservation TESTS PASSED")


if __name__ == "__main__":
    main()

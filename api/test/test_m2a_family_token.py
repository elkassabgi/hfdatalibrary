"""M2a regression tests (AUTH_SSO_PLAN §7) for validateFamilyToken.

Replicates the exact SQL + the audience/registry checks the worker performs,
against a local SQLite built from the live schema + M1 + M2 migrations, and
asserts the security-critical invariants of the family-token validator:

  1. A valid family_access token (audience == request Origin, origin registered
     and active) is ACCEPTED.
  2. Wrong Origin (audience != Origin) is REJECTED.
  3. A suspended registry origin is REJECTED.
  4. An expired family_access token is REJECTED.
  5. A web/legacy session presented to the family query is REJECTED (kind gate).
  6. The api_key is stripped (never returned to a family token).

Self-contained; no network. Run: python api/test/test_m2a_family_token.py
Exit 0 = all pass.
"""
import hashlib
import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
API = os.path.dirname(HERE)

# EXACT family-access lookup from validateFamilyToken (index.js).
FAMILY_SQL = (
    "SELECT u.*, s.user_id AS user_id, s.id AS session_id, s.kind AS session_kind, "
    "s.audience AS session_audience, s.expires_at AS session_expires_at "
    "FROM sessions s JOIN users u ON s.user_id = u.id "
    "WHERE s.id = ? AND s.kind = 'family_access' AND s.expires_at > datetime('now')"
)


def sha256hex(s):
    return hashlib.sha256(s.encode()).hexdigest()


def split_sql(raw):
    # Strip line comments (our DDL never contains '--' inside a string literal),
    # then split into statements on ';'. Robust to semicolons inside comments.
    clean = "\n".join(line.split("--", 1)[0] for line in raw.splitlines())
    return [s.strip() for s in clean.split(";") if s.strip()]


def build_db():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(open(os.path.join(API, "schema_live_20260717.sql"), encoding="utf-8").read())
    for mig in ("2026-07-17-m1-sso.sql", "2026-07-18-m2-sso.sql"):
        raw = open(os.path.join(API, "migrations", mig), encoding="utf-8").read()
        for stmt in split_sql(raw):
            con.execute(stmt)
    con.execute("INSERT INTO users (id,name,email,password_hash,institution,country,role,api_key,is_active,is_admin) "
                "VALUES (42,'Tester','t@example.com','ph','UCA','US','user','SECRETKEY42',1,0)")
    # registry: econ active, elkassabgidata suspended
    con.execute("INSERT INTO sso_clients (origin,status,created_at) VALUES ('https://econdatalibrary.com','active',1)")
    con.execute("INSERT INTO sso_clients (origin,status,created_at) VALUES ('https://elkassabgidata.com','suspended',1)")
    # a live family_access token for econ, a web session, and an expired family token
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind,audience) VALUES (?,42,datetime('now','+15 minutes'),'family_access','https://econdatalibrary.com')",
                (sha256hex("edl_good"),))
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind,audience) VALUES (?,42,datetime('now','-1 minutes'),'family_access','https://econdatalibrary.com')",
                (sha256hex("edl_expired"),))
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind) VALUES ('websess_raw',42,datetime('now','+1 day'),NULL)")
    con.commit()
    return con


def registry(con):
    return {r["origin"]: r["status"] for r in con.execute("SELECT origin,status FROM sso_clients")}


def validate_family_token(con, raw_bearer, request_origin):
    """Faithful port of validateFamilyToken(request, env)."""
    id_hash = sha256hex(raw_bearer)
    row = con.execute(FAMILY_SQL, (id_hash,)).fetchone()
    if row is None or not row["is_active"]:
        return None
    if not row["session_audience"] or row["session_audience"] != request_origin:
        return None
    reg = registry(con)
    if reg.get(request_origin) != "active":
        return None
    # reduced scope: api_key stripped
    out = dict(row)
    out["api_key"] = None
    out["isFamilyToken"] = True
    return out


def main():
    con = build_db()
    failures = []

    def check(name, cond):
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        if not cond:
            failures.append(name)

    # 1. valid token, correct origin -> accepted
    u = validate_family_token(con, "edl_good", "https://econdatalibrary.com")
    check("valid family token (audience==Origin, active) ACCEPTED", u is not None)
    if u:
        check("user.id resolves to the USER id (42)", u["id"] == 42)
        check("api_key STRIPPED (None) for family token", u["api_key"] is None and "SECRETKEY42" not in str(u.get("api_key")))
        check("isFamilyToken flag set", u.get("isFamilyToken") is True)

    # 2. wrong Origin (audience != Origin) -> rejected
    check("wrong Origin REJECTED (audience mismatch)",
          validate_family_token(con, "edl_good", "https://elkassabgidata.com") is None)

    # 3. suspended origin -> rejected (even if a token were minted for it)
    con.execute("INSERT INTO sessions (id,user_id,expires_at,kind,audience) VALUES (?,42,datetime('now','+15 minutes'),'family_access','https://elkassabgidata.com')",
                (sha256hex("edl_susp"),))
    con.commit()
    check("suspended registry origin REJECTED",
          validate_family_token(con, "edl_susp", "https://elkassabgidata.com") is None)

    # 4. expired token -> rejected
    check("expired family token REJECTED",
          validate_family_token(con, "edl_expired", "https://econdatalibrary.com") is None)

    # 5. a web session's raw id presented to the family query -> no match (kind gate)
    check("web session id via family query REJECTED (kind gate)",
          con.execute(FAMILY_SQL, ("websess_raw",)).fetchone() is None)

    # 6. unregistered origin -> rejected
    check("unregistered Origin REJECTED",
          validate_family_token(con, "edl_good", "https://evil.example.com") is None)

    # migration objects present
    objs = {r[0] for r in con.execute("SELECT name FROM sqlite_master")}
    check("sso_refresh_tokens table created", "sso_refresh_tokens" in objs)
    check("sso_oauth_state table created", "sso_oauth_state" in objs)
    check("idx_sessions_kind index created", "idx_sessions_kind" in objs)

    print()
    if failures:
        print(f"FAILED: {len(failures)} assertion(s): {failures}")
        sys.exit(1)
    print("ALL M2a family-token REGRESSION TESTS PASSED")


if __name__ == "__main__":
    main()

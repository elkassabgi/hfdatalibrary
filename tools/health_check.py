"""End-to-end health check for hfdatalibrary: is the API up AND is the data actually fresh?

WHY BOTH HALVES. A green "Daily Data Update" workflow says a job exited 0. It does not say the
served data moved, and this project has been caught by that distinction before - a fetcher can
run, succeed and publish nothing. So this asks the LIVE API for the newest object timestamp and
judges it against the schedule, rather than trusting the run's exit code.

THE SCHEDULE IS TUESDAY-SATURDAY, deliberately: `cron: '0 6 * * 2-6'` in
.github/workflows/daily-update.yml, because IEX publishes the previous day's pcap. A Sunday or
Monday with no run is CORRECT, and a checker that does not know this cries wolf twice a week -
which is how a standing check stops being read.

ROUTES ARE READ FROM THE WORKER, NOT GUESSED. Every path here is prefixed `/v1` because
`api/src/index.js` matches `path === '/v1/public-stats'`. An earlier version of this check
invented `/api/stats` and reported three 404s as an outage; a negative result from an endpoint
you have not confirmed exists is not a finding (ledger R682).

Read-only GETs against the public API. No credentials, no writes.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import urllib.error
import urllib.request

BASE = "https://api.hfdatalibrary.com"
UA = {"User-Agent": "hfdatalibrary-health/1.0"}
RUN_DAYS = {1, 2, 3, 4, 5}          # Tue..Sat, Monday=0 in weekday()


def get(path: str):
    try:
        r = urllib.request.urlopen(urllib.request.Request(BASE + path, headers=UA), timeout=90)
        return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read(300)
    except Exception as e:                                            # noqa: BLE001
        return type(e).__name__, b""


def expected_staleness_days(now: dt.datetime) -> int:
    """How many days may pass without a run before that is abnormal.

    Tue-Sat means the longest legitimate gap is Saturday's run to Tuesday's: three days. Add a
    day of slack for GitHub's cron drift, which is real - observed start times range from 06:00
    to 17:56 UTC on this workflow.
    """
    return 4


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-stale-days", type=int, default=None)
    a = ap.parse_args()

    now = dt.datetime.now(dt.timezone.utc)
    limit = a.max_stale_days or expected_staleness_days(now)
    bad = 0

    code, body = get("/v1/status")
    if code == 200:
        d = json.loads(body)
        r2 = d.get("r2_connected")
        print(f"  OK  status      {d.get('status')} | api {d.get('api_version')} | "
              f"r2_connected={r2} | users {d.get('registered_users'):,}")
        if r2 is not True:
            bad += 1
            print("  BAD r2_connected is not true - downloads cannot be served")
    else:
        bad += 1
        print(f"  BAD status      HTTP {code}")

    code, body = get("/v1/public-stats")
    if code == 200:
        d = json.loads(body)
        print(f"  OK  downloads   {d.get('total_downloads'):,} total | "
              f"{d.get('downloads_today'):,} today | {d.get('downloads_this_week'):,} this week")
        print(f"      served      {d.get('total_bytes_served', 0) / 1e12:.2f} TB")
    else:
        bad += 1
        print(f"  BAD public-stats HTTP {code}")

    # THE HALF THAT ACTUALLY ANSWERS "IS THE DATA FRESH": the newest object's own timestamp.
    code, body = get("/v1/symbols?limit=50")
    if code == 200:
        d = json.loads(body)
        syms = d.get("symbols") or []
        stamps = []
        for s in syms:
            lm = s.get("last_modified")
            if lm:
                try:
                    stamps.append(dt.datetime.fromisoformat(lm.replace("Z", "+00:00")))
                except ValueError:
                    pass
        if stamps:
            newest = max(stamps)
            age = (now - newest).days
            ok = age <= limit
            bad += 0 if ok else 1
            print(f"  {'OK ' if ok else 'BAD'} freshness   {d.get('count'):,} symbols | "
                  f"newest object {newest.strftime('%Y-%m-%d %H:%M')}Z, {age}d old "
                  f"(limit {limit}d)")
            if not ok:
                print("      The workflow runs Tue-Sat, so the longest legitimate gap is 3 days.")
        else:
            bad += 1
            print("  BAD freshness   no last_modified on any symbol")
    else:
        bad += 1
        print(f"  BAD symbols     HTTP {code}")

    print()
    print(f"{bad} problem(s)" if bad else "hfdatalibrary is operational and the data is current")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())

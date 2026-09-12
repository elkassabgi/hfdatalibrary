"""R741 finding 1 / R743 / R744: for every rebased ticker in the pass-1 log, compare the SERVED variables and
quality objects (raw and clean) with the snapshot's copies: which sessions differ, pre-seam vs post-seam, on
which columns, by how much, and on WHICH DATES (R744: the disclosure's "from 2026-03-30" count was derived from
post_seam_first, which is the seam-day row on 45 of 47 tickers and can never show where a 2026 cluster starts;
the dates are stored now). The seam tool re-syncs variables with force_full=True, so a difference on a POST-seam
session means the served variables had been stale against the served bars before the rebase. Read-only. Writes
D:/temp/claude/_verify_variables_delta.json (merged by ticker) and prints one line per ticker.

Per object: rows, common, only_served/only_snapshot, pre/post sessions differing, post_seam_first/last (kept),
post_dates (every post-seam differing date, ISO), post_first_2026 (first differing date >= 2026-01-01, or None),
columns_differing / columns_pre / columns_post (session counts), and magnitude_pre per column: sessions differing,
median and max |relative difference| over finite pairs (denominator max(|snapshot|, 1e-12)), count beyond 1e-3.
"""
import datetime as dt, json, os, sys
sys.path.insert(0, "D:/temp/claude/hf_wt_main/pipeline")
import numpy as np
import pandas as pd
from r2_client import get_client, download_parquet

LOG = "D:/temp/claude/seam_rebase_batch_pass1.log"
SNAP = "F:/hf_r2_snapshot_seam_20260905"
OUT = "D:/temp/claude/_verify_variables_delta.json"
SEAM = pd.Timestamp("2022-03-07")
Y2026 = pd.Timestamp("2026-01-01")


def load_snap(t, key):
    p = os.path.join(SNAP, t, key.replace("/", "__"))
    return pd.read_parquet(p) if os.path.exists(p) else None


def served(client, version, t, kind):
    """kind = 'variables' | 'quality' -> the served object under <version>/<kind>/<T>.parquet"""
    try:
        import io
        from r2_client import download_to_buffer
        data = download_to_buffer(client, f"{version}/{kind}/{t}.parquet")
        return pd.read_parquet(io.BytesIO(data)) if data else None
    except Exception as ex:                                  # noqa: BLE001
        print(f"    ({version}/{kind}/{t}: {type(ex).__name__}: {str(ex)[:80]})")
        return None


def diff(a, b):
    """sessions and columns that differ; both frames indexed by a date-like column. a = served, b = snapshot."""
    dcol = "trade_date" if "trade_date" in a.columns else ("date" if "date" in a.columns else ("datetime" if "datetime" in a.columns else a.columns[0]))
    a = a.copy(); b = b.copy()
    a[dcol] = pd.to_datetime(a[dcol]); b[dcol] = pd.to_datetime(b[dcol])
    a = a.set_index(dcol).sort_index(); b = b.set_index(dcol).sort_index()
    common = a.index.intersection(b.index)
    cols = [c for c in a.columns if c in b.columns]
    pre_d, post_d, colset, cols_pre, cols_post, mag_pre = set(), set(), {}, {}, {}, {}
    is_pre = np.array([d < SEAM for d in common])
    for c in cols:
        x = a.loc[common, c]; y = b.loc[common, c]
        if np.issubdtype(x.dtype, np.number) and np.issubdtype(y.dtype, np.number):
            xv = x.astype(float).to_numpy(); yv = y.astype(float).to_numpy()
            ne = ~np.isclose(xv, yv, rtol=0, atol=1e-9, equal_nan=True)
            if (ne & is_pre).any():
                sel = ne & is_pre & np.isfinite(xv) & np.isfinite(yv)
                rel = np.abs(xv[sel] - yv[sel]) / np.maximum(np.abs(yv[sel]), 1e-12)
                absd = np.abs(xv[sel] - yv[sel])
                mag_pre[c] = {"sessions": int((ne & is_pre).sum()), "median_rel": float(np.median(rel)) if rel.size else None,
                              "max_rel": float(rel.max()) if rel.size else None, "beyond_1e-3": int((rel > 1e-3).sum()),
                              # the ABSOLUTE movement too: a ratio column with a near-zero denominator gives a
                              # huge relative figure for a 1e-9 change, and only the absolute value says which
                              "max_abs": float(absd.max()) if absd.size else None,
                              "median_abs": float(np.median(absd)) if absd.size else None,
                              "nan_mismatch": int((ne & is_pre & ~(np.isfinite(xv) & np.isfinite(yv))).sum())}
        else:
            ne = (x.astype(str).to_numpy() != y.astype(str).to_numpy())
        if ne.any():
            days = common[ne]
            colset[c] = int(ne.sum())
            # pre- and post-seam SEPARATELY (R743): the union hid which columns changed where
            if (ne & is_pre).any():
                cols_pre[c] = int((ne & is_pre).sum())
            if (ne & ~is_pre).any():
                cols_post[c] = int((ne & ~is_pre).sum())
            pre_d.update(d for d in days if d < SEAM); post_d.update(d for d in days if d >= SEAM)
    post_sorted = sorted(post_d)
    post_2026 = [d for d in post_sorted if d >= Y2026]
    # R748: only_served / only_snapshot were counted but never named, so 151 removed sessions reached no
    # sentence. The DATES are stored now: only_snapshot = sessions the rebase REMOVED, only_served = added.
    gained = sorted(a.index.difference(b.index))
    lost = sorted(b.index.difference(a.index))
    return {"rows_served": len(a), "rows_snapshot": len(b), "common": len(common), "only_served": int(len(gained)),
            "dates_gained": [d.date().isoformat() for d in gained], "dates_lost": [d.date().isoformat() for d in lost],
            "only_snapshot": int(len(lost)), "pre_seam_sessions_differing": len(pre_d),
            "post_seam_sessions_differing": len(post_d), "post_seam_first": post_sorted[0].date().isoformat() if post_sorted else None,
            "post_seam_last": post_sorted[-1].date().isoformat() if post_sorted else None,
            "post_dates": [d.date().isoformat() for d in post_sorted],
            "post_first_2026": post_2026[0].date().isoformat() if post_2026 else None,
            "post_2026_sessions": len(post_2026),
            "columns_differing": colset, "columns_pre": cols_pre, "columns_post": cols_post, "magnitude_pre": mag_pre}


def main():
    args = sys.argv[1:]
    only = None
    if args and args[0] == "--only":
        only = [t.strip().upper() for t in args[1].split(",")]; args = args[2:]
    since = args[0] if args else None
    tickers = []
    for line in open(LOG, encoding="utf-8"):
        p = line.rstrip("\n").split("\t")
        if len(p) >= 3 and p[2] == "0" and ("DONE" in line or "release:" in line) and (since is None or p[0] >= since):
            if p[1] not in tickers:
                tickers.append(p[1])
    if only:
        tickers = only
    client = get_client()
    prev = {}
    if os.path.exists(OUT):
        try:
            prev = {r["ticker"]: r for r in json.load(open(OUT, encoding="utf-8"))}
        except Exception:                                    # noqa: BLE001
            prev = {}
    print(f"{dt.datetime.now(dt.timezone.utc):%H:%M:%SZ} {len(tickers)} ticker(s)", flush=True)
    for t in tickers:
        row = {"ticker": t, "measured_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "instrument": "verify_variables_delta.py v2 (dates+magnitudes)"}
        summary = []
        for version in ("raw", "clean"):
            for kind in ("variables", "quality"):
                s = served(client, version, t, kind)
                b = load_snap(t, f"{version}/{kind}/{t}.parquet")
                key = f"{version}_{kind}"
                if s is None or b is None:
                    row[key] = "missing served" if s is None else "missing snapshot"; summary.append(f"{key}:{row[key]}"); continue
                d = diff(s, b)
                row[key] = d
                summary.append(f"{key}: post {d['post_seam_sessions_differing']} (2026 from {d['post_first_2026']}) pre {d['pre_seam_sessions_differing']}")
        prev[t] = row
        print(f"  {t:6} " + " | ".join(summary), flush=True)
        json.dump(list(prev.values()), open(OUT, "w", encoding="utf-8"), indent=1, default=str)   # after every ticker: a kill loses nothing
    print(f"{dt.datetime.now(dt.timezone.utc):%H:%M:%SZ} done")
    return 0


if __name__ == "__main__":
    sys.exit(main())

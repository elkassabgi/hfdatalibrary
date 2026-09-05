"""seam_rebase.py — put ONE ticker's pre-2022-03-07 (PiTrading) history on the basis of its
post-2022-03-07 (IEX) history. Version 3, after two adversarial reviews (2026-09-05).

WHY. The pre-2022 half is split+dividend+spin-off adjusted as of 2022-03-04 (the vendor's file
end). The later half was conformed to the previously served series through 2026-03-27 by the merge
step, and is appended raw since. Nothing back-propagated later corporate actions into the earlier
half, so tickers with a split or dividend since March 2022 carry a step at the seam (AMZN 20x).

TWO MODES. --mode split (DEFAULT) fixes the split part only, which is correct under either price
convention. --mode full also folds in the dividend/spin factor D and refuses without
--convention-decided, because that is Ahmed's decision, not the tool's.

WHAT THE SECOND REVIEW FOUND, AND WHAT V3 DOES ABOUT IT
  * D-BLINDNESS. Some tickers carry a split UNAPPLIED inside the served later half (BKNG 25:1 on
    2026-04-06, BYND 1:30 on 2026-08-14 — after the daily fix). There P looks like a missing split
    but the seam is already consistent; v2 would have manufactured a 25x step, and both
    verifications would have passed. v3 walks every Yahoo split event after the seam that falls
    inside the served range and measures the served close step across it against Yahoo's; a step
    within 2 % of the raw split factor means the split is unapplied in the later half -> REFUSE and
    name it (that is a separate live defect to repair with manual_split + CA_DATE).
  * IDEMPOTENCY. v3 measures the 1-minute RAW and CLEAN files' own last pre-seam session against
    the market before touching them; if either already sits on target, or they disagree with each
    other by more than 0.3 %, it refuses. A crash between uploads therefore cannot be doubled by a
    re-run. --restore SNAPDIR re-uploads every snapshotted object verbatim.
  * SNAPSHOT. Includes csv/raw/T.csv and csv/clean/T.csv (served by /v1/download?format=csv) and
    everything under raw/, clean/, variables/, quality/; each object's MD5 is compared to its ETag
    when the ETag is a single-part MD5, else Content-Length is compared.
  * EX-DATES. If a Yahoo ex-dividend date falls inside the pre-seam window, the window is the
    sessions strictly after the last such ex-date; fewer than 2 sessions -> REFUSE naming the date.
  * The clean set is not rescale-invariant (measured: 12,479 MO bars flip at K=0.714), so raw and
    the served clean are rescaled in place and re-aggregated; nothing is re-cleaned.

ORDER. measure -> refuse or print plan -> [--apply] snapshot (content-checked) -> rescale raw+clean
-> upload 1min parquet+csv x2 -> aggregate_all x2 -> upload every timeframe -> sync variables x2
-> VERIFY: (a) served daily seam step == old/K; (b) rebased P' over the same window == 1.000
(split) or D (full) within 0.3 %; (c) served clean 1-minute last pre-seam session agrees with raw.

Run from inside pipeline/ of a MAIN-based tree (sibling imports; r2_client stamps parquet metadata).
    python seam_rebase.py TICKER [--mode split|full] [--apply] [--snapshot-dir DIR]
    python seam_rebase.py TICKER --restore SNAPDIR
"""
from __future__ import annotations
import argparse
import hashlib
import math
import os
import sys
import pandas as pd

from aggregate import aggregate_all
from r2_client import download_parquet, get_client, upload_csv, upload_parquet
from variables_sync import sync_ticker_variables

TIMEFRAMES = ["5min", "15min", "30min", "hourly", "daily", "weekly", "monthly"]
SEAM = pd.Timestamp("2022-03-07")
FIX = pd.Timestamp("2026-07-13")
PRICE_COLS = ("Open", "High", "Low", "Close")
BUCKET = "hfdatalibrary-data"
INTS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30, 40, 50, 60, 100, 200, 250, 500, 1000, 2000]
CANON_INT = sorted(set([1.0] + [float(n) for n in INTS] + [1.0 / n for n in INTS]))
WIN = 3
SNAP_PREFIXES = ("raw/", "clean/", "variables/", "quality/", "csv/")


def snap_int(p: float):
    best = min(CANON_INT, key=lambda r: abs(math.log(p / r)))
    return best if abs(p / best - 1) <= 0.002 else None


def yahoo(t: str):
    """Daily Close (today's split basis, no dividends) 2022-02-14..today, split events, ex-dates."""
    import yfinance as yf
    tk = yf.Ticker(t.replace(".", "-"))
    h = tk.history(start="2022-02-14", auto_adjust=False, actions=True)
    if h is None or len(h) == 0:
        return None, None, None
    h.index = pd.to_datetime(h.index).tz_localize(None).normalize()
    spl = h["Stock Splits"][h["Stock Splits"] > 0] if "Stock Splits" in h else pd.Series(dtype=float)
    spl = spl[spl.index > pd.Timestamp("2022-03-04")]
    div = h["Dividends"][h["Dividends"] > 0] if "Dividends" in h else pd.Series(dtype=float)
    return h, spl, div


def pre_window(daily_idx, div) -> tuple[list, str | None]:
    """The pre-seam sessions used for P: the last WIN sessions, cut to those strictly after the last
    ex-dividend date that falls inside them. Returns (sessions, ex_date_note)."""
    pre = [d for d in daily_idx if d < SEAM][-WIN:]
    if div is None or len(div) == 0 or not pre:
        return pre, None
    ex_in = [d for d in div.index if pre[0] <= d <= pre[-1]]
    if not ex_in:
        return pre, None
    last_ex = max(ex_in)
    return [d for d in pre if d > last_ex], f"ex-dividend {last_ex.date()} inside the window"


def ratio_over(served: pd.Series, y: pd.Series, sessions) -> tuple[float | None, float | None, int]:
    s = served.reindex(sessions).dropna(); yy = y.reindex(s.index).dropna(); s = s.reindex(yy.index)
    if len(s) < 2:
        return None, None, len(s)
    r = (s / yy).astype(float)
    return float(r.median()), float(r.max() / r.min() - 1), len(s)


def event_steps(served_daily: pd.Series, y: pd.Series, spl) -> tuple[list, list, list]:
    """For each Yahoo split event after the seam: served close step across the event divided by
    Yahoo's step. Yahoo is fully adjusted (no step), so rel ~ 1.0 means the served series is adjusted
    across the event; rel within 5 % of the raw factor 1/s means the split was NEVER applied to the
    served series. Returns (unapplied, applied, unmeasurable) — an event whose sessions cannot be
    placed is reported, never skipped (silent skips are how a wrong basis gets through).
    5 %, not 2 %: BYND's served $0.415 close sits 2 % off the official close by tick size alone, and a
    genuine split factor is at least 2x away from 1, so 5 % cannot confuse a market move with a split."""
    una, app, unm = [], [], []
    idx = served_daily.index
    for ev, s in spl.items():
        if ev > idx.max():
            continue                                   # event after the served range: nothing to test yet
        raw_factor = 1.0 / float(s)
        # Yahoo misdates some ETF splits by a session or more, and a raw step one session away from the
        # recorded date would read "applied" at the recorded date. Test every session boundary within
        # +-3 served sessions of the event and take the boundary whose served/Yahoo step is furthest from 1.
        pos = idx.searchsorted(ev)
        cands = []
        for k in range(max(1, pos - 3), min(len(idx) - 1, pos + 3) + 1):
            p_d, q_d = idx[k - 1], idx[k]
            if p_d not in y.index or q_d not in y.index:
                continue
            rel = float(served_daily.iloc[k] / served_daily.iloc[k - 1]) / float(y[q_d] / y[p_d])
            cands.append((abs(math.log(rel)), rel, q_d.date()))
        if not cands:
            unm.append((ev.date(), float(s), "no served/market session pair within +-3 sessions")); continue
        _, rel, at = max(cands)
        if abs(rel / raw_factor - 1) <= 0.05:
            una.append((ev.date(), float(s), rel) if at == ev.date() else (ev.date(), float(s), rel, f"step seen at {at}"))
        elif abs(rel - 1) <= 0.05:
            app.append((ev.date(), float(s), rel))
        else:
            unm.append((ev.date(), float(s), f"largest nearby step rel {rel:.3f} at {at} is neither ~1 nor ~{raw_factor:.4g}"))
    return una, app, unm


def unapplied_splits(served_daily: pd.Series, y: pd.Series, spl) -> list[tuple]:
    return event_steps(served_daily, y, spl)[0]


def daily_run_state() -> str:
    """'in_progress' / 'queued' / 'idle' / 'unknown' for the Daily Data Update workflow. The daily
    run downloads every served object, appends and re-uploads from in-memory frames; a rebase that
    lands inside that window is overwritten with old-basis data."""
    import json, subprocess
    try:
        r = subprocess.run(["gh", "run", "list", "--workflow", "Daily Data Update", "--limit", "3", "--json", "status"],
                           capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=60)
        st = [x.get("status") for x in json.loads(r.stdout or "[]")]
        if "in_progress" in st: return "in_progress"
        if "queued" in st or "waiting" in st or "requested" in st or "pending" in st: return "queued"
        return "idle"
    except Exception:
        return "unknown"


def md5_of(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def snapshot(client, ticker: str, out_dir: str) -> int:
    os.makedirs(out_dir, exist_ok=True)
    names = {f"{ticker}.parquet", f"{ticker}.csv", f"{ticker}.csv.gz", f"{ticker}.json"}
    keys = []
    for pref in SNAP_PREFIXES:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=pref):
            for o in page.get("Contents", []):
                if o["Key"].rsplit("/", 1)[-1] in names:
                    keys.append((o["Key"], o["Size"], o["ETag"].strip('"')))
    if not keys:
        raise SystemExit(f"snapshot: no served objects found for {ticker}")
    for k, size, etag in keys:
        dest = os.path.join(out_dir, k.replace("/", "__"))
        client.download_file(BUCKET, k, dest)
        if os.path.getsize(dest) != size:
            raise SystemExit(f"snapshot: {k} size {size} on R2 vs {os.path.getsize(dest)} on disk - aborting before any write")
        if "-" not in etag and md5_of(dest) != etag:
            raise SystemExit(f"snapshot: {k} MD5 {md5_of(dest)} != ETag {etag} - aborting before any write")
    with open(os.path.join(out_dir, "_MANIFEST.txt"), "w", encoding="utf-8") as f:
        for k, size, etag in keys:
            f.write(f"{size}\t{etag}\t{k}\n")
    return len(keys)


def restore(client, snap_dir: str) -> int:
    n = 0
    for line in open(os.path.join(snap_dir, "_MANIFEST.txt"), encoding="utf-8"):
        size, etag, k = line.rstrip("\n").split("\t")
        src = os.path.join(snap_dir, k.replace("/", "__"))
        if os.path.getsize(src) != int(size):
            raise SystemExit(f"restore: {src} is {os.path.getsize(src)} bytes, manifest says {size} - not restoring")
        client.upload_file(src, BUCKET, k); n += 1
        print(f"  restored {k}")
    return n


def rescale(df: pd.DataFrame, K: float, V: float) -> pd.DataFrame:
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    m = df["datetime"] < SEAM
    for c in PRICE_COLS:
        df.loc[m, c] = (df.loc[m, c] * K).round(6)
    if abs(V - 1) > 1e-9:
        vol = (df.loc[m, "Volume"] * V).round()
        vol[(df.loc[m, "Volume"] > 0) & (vol == 0)] = 1
        df.loc[m, "Volume"] = vol
    df["Volume"] = df["Volume"].astype("int64")
    return df


def last_pre_session_close(df1: pd.DataFrame) -> tuple[pd.Timestamp | None, float | None]:
    """Close of the LAST BAR of the last pre-seam session in a 1-minute frame — the comparator for
    the market's official close (a session median sits ~0.4 % away on a trending day, which would
    fail a correct rebase at the 0.3 % gate)."""
    d = pd.to_datetime(df1["datetime"])
    if d.dt.tz is not None:
        d = d.dt.tz_localize(None)
    m = d < SEAM
    if not m.any():
        return None, None
    sub = df1.loc[m].assign(_dt=d[m]).sort_values("_dt")
    last_bar = sub.iloc[-1]
    return pd.Timestamp(last_bar["_dt"]).normalize(), float(last_bar["Close"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker"); ap.add_argument("--mode", choices=("split", "full"), default="split")
    ap.add_argument("--apply", action="store_true"); ap.add_argument("--snapshot-dir", default=None)
    ap.add_argument("--convention-decided", action="store_true"); ap.add_argument("--restore", default=None)
    ap.add_argument("--allow-queued", action="store_true", help="proceed while a Daily Data Update run is queued but cannot start for >30 min")
    a = ap.parse_args(); t = a.ticker.upper()
    client = get_client()
    if a.restore:
        n = restore(client, a.restore); print(f"{t}: restored {n} objects from {a.restore}"); return 0
    if a.mode == "full" and not a.convention_decided:
        raise SystemExit("--mode full folds dividends into the pre-2022 half; that is the convention decision. Pass --convention-decided only once it is recorded.")

    daily = download_parquet(client, "raw", t, "daily")
    if daily is None or daily.empty:
        raise SystemExit(f"{t}: no served daily file")
    daily["datetime"] = pd.to_datetime(daily["datetime"]); dd = daily.set_index("datetime").sort_index()
    if "source" in dd.columns and not ((dd["source"] == "pitrading").any() and (dd["source"] == "iex").any()):
        print(f"{t}: no PiTrading/IEX splice in the served file - nothing to rebase"); return 0
    h, spl, div = yahoo(t)
    if h is None:
        print(f"{t}: no market reference at Yahoo - cannot measure; disclose, do not repair"); return 3
    y = h["Close"]

    pre_sessions, ex_note = pre_window(dd.index, div)
    post_sessions = [d for d in dd.index if d >= SEAM][:WIN]
    P, sP, nP = ratio_over(dd["Close"], y, pre_sessions)
    D, sD, nD = ratio_over(dd["Close"], y, post_sessions)
    if P is None or D is None:
        print(f"{t}: REFUSED - fewer than 2 market-overlapping sessions on a side (pre {nP}, post {nD}){' - ' + ex_note if ex_note else ''}"); return 2
    print(f"{t}: P={P:.6f} (spread {sP:.4%}, {nP} sessions {[d.date() for d in pre_sessions if d in dd.index]}{'; ' + ex_note if ex_note else ''})  D={D:.6f} (spread {sD:.4%})")
    events = [(str(i.date()), float(v)) for i, v in spl.items()]
    prod_all = float(spl.prod()) if len(spl) else 1.0
    prod_prefix = float(spl[spl.index <= FIX].prod()) if len(spl) else 1.0
    print(f"  Yahoo split events after 2022-03-04: {events or 'none'} -> product all={prod_all:g}, <=2026-07-13={prod_prefix:g}")

    # D-blindness fix, two independent tests:
    #  (i) both halves equally off the market: P ~ D != 1 with a split on record means the WHOLE served
    #      series sits on one (old) basis - the seam is consistent and the ticker's defect is an
    #      unapplied split, not the seam. Simplest and event-arithmetic-free.
    if len(spl) and abs(P / D - 1) <= 0.005 and abs(P - 1) > 0.002:
        print(f"  REFUSED: P={P:.6f} and D={D:.6f} are equal - both halves are on the same pre-split basis, the seam is "
              f"already consistent; the split(s) {events} are UNAPPLIED in the served series. LIVE DEFECT: repair with "
              f"manual_split + CA_DATE, not with this tool."); return 2
    #  (ii) per event: served close step across the event vs Yahoo's; unmeasurable events REFUSE too
    una, app, unm = event_steps(dd["Close"], y, spl)
    if una:
        print(f"  REFUSED: the served post-seam half carries UNAPPLIED split(s) {una} (served step across the event ~ raw factor). "
              f"The seam is not what P says; repair those splits first (manual_split with CA_DATE). LIVE DEFECT."); return 2
    if unm:
        print(f"  REFUSED: split event(s) whose served step cannot be classified {unm} - not proceeding on an unread event"); return 2

    P_int = snap_int(P)
    if P_int is None:
        print(f"  REFUSED: P={P:.6f} does not snap to an integer split ratio within 0.2 % (spin-off or unclear)"); return 2
    match = "all" if abs(P_int / prod_all - 1) <= 0.002 else ("prefix" if abs(P_int / prod_prefix - 1) <= 0.002 else None)
    if match is None:
        print(f"  REFUSED: P_int={P_int:g} matches neither Yahoo's split product ({prod_all:g}) nor the <=2026-07-13 product ({prod_prefix:g})"); return 2
    if match == "prefix":
        # "prefix" is accepted only on POSITIVE evidence: every post-FIX event must be measured as APPLIED
        # in the served series (rel ~ 1). Assuming it (R719) is how MNST/SOXS/TECS were misread.
        post_fix = [(d, s) for d, s in ((i.date(), float(v)) for i, v in spl.items()) if pd.Timestamp(d) > FIX]
        applied_dates = {d for d, s, rel in app}
        missing = [e for e in post_fix if e[0] not in applied_dates]
        if missing:
            print(f"  REFUSED: P matches the pre-2026-07-13 split product only if the later event(s) {missing} were applied to the "
                  f"full history, and the served data does not show them as applied (measured applied: {app})"); return 2
        print(f"  post-FIX event(s) measured APPLIED in the served series: {app}")
    if sP > 0.003:
        print(f"  REFUSED: PiTrading side unstable over its window ({sP:.3%}){' - ' + ex_note if ex_note else ''}"); return 2
    K = 1.0 / P_int; V = P_int
    if a.mode == "full":
        K = D / P_int
    target = 1.0 if a.mode == "split" else D

    # 1-minute self-check FIRST (before any "nothing to do" exit): the files' own last pre-seam BAR vs
    # the market, raw AND clean. An interrupted earlier run leaves raw and clean on different bases,
    # and that must be seen even when the plan itself would be a no-op.
    raw = download_parquet(client, "raw", t); clean = download_parquet(client, "clean", t)
    if raw is None or raw.empty or clean is None or clean.empty:
        raise SystemExit(f"{t}: served raw/clean 1-minute file missing")
    extra = [c for c in raw.columns if c not in PRICE_COLS + ("Volume", "datetime", "source")]
    if extra:
        raise SystemExit(f"{t}: raw carries unexpected columns {extra} - refusing")
    d_r, c_r = last_pre_session_close(raw); d_c, c_c = last_pre_session_close(clean)
    if d_r is None or d_c is None or d_r not in y.index or d_c not in y.index:
        print(f"  REFUSED: cannot place the 1-minute files' last pre-seam session against the market (raw {d_r}, clean {d_c})"); return 2
    P_raw, P_clean = c_r / float(y[d_r]), c_c / float(y[d_c])
    if not (math.isfinite(P_raw) and math.isfinite(P_clean)):
        print(f"  REFUSED: non-finite self-check (P_raw={P_raw}, P_clean={P_clean})"); return 2
    print(f"  1-minute self-check: raw last pre-seam bar {d_r.date()} P_raw={P_raw:.6f}; clean {d_c.date()} P_clean={P_clean:.6f}; target after rebase {P_raw * K:.6f}")
    if abs(P_raw / P_clean - 1) > 0.003:
        print(f"  REFUSED: raw and clean disagree on the pre-seam basis (P_raw={P_raw:.6f}, P_clean={P_clean:.6f}) - a previous run was interrupted; use --restore"); return 2
    if abs(K - 1) <= 0.002 and abs(V - 1) <= 1e-9:
        print(f"  nothing to rebase in --mode {a.mode} (P_int=1{'; dividend/spin seam D=%.4f held for the convention decision' % D if abs(D - 1) > 0.002 else ''}); raw and clean agree"); return 0
    if abs(P_raw / target - 1) <= 0.003:
        print(f"  ALREADY on target (P_raw={P_raw:.6f} ~ {target:.6f}); nothing to do"); return 0
    print(f"  {a.mode.upper()} rebase plan: price x{K:.6g}, volume x{V:g} on bars strictly before {SEAM.date()} (split match: {match}; D={D:.4f}{' NOT applied' if a.mode == 'split' else ' applied'})")
    n_pre = int((pd.to_datetime(raw["datetime"]) < SEAM).sum())
    print(f"  raw {len(raw):,} bars ({n_pre:,} before the seam), clean {len(clean):,} bars; both rescaled in place, neither re-cleaned")
    if not a.apply:
        print("(dry run - pass --apply to rebase)"); return 0

    # never inside a Daily Data Update run: it re-uploads every object from old-basis frames
    st = daily_run_state()
    if st == "in_progress" or (st == "queued" and not a.allow_queued) or st == "unknown":
        print(f"  REFUSED: Daily Data Update workflow is {st}; a rebase inside its window is overwritten. "
              f"Re-run when idle{' (or --allow-queued if the queued run cannot start for >30 min)' if st == 'queued' else ''}."); return 2

    snap_dir = a.snapshot_dir or os.path.join("F:\\", f"hf_r2_snapshot_seam_{pd.Timestamp.today():%Y%m%d}", t)
    n_snap = snapshot(client, t, snap_dir)
    print(f"  snapshot: {n_snap} objects -> {snap_dir} (size + MD5/ETag verified)")

    raw2, clean2 = rescale(raw, K, V), rescale(clean, K, V)
    n = 0; sync_failed = []
    for version, df in (("raw", raw2), ("clean", clean2)):
        upload_parquet(client, df, version, t, "1min"); upload_csv(client, df, version, t, "1min"); n += 2
        aggs = aggregate_all(df)
        for tf in TIMEFRAMES:
            if tf in aggs and not aggs[tf].empty:
                upload_parquet(client, aggs[tf], version, t, tf); n += 1
        # variables/quality: isolated like the daily pipeline does it, so a failure here cannot skip VERIFY
        for attempt in (1, 2):
            try:
                sync_ticker_variables(client, version, t, df, force_full=True); n += 2; break
            except Exception as ex:
                if attempt == 2:
                    sync_failed.append(f"{version}: {str(ex)[:120]}")
    print(f"  uploaded {n} objects" + (f"; variables sync FAILED for {sync_failed}" if sync_failed else ""))

    # verify from the served side
    d2 = download_parquet(client, "raw", t, "daily"); d2["datetime"] = pd.to_datetime(d2["datetime"])
    dd2 = d2.set_index("datetime").sort_index()
    step_before = float(dd[dd.index >= SEAM]["Close"].iloc[0] / dd[dd.index < SEAM]["Close"].iloc[-1])
    step_after = float(dd2[dd2.index >= SEAM]["Close"].iloc[0] / dd2[dd2.index < SEAM]["Close"].iloc[-1])
    ok_a = abs(step_after / (step_before / K) - 1) < 0.005
    P2, _, _ = ratio_over(dd2["Close"], y, pre_sessions)
    ok_b = P2 is not None and abs(P2 / target - 1) < 0.003
    # (c) internal consistency of the served 1-minute files: clean's last pre-seam bar == raw's, and
    #     raw's == the pre-computed P_raw*K (a 15:59 print is not the closing auction, so no market gate here)
    raw_srv = download_parquet(client, "raw", t); clean_srv = download_parquet(client, "clean", t)
    d_r2, c_r2 = last_pre_session_close(raw_srv); d_c2, c_c2 = last_pre_session_close(clean_srv)
    ok_c = (d_r2 is not None and d_c2 is not None and abs(c_c2 / c_r2 - 1) < 0.0005
            and abs((c_r2 / float(y[d_r2])) / (P_raw * K) - 1) < 0.0005)
    print(f"  VERIFY (a) served daily seam step x{step_after:.6f} vs expected x{step_before / K:.6f} -> {'OK' if ok_a else 'MISMATCH'}")
    print(f"  VERIFY (b) rebased P'={P2} vs target {target:.6f} -> {'OK' if ok_b else 'MISMATCH'}")
    print(f"  VERIFY (c) served 1-minute: clean last pre-seam bar {c_c2} vs raw {c_r2}; raw/market {c_r2 / float(y[d_r2]) if d_r2 is not None else None:.6f} vs P_raw*K {P_raw * K:.6f} -> {'OK' if ok_c else 'MISMATCH'}")
    if not (ok_a and ok_b and ok_c):
        print(f"  NOT VERIFIED - run: python seam_rebase.py {t} --restore \"{snap_dir}\""); return 1
    if sync_failed:
        print(f"  PRICES VERIFIED but variables/quality sync failed: {sync_failed}. Not restoring; re-run sync_ticker_variables for {t} before serving is complete."); return 1
    print(f"  DONE: {t} rebased ({a.mode}); snapshot kept at {snap_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

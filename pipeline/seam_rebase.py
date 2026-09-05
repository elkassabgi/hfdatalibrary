"""seam_rebase.py — put ONE ticker's pre-2022-03-07 (PiTrading) history on the basis of its
post-2022-03-07 (IEX) history. Version 5, after five adversarial reviews (R719/R720, R725, R728, R731; 2026-09-05).

EXIT CODES - one meaning each (R731: exit 1 used to carry four meanings and the batch driver
narrated one of them for all four):
    0  rebased and verified, or nothing to rebase / already on target
    1  written, then RESTORED from the snapshot (a failure or a VERIFY mismatch); served = pre-rebase
    2  refused before any write (measurement says no)
    3  unmeasurable (no market reference)
    4  written, and the automatic RESTORE FAILED - served state UNKNOWN; run --restore now. Also:
       a --restore that fails, and an INCONSISTENT served set (1-minute files already on target
       while the daily still carries the split - a partial earlier write); nothing touched then.
    5  aborted before any write (missing served object, snapshot check, convention gate, manifest
       present, or ANY unhandled exception before the first upload - _guarded_main maps it)
    6  prices rebased AND verified, but the variables/quality sync failed - serving incomplete
Everything from the first upload to the last VERIFY line runs inside ONE try: any exception there
RESTORES FIRST, writes the outcome to <snap_dir>/_RESULT.txt, and only then prints (a dead console
cannot stop a restore, R735); exit 1 (or 4 if the restore failed). A snapshot directory that already
holds a _MANIFEST.txt is never overwritten (exit 5): after an exit 4 it is the only copy of the
pre-rebase state.

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
A verification MISMATCH restores the snapshot automatically and exits 1 (R725).

RECORDED EVENTS AND THE EVIDENCE RULE (2026-09-05, R725/R728). Yahoo's `.splits` is the default
record of splits after the seam (it carries RZG's 3:1 of 2023-07-17, so pass 1 needs no events
file); `--events-file ticker,date,ratio,source` adds issuer-recorded events Yahoo lacks - symbols
renamed at their split (the Invesco wave, SEC 497 accession 0001104659-23-077058) - and is
unioned with Yahoo's on the same event_steps / prefix path. When a chronological-prefix product
of the recorded events lies within 1 % of P, that product is K (the 0.2 % snap then serves only
the no-event case) - under THREE conditions, each of which caught a wrong rebase in review:
every event in the prefix must be a canonical split ratio (spin-off adjustment factors such as
MMM 1.196 / T 1.324 are recorded by Yahoo as "splits" and are REFUSED); the prefix's newest event
must fall on or before the last served session; and D must sit nearer 1 than P_int
(|ln D| < |ln(D/P_int)|) - a recorded split that reached NEITHER half leaves D near P, and
event_steps cannot tell "applied" from "never happened" at a recorded date (R719), which is how
a phantom event one session inside a dead series' range planned a 10x cliff (R728). P/P_int is
tested against 0.3 % BEFORE any write, because (b) is exactly that number; with an evidence K
the pre-seam window may carry up to 0.6 % of closing-print noise (WARNING), beyond which it
REFUSES. Of the eight tickers first refused for "no recorded event", five are dead series whose
post half never carried the split (RYH RYT RTM RGI RYU: renamed in 2023, D within 3 % of 1 -
refused by the D-rule); PSJ and PWC are real K=3 seams on series dead since 2023-08 (PWC's
D=0.986 says its post half DOES carry the 3:1 - it is PSJ's twin, not a dead-before-event case);
and RZG is a real K=3 seam that Yahoo records - it had been refused on the 0.2 % snap (P=2.9914).
The D-rule is silent at P_int = 1 (its fixed point; 913 dividend-only payers sit there), and
|ln D| > ln 1.5 refuses outright: a post half that far off the market is a defect of its own.

Run from inside pipeline/ of a MAIN-based tree (sibling imports; r2_client stamps parquet metadata).
    python seam_rebase.py TICKER [--mode split|full] [--apply] [--snapshot-dir DIR]
    python seam_rebase.py TICKER --restore SNAPDIR
"""
from __future__ import annotations
import argparse
import datetime as _dt
import hashlib
import math
import os
import sys
import pandas as pd


_STATE = {"wrote": False}     # set the moment the first upload starts (R738): a late escape is then 4, never 5


def _say(msg: str) -> None:
    """A print that survives a dead stdout. The fourth review (R735) measured that when the batch
    driver's pipe is gone, `print` raises OSError 22 - and the old handler printed BEFORE it
    restored, so the restore was never reached. Nothing on the restore path may depend on the
    console. Catches BaseException (R738): a Ctrl-C landing inside this print must not turn a
    finished restore into an unrecorded exit."""
    try:
        print(msg, flush=True)
    except BaseException:                                       # noqa: BLE001
        pass


def _record(snap_dir: str, text: str) -> None:
    """Append the outcome to <snap_dir>/_RESULT.txt BEFORE any stdout: the file is the record when
    the console is gone (R735). Never raises - not even for an interrupt (R738)."""
    try:
        with open(os.path.join(snap_dir, "_RESULT.txt"), "a", encoding="utf-8") as f:
            f.write(f"{_dt.datetime.now(_dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ}\t{text}\n")
    except BaseException:                                       # noqa: BLE001
        pass

from aggregate import aggregate_all
from r2_client import download_parquet, get_client, upload_csv, upload_parquet
from variables_sync import sync_ticker_variables

TIMEFRAMES = ["5min", "15min", "30min", "hourly", "daily", "weekly", "monthly"]
SEAM = pd.Timestamp("2022-03-07")
FIX = pd.Timestamp("2026-07-13")
PRICE_COLS = ("Open", "High", "Low", "Close")
BUCKET = "hfdatalibrary-data"
INTS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30, 40, 50, 60, 100, 200, 250, 500, 1000, 2000]
# 3:2 and 5:2 are the fractional ratios that occur as real splits in this universe (PCAR 2023, ODFL, RJF,
# ROL, WRB ...); 5:4 and 4:3 are left out - they only ever match spin-off factors (GE 1.253, HPE 1.3348)
FRACS = [1.5, 2.5]
CANON_INT = sorted(set([1.0] + [float(n) for n in INTS] + [1.0 / n for n in INTS] + FRACS + [1.0 / f for f in FRACS]))
WIN = 3
SNAP_PREFIXES = ("raw/", "clean/", "variables/", "quality/", "csv/")


def snap_int(p: float):
    best = min(CANON_INT, key=lambda r: abs(math.log(p / r)))
    return best if abs(p / best - 1) <= 0.002 else None


def yahoo(t: str):
    """Daily Close (today's split basis, no dividends) 2022-02-14..today, split events, ex-dates.

    REASSIGNED symbols (R732, the class fix): Yahoo's history under a symbol is its CURRENT owner's.
    For the seven tickers whose IEX symbol passed to another issuer, the closes, splits and
    ex-dates Yahoo returns describe the new company, not the instrument this series is (or was);
    measuring our PiTrading/IEX seam against them is measuring against the wrong instrument.
    Refused here, once, for every caller (seam, scan, repair) - no market reference."""
    from symbol_map import REASSIGNED
    if t.upper() in REASSIGNED:
        print(f"  {t}: IEX symbol reassigned to another issuer from {REASSIGNED[t.upper()][0]} - Yahoo's history under "
              f"{t} is the NEW owner's; no market reference for this instrument (R732)")
        return None, None, None
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


def load_events_file(path: str, t: str):
    """Recorded split events for ticker t from a CSV with columns ticker,date,ratio,source, as a
    pd.Series indexed by the (normalized) event date with the share ratio (new/old) as value - the
    same shape as Yahoo's `.splits` slice, restricted to events after the vendor seam."""
    # index_col=False: a comma inside the free-text `source` field otherwise shifts the first
    # column into the index and the file parses to nothing, silently (R728). The row count is
    # asserted against the file's own non-comment lines so a parse that lost rows cannot pass.
    df = pd.read_csv(path, dtype=str, comment="#", index_col=False)
    need = {"ticker", "date", "ratio", "source"}
    if not need.issubset(set(df.columns)):
        raise SystemExit(f"--events-file {path}: columns must include {sorted(need)}, got {list(df.columns)}")
    data_lines = [ln for ln in open(path, encoding="utf-8") if ln.strip() and not ln.lstrip().startswith("#")]
    n_lines = len(data_lines) - 1
    if len(df) != n_lines:
        raise SystemExit(f"--events-file {path}: parsed {len(df)} row(s) but the file has {n_lines} data line(s) - "
                         f"quote any field containing a comma; refusing to run on a partially read event list")
    n_cols = data_lines[0].count(",") + 1
    over = [i for i, ln in enumerate(data_lines[1:], 2) if ln.count('"') == 0 and ln.count(",") + 1 > n_cols]
    if over:
        # index_col=False keeps ticker/date/ratio intact and truncates the excess at the END of the
        # line, i.e. inside `source`; say so rather than let a citation lose its tail silently.
        print(f"  WARNING: --events-file line(s) {over} carry an unquoted comma in `source`; the text after it was dropped")
    sub = df[df["ticker"].str.upper().str.strip() == t.upper()]
    if sub.empty:
        return None
    idx = pd.to_datetime(sub["date"].str.strip()).dt.normalize()
    s = pd.Series([float(r) for r in sub["ratio"]], index=pd.DatetimeIndex(idx)).sort_index()
    return s[s.index > pd.Timestamp("2022-03-04")]


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
    """Download every served object of the ticker into out_dir, content-checked. Every failure here
    exits 5 (ABORTED BEFORE ANY WRITE): nothing has been uploaded yet, so there is nothing to
    restore and the served state is untouched."""
    if os.path.exists(os.path.join(out_dir, "_MANIFEST.txt")):
        # R731: a second run on the same day would overwrite the first run's snapshot - and after
        # an exit 4 that snapshot IS the only copy of the pre-rebase state. Never silently.
        print(f"snapshot: {out_dir} already holds a _MANIFEST.txt from an earlier run - refusing to overwrite it. "
              f"If that run ended with exit 4, run --restore on it first; otherwise pass a fresh --snapshot-dir")
        raise SystemExit(5)
    os.makedirs(out_dir, exist_ok=True)
    names = {f"{ticker}.parquet", f"{ticker}.csv", f"{ticker}.csv.gz", f"{ticker}.json"}
    keys = []
    for pref in SNAP_PREFIXES:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=pref):
            for o in page.get("Contents", []):
                if o["Key"].rsplit("/", 1)[-1] in names:
                    keys.append((o["Key"], o["Size"], o["ETag"].strip('"')))
    if not keys:
        print(f"snapshot: no served objects found for {ticker} - aborting before any write"); raise SystemExit(5)
    for k, size, etag in keys:
        dest = os.path.join(out_dir, k.replace("/", "__"))
        client.download_file(BUCKET, k, dest)
        if os.path.getsize(dest) != size:
            print(f"snapshot: {k} size {size} on R2 vs {os.path.getsize(dest)} on disk - aborting before any write"); raise SystemExit(5)
        if "-" not in etag and md5_of(dest) != etag:
            print(f"snapshot: {k} MD5 {md5_of(dest)} != ETag {etag} - aborting before any write"); raise SystemExit(5)
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
        _say(f"  restored {k}")                                  # a dead console must not abort a restore (R735)
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
    ap.add_argument("--events-file", default=None,
                    help="CSV ticker,date,ratio,source of issuer-recorded split events to union with Yahoo's (old symbols after a rename have no Yahoo split history)")
    a = ap.parse_args(); t = a.ticker.upper()
    client = get_client()
    if a.restore:
        _STATE["wrote"] = True                                  # a restore IS a write to the served set
        try:
            n = restore(client, a.restore)
        except BaseException as ex:                             # noqa: BLE001
            _record(a.restore, f"EXIT 4 --restore FAILED: {type(ex).__name__}: {str(ex)[:200]}")
            _say(f"{t}: RESTORE FAILED ({type(ex).__name__}: {str(ex)[:200]}) - served state UNKNOWN; fix the snapshot "
                 f"directory and re-run --restore"); return 4
        _record(a.restore, f"EXIT 0 --restore put back {n} objects")
        _say(f"{t}: restored {n} objects from {a.restore}"); return 0
    if a.mode == "full" and not a.convention_decided:
        print("--mode full folds dividends into the pre-2022 half; that is the convention decision. Pass --convention-decided only once it is recorded."); return 5

    daily = download_parquet(client, "raw", t, "daily")
    if daily is None or daily.empty:
        print(f"{t}: no served daily file - aborted before any write"); return 5
    daily["datetime"] = pd.to_datetime(daily["datetime"]); dd = daily.set_index("datetime").sort_index()
    if "source" in dd.columns and not ((dd["source"] == "pitrading").any() and (dd["source"] == "iex").any()):
        print(f"{t}: no PiTrading/IEX splice in the served file - nothing to rebase"); return 0
    h, spl, div = yahoo(t)
    if h is None:
        print(f"{t}: no market reference at Yahoo - cannot measure; disclose, do not repair"); return 3
    y = h["Close"]
    if a.events_file:
        # Issuer-recorded splits that Yahoo's `.splits` does not carry. Measured 2026-09-05: the seven
        # Invesco ETFs refused as "P_int=N equals no chronological-prefix product of the recorded events []"
        # all split on 2023-07-17 (RYH/RYT 10:1, RTM/RGI 5:1, RYU 2:1, PSJ/PWC 3:1 - an issuer wave of 23
        # forward splits), and RZG's 3:1 the same day sits behind a 0.29 % snap miss. The file is a
        # citation, not a guess: ticker,date,ratio,source. It is UNIONED with Yahoo's events (file wins on
        # a shared date) and then flows through exactly the same event_steps / prefix checks.
        extra = load_events_file(a.events_file, t)
        if extra is not None and len(extra):
            spl = pd.concat([spl, extra]) if len(spl) else extra
            spl = spl[~spl.index.duplicated(keep="last")].sort_index()
            print(f"  events file {a.events_file}: {len(extra)} recorded event(s) for {t} unioned with Yahoo's: "
                  f"{[(str(i.date()), float(v)) for i, v in extra.items()]}")

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

    # EVIDENCE BEFORE SNAP. The 0.2 % snap exists for the no-event case, where a spin-off factor near an
    # integer must not be mistaken for a split. When recorded events exist AND every in-range event has
    # just been measured APPLIED in the served series (event_steps above), a chronological-prefix product
    # within 1 % of P is positive evidence of K and outranks the snap: a 3-session median against Yahoo on
    # a thin ETF carries ~0.5 % of closing-print noise (RZG 2026-09-05: P=2.991433, spread 0.50 %, recorded
    # 3:1 on 2023-07-17 measured applied), which is measurement noise, not a spin-off.
    # R725 (review of the first version of this rule): two conditions, both load-bearing.
    #  (1) CANONICAL ONLY - every event in the matching prefix must itself be a split ratio
    #      (snap_int(v) is not None). Yahoo records spin-off ADJUSTMENT FACTORS as "splits"
    #      (MMM 1.196 Solventum, T 1.324 WBD); without this filter the rule accepted them as K
    #      and split mode would have rescaled volume by a spin-off factor - wrong under every
    #      price convention. The convention decision on those factors is Ahmed's.
    #  (2) INSIDE THE SERVED RANGE - the matching prefix's newest event must be dated on or
    #      before the last served session. event_steps() skips later events, so nothing else
    #      would see them: with an events file, five ETFs whose series END in June 2023 planned a
    #      5x/10x/2x rebase for a July 2023 split that never reached their served data.
    evidence_K, evidence_last = None, None
    if len(spl):
        _prod, _all_canon = 1.0, True
        for _i, _v in spl.items():
            _all_canon = _all_canon and (snap_int(float(_v)) is not None)
            _prod *= float(_v)
            if _all_canon and abs(P / _prod - 1) <= 0.01:
                evidence_K, evidence_last = _prod, _i
    if evidence_K is not None and evidence_last > dd.index.max():
        print(f"  REFUSED: the recorded event {evidence_last.date()} that would explain P={P:.6f} is AFTER the last served "
              f"session {dd.index.max().date()} - it never reached this series; a rebase would manufacture the step"); return 2
    if evidence_K is not None:
        P_int = evidence_K
        print(f"  P={P:.6f} matched the recorded-event product {P_int:g} within 1 % (evidence rule: canonical events, "
              f"newest {evidence_last.date()} inside the served range; snap tolerance not applied)")
    else:
        P_int = snap_int(P)
    if P_int is None:
        print(f"  REFUSED: P={P:.6f} does not snap to an integer split ratio within 0.2 % (spin-off or unclear)"); return 2
    # VERIFY (b) is deterministic - every pre-seam bar is scaled by K, so P' = P / P_int exactly
    # (AMZN: 19.993751 / 20 = 0.999688, the logged P'). Test it here, BEFORE any write, at the
    # same 0.3 % the verification uses; a plan that would fail (b) is refused instead of applied
    # and then restored.
    if abs(P / P_int - 1) > 0.003:
        print(f"  REFUSED: P/P_int = {P / P_int:.6f} is {abs(P / P_int - 1):.3%} from 1 - the rebase would fail VERIFY (b) at 0.3 %; "
              f"measure with a wider window or leave for the convention decision"); return 2
    # R728: THE DATA DISCRIMINATOR. A split that reached the post-seam half leaves D (post half vs
    # the market) near 1 - or near the dividend factor; one that reached NEITHER half leaves D near
    # P. event_steps cannot tell "applied" from "never happened" at a recorded date (both read a
    # step of ~1, R719), and the after-range guard compares dates, so a recorded event dated one
    # session inside a dead series' range (RYH: series ends 2023-06-02, phantom event 2023-06-01)
    # still planned a 10x cliff. D decides: if D is at least as far from 1 as it is from P_int,
    # the post half never carried the split and there is no seam to remove. Measured 2026-09-05
    # over the 105 planned tickers: 0 refused (closest EEV/EFU/FXP/EUM/PCAR at 0.13-0.15 vs
    # 0.55-0.57); RYH/RGI/RTM/RYT/RYU refused with no events file at all (0.008-0.028 vs 0.67-2.30).
    # P_int = 1 is the rule's fixed point (|ln D| >= |ln(D/1)| is always true) and 913 measurable
    # tickers sit there - dividend-only payers with no split to test; the rule has nothing to say
    # about them and must not fire (R731).
    if abs(math.log(P_int)) > 0.002 and abs(math.log(D)) >= abs(math.log(D / P_int)):
        print(f"  REFUSED: D={D:.6f} is as far from 1 as from P_int={P_int:g} - the post-seam half sits on the SAME "
              f"basis as the pre-seam half, so the recorded split never reached this series (dead series or "
              f"phantom event); no seam to remove - refusing"); return 2
    # THE BAND (R731, corrected R735). D is the post-seam half against the market. With a split in
    # play (P_int != 1) a |ln D| beyond ln 1.5 means the served later half sits on a basis no
    # recorded event explains, and rebasing the pre half onto it would spread the defect. The band
    # is SILENT at P_int = 1: there D is the dividend factor itself, and the fourth review measured
    # BITO 0.267, PBR 0.378, AIV 0.502 as exactly their cumulative payouts (Yahoo ex-dates,
    # ratio to D 0.99-1.01) - the first version refused all three as "a bad merge".
    if abs(math.log(P_int)) > 0.002 and abs(math.log(D)) > math.log(1.5):
        print(f"  REFUSED: the post-seam half is x{D:.4f} against the market (|ln D|={abs(math.log(D)):.3f} > ln 1.5) with a "
              f"{P_int:g}x split in play - more than a dividend factor accounts for; the served later half's basis has to be "
              f"explained before its pre-seam half is rebased onto it"); return 2
    # P_int must equal the product of the OLDEST k recorded events for some k. k = n: the pre-seam half
    # misses all of them. k < n: the later events were applied history-wide (by the daily path, or by a
    # manual_split repair - SCO/SMN tonight) and only the oldest k are still missing before the seam.
    # No date (the 2026-07-13 "fix") is assumed to mean anything (R719): the rule is accepted only when
    # every recorded event inside the served range measured APPLIED in the served series.
    evs = [(i, float(v)) for i, v in spl.items()]
    prefixes, prod = [(0, 1.0)], 1.0          # k = 0: nothing missing before the seam (already rebased / repaired history-wide)
    for k, (i, v) in enumerate(evs, 1):
        prod *= v; prefixes.append((k, prod))
    match_k = next((k for k, pr in prefixes if abs(P_int / pr - 1) <= 0.002), None)
    if match_k is None:
        print(f"  REFUSED: P_int={P_int:g} equals no chronological-prefix product of the recorded events {[(str(i.date()), v) for i, v in evs]}"); return 2
    in_range = [(i.date(), v) for i, v in evs if i <= dd.index.max()]
    applied_dates = {d for d, s, rel in app}
    missing = [e for e in in_range if e[0] not in applied_dates]
    if missing:
        print(f"  REFUSED: event(s) {missing} are not measured APPLIED in the served series (measured applied: {app}); nothing is assumed"); return 2
    match = "all" if match_k == len(evs) else f"oldest {match_k} of {len(evs)}"
    if match_k < len(evs):
        print(f"  later event(s) measured APPLIED history-wide; the pre-seam half misses only the oldest {match_k}: {[(str(i.date()), v) for i, v in evs[:match_k]]}")
    if sP > 0.003 and (evidence_K is None or sP > 0.006):
        # Without a recorded event the window IS the identification of K, so 0.3 % stands. With one,
        # K is settled by the event and P/P_int has already passed the pre-write (b) test above, so
        # up to 0.6 % of closing-print noise (thin ETFs: RZG 0.50 %) is tolerated; beyond that the
        # window is telling us something else is going on (PWC 1.14 %, UPW 0.94 % - R725 item 5).
        print(f"  REFUSED: PiTrading side unstable over its window ({sP:.3%}){' - ' + ex_note if ex_note else ''}"); return 2
    if sP > 0.003:
        print(f"  WARNING: PiTrading side unstable over its window ({sP:.3%}, within the 0.6 % allowed when K comes from a "
              f"recorded event and P/P_int passed the pre-write (b) test)")
    K = 1.0 / P_int; V = P_int
    if a.mode == "full":
        K = D / P_int
    target = 1.0 if a.mode == "split" else D

    # 1-minute self-check FIRST (before any "nothing to do" exit): the files' own last pre-seam BAR vs
    # the market, raw AND clean. An interrupted earlier run leaves raw and clean on different bases,
    # and that must be seen even when the plan itself would be a no-op.
    raw = download_parquet(client, "raw", t); clean = download_parquet(client, "clean", t)
    if raw is None or raw.empty or clean is None or clean.empty:
        print(f"{t}: served raw/clean 1-minute file missing - aborted before any write"); return 5
    extra = [c for c in raw.columns if c not in PRICE_COLS + ("Volume", "datetime", "source")]
    if extra:
        print(f"{t}: raw carries unexpected columns {extra} - refusing, aborted before any write"); return 5
    d_r, c_r = last_pre_session_close(raw); d_c, c_c = last_pre_session_close(clean)
    if d_r is None or d_c is None or d_r not in y.index or d_c not in y.index:
        print(f"  REFUSED: cannot place the 1-minute files' last pre-seam session against the market (raw {d_r}, clean {d_c})"); return 2
    P_raw, P_clean = c_r / float(y[d_r]), c_c / float(y[d_c])
    if not (math.isfinite(P_raw) and math.isfinite(P_clean)):
        print(f"  REFUSED: non-finite self-check (P_raw={P_raw}, P_clean={P_clean})"); return 2
    print(f"  1-minute self-check: raw last pre-seam bar {d_r.date()} P_raw={P_raw:.6f}; clean {d_c.date()} P_clean={P_clean:.6f}; target after rebase {P_raw * K:.6f}")
    if abs(P_raw / P_clean - 1) > 0.003:
        print(f"  REFUSED: raw and clean disagree on the pre-seam basis (P_raw={P_raw:.6f}, P_clean={P_clean:.6f}) - a previous run was interrupted; use --restore"); return 2
    # THE 1-MINUTE FILES MUST AGREE WITH THE DAILY THAT PRODUCED P (R738 finding 4): a daily at P = 1
    # over 1-minute files still at 20x is a surviving partial write, and both no-op exits below would
    # have called it healthy. Same tolerance as everything else on this basis; the largest live
    # disagreement seen today is 0.03 % (AMZN).
    if abs(P_raw / P - 1) > 0.003:
        print(f"  INCONSISTENT SERVED SET: the served daily measures P={P:.6f} against the market while the 1-minute raw "
              f"file measures P_raw={P_raw:.6f} on its last pre-seam bar - the aggregates and the 1-minute objects sit on "
              f"different bases (a partial earlier write). Restore that run's snapshot or re-aggregate from the 1-minute "
              f"files; nothing touched"); return 4
    if abs(K - 1) <= 0.002 and abs(V - 1) <= 1e-9:
        print(f"  nothing to rebase in --mode {a.mode} (P_int=1{'; dividend/spin seam D=%.4f held for the convention decision' % D if abs(D - 1) > 0.002 else ''}); raw and clean agree"); return 0
    if abs(P_raw / target - 1) <= 0.003:
        if abs(math.log(P_int)) > 0.002:
            # R735 finding 4: with a split in play this state is inconsistent by construction - the
            # 1-minute files already sit on target while the served DAILY (which produced P) does
            # not. An earlier run wrote the 1-minute objects and not the aggregates. Not "nothing to
            # do": a human restores that run's snapshot or re-aggregates; exit 4 stops the batch.
            print(f"  INCONSISTENT SERVED SET: 1-minute files already on target (P_raw={P_raw:.6f} ~ {target:.6f}) while the "
                  f"served daily still shows P={P:.6f} (P_int={P_int:g}) - a partial earlier write survived. Restore that run's "
                  f"snapshot (F:/hf_r2_snapshot_seam_<date>/{t}) or re-aggregate from the 1-minute files; nothing touched"); return 4
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

    snap_dir = a.snapshot_dir or os.path.join("F:\\", f"hf_r2_snapshot_seam_{_dt.datetime.now(_dt.timezone.utc):%Y%m%d}", t)
    n_snap = snapshot(client, t, snap_dir)
    print(f"  snapshot: {n_snap} objects -> {snap_dir} (size + MD5/ETag verified)")

    raw2, clean2 = rescale(raw, K, V), rescale(clean, K, V)
    n = 0; sync_failed = []
    _STATE["wrote"] = True                                      # from here an escape is exit 4, never 5 (R738)
    # ONE try from the first upload through the last VERIFY print (R728 item 2, R731 finding 1).
    # Anything that dies in here - an upload, a read-back returning None, a botocore error on the
    # three verification downloads, a format error in a VERIFY line - restores the snapshot and
    # exits 1; a failed restore exits 4. The third review proved the previous shape wrapped only the
    # uploads, so a VERIFY crash exited 1 UNRESTORED while the batch driver announced "restored".
    try:
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
        d2 = download_parquet(client, "raw", t, "daily")
        if d2 is None or d2.empty:
            raise RuntimeError("served daily file unreadable after the upload")
        d2["datetime"] = pd.to_datetime(d2["datetime"])
        dd2 = d2.set_index("datetime").sort_index()
        step_before = float(dd[dd.index >= SEAM]["Close"].iloc[0] / dd[dd.index < SEAM]["Close"].iloc[-1])
        step_after = float(dd2[dd2.index >= SEAM]["Close"].iloc[0] / dd2[dd2.index < SEAM]["Close"].iloc[-1])
        ok_a = abs(step_after / (step_before / K) - 1) < 0.005
        P2, _, _ = ratio_over(dd2["Close"], y, pre_sessions)
        ok_b = P2 is not None and abs(P2 / target - 1) < 0.003
        # (c) internal consistency of the served 1-minute files: clean's last pre-seam bar == raw's, and
        #     raw's == the pre-computed P_raw*K (a 15:59 print is not the closing auction, so no market gate here)
        raw_srv = download_parquet(client, "raw", t); clean_srv = download_parquet(client, "clean", t)
        if raw_srv is None or raw_srv.empty or clean_srv is None or clean_srv.empty:
            raise RuntimeError("served 1-minute file unreadable after the upload")
        d_r2, c_r2 = last_pre_session_close(raw_srv); d_c2, c_c2 = last_pre_session_close(clean_srv)
        mkt2 = (c_r2 / float(y[d_r2])) if (d_r2 is not None and d_r2 in y.index) else float("nan")
        ok_c = (d_r2 is not None and d_c2 is not None and math.isfinite(mkt2)
                and abs(c_c2 / c_r2 - 1) < 0.0005 and abs(mkt2 / (P_raw * K) - 1) < 0.0005)
        print(f"  VERIFY (a) served daily seam step x{step_after:.6f} vs expected x{step_before / K:.6f} -> {'OK' if ok_a else 'MISMATCH'}")
        print(f"  VERIFY (b) rebased P'={P2} vs target {target:.6f} -> {'OK' if ok_b else 'MISMATCH'}")
        print(f"  VERIFY (c) served 1-minute: clean last pre-seam bar {c_c2} vs raw {c_r2}; raw/market {mkt2:.6f} vs P_raw*K {P_raw * K:.6f} -> {'OK' if ok_c else 'MISMATCH'}")
        verified = bool(ok_a and ok_b and ok_c)
    except BaseException as ex:                                 # noqa: BLE001
        # RESTORE FIRST, RECORD SECOND, PRINT LAST (R735). The previous shape printed before it
        # restored, and when the driver's pipe was gone that print raised OSError 22 - the restore
        # was never reached and the rescaled objects stayed live with no record. Nothing here may
        # raise before restore() returns; the outcome is written to <snap_dir>/_RESULT.txt before
        # any stdout; every print is _say.
        why = f"{type(ex).__name__}: {str(ex)[:200]}"
        try:
            n_back = restore(client, snap_dir)
        except BaseException as ex2:                            # noqa: BLE001
            _record(snap_dir, f"EXIT 4 RESTORE FAILED after {n} upload(s); cause {why}; restore error {type(ex2).__name__}: {str(ex2)[:200]}")
            _say(f"  FAILED after the snapshot with {n} object(s) uploaded ({why}) and the RESTORE FAILED "
                 f"({type(ex2).__name__}: {str(ex2)[:200]}) - the rescaled objects may be LIVE; "
                 f"run: python seam_rebase.py {t} --restore \"{snap_dir}\""); return 4
        _record(snap_dir, f"EXIT 1 RESTORED {n_back} objects after {n} upload(s); cause {why}")
        _say(f"  FAILED after the snapshot with {n} object(s) uploaded ({why}) - restored {n_back} objects from {snap_dir}; "
             f"served state is the pre-rebase state"); return 1
    if not verified:
        # R725 item 4: a rescale that fails its own verification must not stay served while a human
        # reads a log. Restore the content-checked snapshot NOW (restore first, R735), then report.
        try:
            n_back = restore(client, snap_dir)
        except BaseException as e:                              # noqa: BLE001
            _record(snap_dir, f"EXIT 4 NOT VERIFIED and RESTORE FAILED: {type(e).__name__}: {str(e)[:200]}")
            _say(f"  NOT VERIFIED and the automatic restore FAILED ({type(e).__name__}: {e}) - the rescaled objects are LIVE; "
                 f"run: python seam_rebase.py {t} --restore \"{snap_dir}\"")
            return 4                                            # stopped, served state UNKNOWN - a human acts now
        _record(snap_dir, f"EXIT 1 NOT VERIFIED - restored {n_back} objects")
        _say(f"  NOT VERIFIED - restored {n_back} objects from the snapshot {snap_dir}; served state is the pre-rebase state")
        return 1                                                # stopped, nothing left changed
    if sync_failed:
        _record(snap_dir, f"EXIT 6 verified, variables sync failed: {sync_failed}")
        print(f"  PRICES VERIFIED but variables/quality sync failed: {sync_failed}. Not restoring; run "
              f"sync_ticker_variables(force_full=True) for {t} - its serving is incomplete until then (a re-run of this "
              f"tool answers ALREADY on target and does not re-sync)"); return 6
    _record(snap_dir, f"EXIT 0 DONE rebased ({a.mode}) K={K:.6g} V={V:g}")
    _say(f"  DONE: {t} rebased ({a.mode}); snapshot kept at {snap_dir}")
    return 0


def _guarded_main() -> int:
    """Exit 1 must mean 'written then restored' and nothing else (R735 finding 3). Every upload sits
    inside main()'s guarded block, which returns 1 or 4 itself, so an exception that escapes main()
    happened BEFORE the first upload (a Yahoo rate limit, a botocore error on a read, a division by
    zero in the measurement) - that is exit 5, aborted before any write. SystemExit(5) from
    snapshot() passes through unchanged."""
    try:
        return main()
    except SystemExit as ex:
        if isinstance(ex.code, int) or ex.code is None:
            raise                                               # the tool's own numeric codes (5 from snapshot(), argparse's 2)
        # SystemExit(<string>) - Python would exit 1 = "restored" (R738 finding 2): the events-file
        # checks raise these before any write
        _say(f"  {ex.code}")
        _say("  ABORTED before any write (exit 5)" if not _STATE["wrote"] else "  ESCAPED AFTER WRITES - served state UNKNOWN (exit 4)")
        return 4 if _STATE["wrote"] else 5
    except KeyboardInterrupt:
        if _STATE["wrote"]:
            _say("  interrupted AFTER writes began and outside the guarded block - served state UNKNOWN; read _RESULT.txt in the snapshot dir - exit 4"); return 4
        _say("  interrupted before any write (an interrupt after the first upload is caught inside and restores) - exit 5"); return 5
    except BaseException as ex:                                 # noqa: BLE001
        if _STATE["wrote"]:
            _say(f"  ESCAPED AFTER WRITES by an unhandled {type(ex).__name__}: {str(ex)[:300]} - served state UNKNOWN; read _RESULT.txt - exit 4"); return 4
        _say(f"  ABORTED before any write by an unhandled {type(ex).__name__}: {str(ex)[:300]} - exit 5"); return 5


if __name__ == "__main__":
    sys.exit(_guarded_main())

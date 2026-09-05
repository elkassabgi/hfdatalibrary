"""repair_reassigned.py v3 - remove another company's prints from a served series whose IEX symbol
was reassigned, put the kept half back on the ORIGINAL instrument's basis where a later owner's
corporate action was applied to it, and (GOLD only) rebuild the window from the retained
class-share extractions. Version 3 after adversarial reviews R732, R736 and R740 (2026-09-05).

EXIT CODES (one meaning each; every outcome after the snapshot is recorded in <snap_dir>/_RESULT.txt
BEFORE anything is printed, every print on those paths survives a dead console, and the entry point
maps any escape to 5 before the first upload and 4 after it - the seam tool's contract, R735/R738):
0 done and verified | 1 written then RESTORED | 2 refused before any write | 3 written, UNVERIFIABLE
(read-back or market fetch failed), data live | 4 restore FAILED or an escape after writes | 5 aborted
before any write | 6 prices verified, variables/quality sync failed (stale objects named).
The served contract is seven columns (datetime, Open, High, Low, Close, Volume, source) on both
1-minute files: both frames are projected onto them before the gate and asserted, because the
2026-07-13 clean snapshots carry four legacy flag columns (R740).

WHY (2026-09-05, ledger R727). The daily path and the backfill filter IEX prints by EXACT symbol,
so when a symbol passes to a different issuer the served series silently continues with the new
issuer's prints. Measured by a census of every served ticker live in 2025-2026 (our daily close
vs the symbol's current owner on Yahoo):

    GOLD  Barrick through 2025-12-01, Gold.com, Inc. from 2025-12-02 (191 sessions)
    STI   SunTrust through 2019-12-06, Solidion Technology from 2024-02-05
    IPW   SPDR S&P International Energy Sector ETF (2008-2017; SPDR Index Shares Funds 485BPOS 2008-07-16,
          acc. 0000950135-08-004982), iPower Inc. from 2021-05-12
    SKK   ProShares UltraShort Russell2000 Growth (2007-2015; ProShares Trust 497 2014-12-23,
          acc. 0001193125-14-452796), SKK Holdings from 2024-10-08
    VRM   Vroom's cancelled equity through 2024-11-29, the post-Chapter-11 Vroom from 2025-02-20
    USLV  VelocityShares 3x Silver ETN through 2020-07-02, a Direxion ETF from 2026-05-27
    PARA  Paramount Global through 2025-08-06, Banzai International from 2026-08-07

WHAT REVIEW R732 FOUND. Cutting the foreign prints is not enough: three of the seven serve the
ORIGINAL instrument on the NEW owner's split basis across their whole history - PARA at 1/6 (the
daily split detector fired on Banzai's $1.84 first day, 2026-08-12, and rescaled Paramount's and
the PiTrading half's history), IPW at 72x and SKK at 10x (repair_unapplied_splits applied iPower's
and SKK Holdings' splits to instruments that died in 2017 and 2015, 2026-09-05 07:06-07:46Z by the
snapshot manifests - the "02:07-02:46Z" in R732 were local-clock stamps). The
cut alone would have VERIFIED and kept serving them wrong. So v2:

  * --unscale F (PARA: 6). Every kept RAW bar: price x F rounded to 4 decimals and volume / F,
    which must divide exactly on every bar or the tool refuses. "Exact" is qualified (R736): the
    PiTrading half round-trips to the 2026-07-13 snapshot bar for bar (0 of 1,577,005 off the 4-dp
    grid), the IEX window half lands within 5e-5 of the grid on 220,438 of 294,102 bars (prints
    carry sub-cent prices). The CLEAN half is NOT rescaled in place: the served clean is the daily
    path's full re-clean at the foreign basis, so its bar set was decided there (58,582 pre-detector
    bars missing); the pre-window clean is taken from the 2026-07-13 clean and the window is
    re-cleaned at the original basis, counts printed.
  * --kept-from SNAPDIR (IPW: F:/hf_r2_snapshot_splits_20260905/IPW_20260522, SKK:
    .../SKK_20260406). The kept half is taken from the pre-repair snapshot's raw__T / clean__T
    (bars before the cut): a rounded volume (5,471 -> 76) cannot be inverted arithmetically.
  * THE BASIS GATE, every ticker, in the dry run too. The frame about to be uploaded is compared
    with INDEPENDENT anchors: for window sessions (2022-03-07..2026-03-27) the retained prints
    E:/iex_hist_backfill/<ymd>/trades_<ymd>.csv rebuilt into bars with the daily path's own
    parser and bar builder; for pre-window sessions the 2026-07-13 R2 snapshot
    (F:/hf_r2_snapshot_20260713), which predates every split-detector fire; plus any explicit
    --anchor DATE:CLOSE:VOLUME (R732's print-set and snapshot values). Session close within
    0.05 %, session volume EXACT (bars and prints are the same numbers). Any deviation refuses.
    And the whole pre-window half, raw AND clean, must equal the 2026-07-13 snapshot bar for bar
    on every column (R736): that half is a complete free oracle, so it is not sampled.
  * VERIFY runs inside the try (a crash restores, exit 1; a failed restore exits 4). A market
    fetch failure DURING verification exits 3 - UNVERIFIED, DATA LIVE - without restoring:
    nothing showed the writes wrong, and a restore on an empty Yahoo answer would undo a correct
    repair. (a) covers raw AND clean daily; (c) covers the rebuild case; the anchors and the basis
    samples are re-checked on the SERVED 1-minute file; a failed variables sync names the four
    stale objects and exits 6.

WHAT IT DOES for TICKER --cut DATE (the new owner's first session):
  1. reads the served raw/clean 1-minute files; drops every bar dated >= DATE (the foreign
     prints), or replaces the kept half from --kept-from; applies --unscale;
  2. GOLD only, --rebuild-from-cs B --until 2026-03-27: rebuilds Barrick's bars for DATE..until
     from trades_cs_<ymd>.csv (the backfill's class-share pass; 2026-03-27 holds 10,363 B prints),
     remaps B -> GOLD, appends them to raw, and cleans them exactly as merge_ticker does an
     incremental day (CONTEXT_BARS of the existing clean tail through clean_bars);
  3. runs the basis gate; prints the plan and the bar-count delta (metadata.json's counters are
     increment-only and will not reflect it - record the delta);
  4. with --apply and no Daily Data Update in flight: content-checked snapshot of all 22 served
     objects (seam_rebase.snapshot; a directory that already holds a manifest exits 5), uploads
     1-minute parquet + CSV x2, 14 timeframe objects, variables/quality (force_full) - merge_ticker's
     sequence - then VERIFY from the served side (the exit-code contract is at the top of this docstring).

  python repair_reassigned.py GOLD --cut 2025-12-02 --rebuild-from-cs B --until 2026-03-27 --verify-against B
  python repair_reassigned.py PARA --cut 2026-08-07 --unscale 6 --anchor 2025-08-06:11.07:1408409 --anchor 2022-03-04:34.07:12816002
  python repair_reassigned.py IPW  --cut 2021-05-12 --kept-from F:/hf_r2_snapshot_splits_20260905/IPW_20260522 --anchor 2017-07-24:17.80:5471
  python repair_reassigned.py SKK  --cut 2024-10-08 --kept-from F:/hf_r2_snapshot_splits_20260905/SKK_20260406 --anchor 2015-01-08:33.55:4056
  python repair_reassigned.py STI  --cut 2024-02-05        (VRM --cut 2025-02-20, USLV --cut 2026-05-27)
Run from inside pipeline/ of a MAIN-based tree (sibling imports; r2_client stamps parquet metadata).
Sequencing (R732 item 1): nothing is applied before PR #12's symbol map is on main - the next daily
run would otherwise re-append the new owners and, on today's prices, rescale STI (1/9) and USLV (1/4).
"""
from __future__ import annotations
import argparse
import dataclasses
import datetime as dt
import math
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from r2_client import get_client, download_parquet, upload_parquet, upload_csv        # noqa: E402
from aggregate import aggregate_all, TIMEFRAMES                                       # noqa: E402
from clean_pipeline import clean_bars                                                 # noqa: E402
from variables_sync import sync_ticker_variables                                      # noqa: E402
from tops_parser import parse_trades_csv                                              # noqa: E402
from build_bars import build_bars                                                     # noqa: E402
import daily_update                                                                   # noqa: E402
import seam_rebase                                                                    # noqa: E402

CS_ROOT = "E:/iex_hist_backfill"
SNAP_0713 = "F:/hf_r2_snapshot_20260713"
WINDOW = (dt.date(2022, 3, 7), dt.date(2026, 3, 27))      # the retained-prints window
CS_FALLBACK_FROM = dt.date(2025, 5, 1)                      # Barrick printed as B from 2025-05-01 (symbol_map REMAP); the
                                                            # class-share anchor fallback covers the original's B-era only
STANDARD_COLS = ["datetime", "Open", "High", "Low", "Close", "Volume", "source"]   # the served contract (daily_update.py)
PRICE_COLS = ("Open", "High", "Low", "Close")
CLOSE_TOL = 0.0005                                          # 0.05 % on a session close


def _bars_from_cs(day: dt.date, iex_symbol: str, ticker: str) -> list[dict]:
    """Minute bars for one day from the class-share extraction, remapped to the dataset ticker."""
    ymd = day.strftime("%Y%m%d")
    path = os.path.join(CS_ROOT, ymd, f"trades_cs_{ymd}.csv")
    if not os.path.exists(path):
        return []
    trades = [dataclasses.replace(t, symbol=ticker) for t in parse_trades_csv(path, universe={iex_symbol})]
    if not trades:
        return []
    rows = []
    for b in build_bars(trades).get(ticker, []):
        rows.append({"ticker": ticker, "datetime": b.minute_start, "Open": b.open, "High": b.high,
                     "Low": b.low, "Close": b.close, "Volume": b.volume, "source": "iex"})
    return rows


def _print_anchor(day: dt.date, symbol: str, cs_symbol: str | None = None):
    """(last close, session volume, bars) of `symbol` on `day` from the retained main-pass prints; when the
    main pass has none and a class-share symbol is given (GOLD: Barrick printed as B on 2025-12-01, its
    last session, and the served bars for that day came from the cs pass), from trades_cs_<ymd>.csv."""
    ymd = day.strftime("%Y%m%d")
    for path, sym in ((os.path.join(CS_ROOT, ymd, f"trades_{ymd}.csv"), symbol),
                      (os.path.join(CS_ROOT, ymd, f"trades_cs_{ymd}.csv"), cs_symbol)):
        if sym is None or not os.path.exists(path):
            continue
        if sym == cs_symbol and sym != symbol and day < CS_FALLBACK_FROM:
            # the class-share symbol is another instrument for most of its life (B was Barnes Group
            # until 2025-01-27); the fallback exists for the original's LAST sessions only (R736)
            continue
        trades = list(parse_trades_csv(path, universe={sym}))
        bars = build_bars(trades).get(sym, []) if trades else []
        if bars:
            # per-minute volumes keyed like the served file: naive New York wall time
            minutes = {pd.Timestamp(b.minute_start).tz_convert("America/New_York").tz_localize(None): int(b.volume)
                       for b in bars}
            return float(bars[-1].close), int(sum(b.volume for b in bars)), len(bars), minutes
    return None


def _session_stats(df: pd.DataFrame, day: dt.date):
    s = df[df["datetime"].dt.date == day]
    if s.empty:
        return None
    s = s.sort_values("datetime")
    minutes = dict(zip(s["datetime"], s["Volume"].astype(int)))
    return float(s["Close"].iloc[-1]), int(s["Volume"].sum()), len(s), minutes


def _minute_volume_match(ours: dict, theirs: dict, factor: float):
    """Under an own split the served minute volumes are round(print / factor) - rounded per MINUTE, so a
    session SUM is systematically below prints/factor (VRM 2024-01-02: 99 vs 9,746/80 = 121.8). The
    honest test is per minute: |served - print/factor| <= 1 on >= 95 % of the common minutes, with the
    common minutes covering >= 90 % of the print minutes. Returns (fraction_ok, coverage)."""
    # a print minute whose volume / factor rounds to 0 is legitimately absent from the served file
    # (the rescale wrote 0 and the merge keeps no zero-volume raw bar): coverage is measured over the
    # minutes that survive the rounding, and the per-minute test over the common ones.
    survivors = {m: v for m, v in theirs.items() if v / factor >= 0.5}
    common = [m for m in survivors if m in ours]
    if not survivors or not common:
        return 0.0, 0.0
    ok = sum(1 for m in common if abs(ours[m] - survivors[m] / factor) <= 1.0)
    return ok / len(common), len(common) / len(survivors)


def _pick(days: list, k: int) -> list:
    if len(days) <= k:
        return list(days)
    idx = np.linspace(0, len(days) - 1, k).round().astype(int)
    return [days[i] for i in sorted(set(idx))]


def _sessions(a: dt.date, b: dt.date):
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d += dt.timedelta(days=1)


def _yahoo_close(symbol: str, start: dt.date, end: dt.date) -> pd.Series:
    import yfinance as yf
    h = yf.Ticker(symbol).history(start=start.isoformat(), end=(end + dt.timedelta(days=1)).isoformat(), auto_adjust=False)
    s = h["Close"]
    s.index = [x.date() for x in s.index]
    return s


def _load_snapshot_bars(snap_dir: str, version: str, ticker: str) -> pd.DataFrame:
    p = os.path.join(snap_dir, f"{version}__{ticker}.parquet")
    if not os.path.exists(p):
        raise FileNotFoundError(p)
    df = pd.read_parquet(p)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def _unscale(df: pd.DataFrame, F: float) -> pd.DataFrame:
    """price x F rounded to 4 dp, volume / F exact - refuses when any volume does not divide."""
    out = df.copy()
    vol = out["Volume"].to_numpy()
    rem = np.mod(vol, F)
    if (rem != 0).any():
        bad = int((rem != 0).sum())
        raise SystemExit(f"--unscale {F:g}: {bad:,} kept bar(s) have a volume not divisible by {F:g} "
                         f"(first at {out.loc[rem != 0, 'datetime'].iloc[0]}) - the history is not uniformly "
                         f"scaled by {F:g}; refusing, aborted before any write")
    for c in PRICE_COLS:
        out[c] = (out[c].astype(float) * F).round(4)
    out["Volume"] = (vol / F).round().astype("int64")
    return out


DIV_BAND = (0.55, 1.0005)   # a dividend-adjusted window session sits below the raw print by the cumulative payout
                            # (library-wide 2022-03-07 factors measured in review R736: T 0.609, MO 0.714, KO 0.882,
                            # IBM 0.859, XOM 0.871, JNJ 0.891, SPY 0.943 - the floor sits under the deepest payer)


class Unverifiable(Exception):
    """A served object could not be READ BACK for verification (an R2 error that is not a 404). The
    writes are not known wrong, so nothing is restored: exit 3, data live, a human re-verifies (R736)."""


def _served_read(client, version: str, ticker: str, tf: str | None = None) -> pd.DataFrame:
    try:
        df = download_parquet(client, version, ticker, tf) if tf else download_parquet(client, version, ticker)
    except Exception as ex:                                  # noqa: BLE001
        raise Unverifiable(f"R2 read of {version}/{ticker}{('/' + tf) if tf else ''} failed: {type(ex).__name__}: {str(ex)[:160]}")
    if df is None or df.empty:
        # a 404 right after a successful put is an inconsistency, not a read problem: the caller restores
        raise RuntimeError(f"served {version}/{ticker}{('/' + tf) if tf else ''} missing or empty after the upload")
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def pre_window_equality(frame: pd.DataFrame, version: str, ticker: str, cut: dt.date):
    """Every bar of `frame` before WINDOW[0] must equal the 2026-07-13 snapshot's bar at the same minute on
    every column (R736). The snapshot predates every split-detector fire and the backfill kept the
    pre-window half verbatim, so it is a COMPLETE oracle for that half - sampling three sessions of it
    left 1.57M PARA bars unchecked when checking them all costs seconds. Returns
    (n_ours, n_snapshot, n_common, n_mismatch, n_only_ours, n_only_snapshot), or None when the snapshot
    has no file for the ticker."""
    p = os.path.join(SNAP_0713, version, f"{ticker}.parquet")
    if not os.path.exists(p):
        return None
    # the oracle half ends at the earlier of the window start and the CUT: for a symbol reassigned
    # before 2022-03-07 (IPW, 2021-05-12) the snapshot's later pre-window bars are the foreign prints
    bound = min(WINDOW[0], cut)
    snap = pd.read_parquet(p); snap["datetime"] = pd.to_datetime(snap["datetime"])
    snap = snap[snap["datetime"].dt.date < bound]
    ours = frame[frame["datetime"].dt.date < bound]
    cols = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in ours.columns and c in snap.columns]
    m = ours[["datetime"] + cols].merge(snap[["datetime"] + cols], on="datetime", how="outer", suffixes=("_f", "_s"), indicator=True)
    both = m[m["_merge"] == "both"]
    mism = 0
    for c in cols:
        if c == "Volume":
            mism += int((both[f"{c}_f"].astype("int64") != both[f"{c}_s"].astype("int64")).sum())
        else:
            mism += int((~np.isclose(both[f"{c}_f"].astype(float), both[f"{c}_s"].astype(float), rtol=0, atol=1e-9)).sum())
    return len(ours), len(snap), len(both), mism, int((m["_merge"] == "left_only").sum()), int((m["_merge"] == "right_only").sum())


def _standard(df: pd.DataFrame, what: str) -> pd.DataFrame:
    """Project onto the served contract's seven columns and refuse anything else (R740): the 2026-07-13
    clean snapshots carry four legacy boolean flag columns (volume_spike, stale_quote, is_auction,
    splice_artifact on 252 of 1,391 tickers), and a frame that keeps them uploads an 11-column clean
    that nothing downstream ever strips again. The served objects have exactly these seven."""
    missing = [c for c in STANDARD_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"{what}: missing served column(s) {missing} - aborted before any write")
    return df[STANDARD_COLS].copy()


def basis_gate(frame: pd.DataFrame, ticker: str, cut: dt.date, anchors: list, k_window: int, k_pre: int, own_splits=(), cs_symbol=None):
    """Compare the kept half of `frame` (raw 1-minute, what will be uploaded) with independent anchors.

    WHAT THE FIRST DRY RUNS PROVED (2026-09-05 12:38-12:41Z), and what the rule therefore is:
      * PARA after x6: window sessions sit at x0.915 (2022-03-07), x0.964 (2023-04-26), x0.977
        (2024-06-13) against the raw prints, volumes EQUAL, and the last kept session 2025-08-06 at
        exactly x1. That is the library's dividend-adjusted convention (the window was conformed to
        the adjusted series; the factor is the cumulative payout after the session and tends to 1),
        not a corporate action of anyone. A dividend factor never touches volume.
      * VRM: window sessions before 2024-02 sit at x80.000 with volume near 1/80, the 2024-11-29
        session at x1 - Vroom's OWN 1-for-80 reverse split applied to its own history under the
        current-basis convention: correct, and declared with --own-split DATE:FACTOR (with its source
        in the run record) rather than guessed by the tool.
      * A FOREIGN action (PARA 1/6, IPW 72x, SKK 10x) is uniform across the whole kept half, so it
        shows on the pre-window sessions against the 2026-07-13 snapshot and on the last kept
        session against its anchor - both of which must be EXACT.
    Rule per row: pre-window and explicit anchors and the LAST kept session: close within 0.05 %,
    volume exact. Window sessions: expected = product of declared own-split factors dated after the
    session; ratio/expected must lie in DIV_BAND with volume exact (expected 1) or within 10 % of
    anchor/expected (an own split rounds per-minute volumes). Returns (rows, ok); each row:
    (day, source, our_close, anchor_close, ratio, our_vol, anchor_vol, ok)."""
    kept_days = sorted(d for d in set(frame["datetime"].dt.date) if d < cut)
    last_kept = kept_days[-1] if kept_days else None
    win = [d for d in kept_days if WINDOW[0] <= d <= WINDOW[1]
           and os.path.exists(os.path.join(CS_ROOT, d.strftime("%Y%m%d"), f"trades_{d.strftime('%Y%m%d')}.csv"))]
    pre = [d for d in kept_days if d < WINDOW[0]]
    rows = []
    sample = _pick(win, k_window)
    if last_kept in win and last_kept not in sample:
        sample.append(last_kept)
    for d in sample:
        expected = 1.0
        for sd, f in own_splits:
            if d < sd:
                expected *= f
        anc = _print_anchor(d, ticker, cs_symbol)
        ours = _session_stats(frame, d)
        rows.append(_cmp(d, "prints" if d != last_kept else "prints/last", ours, anc,
                         expected=expected, exact=(d == last_kept)))
    snap_path = os.path.join(SNAP_0713, "raw", f"{ticker}.parquet")
    if pre and os.path.exists(snap_path):
        snap = pd.read_parquet(snap_path)
        snap["datetime"] = pd.to_datetime(snap["datetime"])
        snap_days = set(snap["datetime"].dt.date)
        pre_sample = _pick([d for d in pre if d in snap_days], k_pre)
        if last_kept in pre and last_kept in snap_days and last_kept not in pre_sample:
            pre_sample.append(last_kept)
        for d in pre_sample:
            rows.append(_cmp(d, "snapshot-0713", _session_stats(frame, d), _session_stats(snap, d)))
    elif pre:
        rows.append((None, "snapshot-0713", None, None, None, None, None, False))
    for d, close, vol in anchors:
        rows.append(_cmp(d, "anchor", _session_stats(frame, d), (close, vol, None)))
    ok = bool(rows) and all(r[-1] for r in rows)
    return rows, ok


def _cmp(d, source, ours, anc, expected=1.0, exact=True):
    if ours is None or anc is None:
        return (d, source, ours[0] if ours else None, anc[0] if anc else None, None,
                ours[1] if ours else None, anc[1] if anc else None, False)
    ratio = ours[0] / anc[0] if anc[0] else float("nan")
    if not math.isfinite(ratio):
        return (d, source, ours[0], anc[0], ratio, ours[1], anc[1], False)
    if exact or source in ("snapshot-0713", "anchor"):
        ok = abs(ratio - 1) <= CLOSE_TOL and ours[1] == anc[1]
    else:
        r_adj = ratio / expected
        if expected == 1.0:
            vol_ok = ours[1] == anc[1]
        else:
            # Under an own split the served history's volume basis is LOSSY: VRM's pre-2024-02
            # minutes agree with round(print/80) within one share on only 73-85 % of minutes
            # (coverage 100 %) and the session sums sit 4-19 % below prints/80 - rounded at a
            # finer grain than the minute when the split was applied. Price at exactly the declared
            # factor is the test (a foreign split on top would move it by >= 2x); volume is a coarse
            # band on the session sum, with the per-minute figures printed for the record.
            frac, cover = _minute_volume_match(ours[3], anc[3], expected) if (len(ours) > 3 and len(anc) > 3 and anc[3]) else (0.0, 0.0)
            vol_ratio = ours[1] * expected / anc[1] if anc[1] else float("nan")
            vol_ok = math.isfinite(vol_ratio) and 0.60 <= vol_ratio <= 1.05 and cover >= 0.90
            source = f"{source} /{expected:g} sum x{vol_ratio:.3f} min {frac:.0%}/{cover:.0%}"
        ok = DIV_BAND[0] <= r_adj <= DIV_BAND[1] and vol_ok
    return (d, source, ours[0], anc[0], ratio, ours[1], anc[1], ok)


def _print_gate(rows, title):
    print(f"  {title}")
    for d, src, oc, ac, r, ov, av, ok in rows:
        rs = f"x{r:.5f}" if r is not None and math.isfinite(r) else "n/a"
        note = ""
        if ok and r is not None and math.isfinite(r) and src.startswith("prints") and r < 0.9995:
            note = "  (dividend-adjusted window session)"
        print(f"    {str(d):10} {src:14} close ours {oc} vs {ac} ({rs})  volume ours {ov} vs {av} -> {'OK' if ok else 'FAIL'}{note}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker")
    ap.add_argument("--cut", required=True, help="first session of the NEW owner; every bar dated >= this is dropped")
    ap.add_argument("--rebuild-from-cs", default=None, help="IEX symbol of the ORIGINAL company in trades_cs_<ymd>.csv (GOLD: B)")
    ap.add_argument("--until", default=None, help="last session to rebuild (the backfill window ends 2026-03-27)")
    ap.add_argument("--verify-against", default=None, help="Yahoo symbol of the ORIGINAL company for the rebuilt range (GOLD: B)")
    ap.add_argument("--unscale", type=float, default=None, help="undo a later owner's split applied to the kept half: price x F (4 dp), volume / F exact (PARA: 6)")
    ap.add_argument("--kept-from", default=None, help="take the kept half (bars before the cut) from this pre-repair snapshot dir (IPW, SKK)")
    ap.add_argument("--anchor", action="append", default=[], help="DATE:CLOSE:VOLUME the kept half must show (session last close, session volume); repeatable")
    ap.add_argument("--basis-samples", type=int, default=4, help="window sessions checked against the retained prints (default 4; pre-window: 3 vs the 2026-07-13 snapshot)")
    ap.add_argument("--own-split", action="append", default=[], help="DATE:FACTOR - a split of the ORIGINAL instrument inside the kept half (VRM 1-for-80 in Feb 2024: 2024-02-14:80); window sessions before DATE are expected at FACTOR x the raw prints. Cite the source in the run record.")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--snapshot-dir", default=None)
    ap.add_argument("--allow-queued", action="store_true")
    a = ap.parse_args()
    t = a.ticker.upper()
    cut = dt.date.fromisoformat(a.cut)
    until = dt.date.fromisoformat(a.until) if a.until else None
    if bool(a.rebuild_from_cs) != bool(a.until):
        print("--rebuild-from-cs and --until go together"); return 5
    anchors = []
    for s in a.anchor:
        d, c, v = s.split(":")
        anchors.append((dt.date.fromisoformat(d), float(c), int(v)))
    own_splits = []
    for s in a.own_split:
        d, f = s.split(":")
        own_splits.append((dt.date.fromisoformat(d), float(f)))
    if own_splits:
        print(f"  declared own split(s) of the original instrument: {[(str(d), f) for d, f in own_splits]}")
    client = get_client()

    raw = download_parquet(client, "raw", t)
    clean = download_parquet(client, "clean", t)
    if raw is None or raw.empty or clean is None or clean.empty:
        print(f"{t}: served raw/clean 1-minute file missing - aborted before any write"); return 5
    raw["datetime"] = pd.to_datetime(raw["datetime"]); clean["datetime"] = pd.to_datetime(clean["datetime"])
    rd, cd = raw["datetime"].dt.date, clean["datetime"].dt.date
    n_raw_drop, n_clean_drop = int((rd >= cut).sum()), int((cd >= cut).sum())
    first_drop = rd[rd >= cut].min() if n_raw_drop else None
    last_keep = rd[rd < cut].max()
    print(f"{t}: served raw {len(raw):,} bars, clean {len(clean):,}; cut {cut}: dropping raw {n_raw_drop:,} / clean {n_clean_drop:,} bars "
          f"({first_drop}..{rd.max()}); last kept session {last_keep}")
    if a.kept_from:
        try:
            raw_keep = _load_snapshot_bars(a.kept_from, "raw", t)
            clean_keep = _load_snapshot_bars(a.kept_from, "clean", t)
        except FileNotFoundError as e:
            print(f"{t}: --kept-from snapshot object missing: {e} - aborted before any write"); return 5
        raw_keep = raw_keep[raw_keep["datetime"].dt.date < cut].copy()
        clean_keep = clean_keep[clean_keep["datetime"].dt.date < cut].copy()
        print(f"  kept half taken from {a.kept_from}: raw {len(raw_keep):,} / clean {len(clean_keep):,} bars before the cut "
              f"(served kept half: raw {int((rd < cut).sum()):,} / clean {int((cd < cut).sum()):,})")
        if raw_keep.empty or clean_keep.empty:
            print("  the snapshot holds no bars before the cut - aborted before any write"); return 5
    else:
        raw_keep = raw[rd < cut].copy()
        clean_keep = clean[cd < cut].copy()
    if a.unscale:
        try:
            raw_keep = _unscale(raw_keep, a.unscale)
        except SystemExit as e:
            print(f"  {e}"); return 5
        # THE CLEAN HALF IS NOT UNSCALED IN PLACE (R736). The served clean set is the daily path's FULL
        # re-clean at the foreign basis (the split detector forces is_backfill=True on a rescale, and
        # clean_bars(served raw) reproduces PARA's 1,802,154 served clean bars exactly). Rescaling that
        # set back keeps the foreign-basis DECISIONS about which bars exist: 58,582 pre-detector bars
        # would stay missing against the 2026-07-13 clean. So: the pre-window clean is the 2026-07-13
        # snapshot's clean - the pre-detector legacy clean, kept verbatim by the backfill outside its
        # window (STI's and GOLD's served pre-window cleans equal it exactly) - and the window clean is a
        # fresh clean_bars() over the unscaled window raw at the ORIGINAL basis, what the pipeline would
        # have produced had the detector never fired (the pre-detector window clean itself is not
        # recoverable: the 0713 window half is the PRE-backfill series). Counts printed for the record.
        snap_clean_path = os.path.join(SNAP_0713, "clean", f"{t}.parquet")
        if not os.path.exists(snap_clean_path):
            print(f"  --unscale needs the 2026-07-13 clean snapshot for the pre-window clean: {snap_clean_path} missing - "
                  f"aborted before any write"); return 5
        clean_0713 = pd.read_parquet(snap_clean_path); clean_0713["datetime"] = pd.to_datetime(clean_0713["datetime"])
        pre_clean = clean_0713[clean_0713["datetime"].dt.date < WINDOW[0]].copy()
        win_raw = raw_keep[raw_keep["datetime"].dt.date >= WINDOW[0]]
        inplace_would_keep = int((clean_keep["datetime"].dt.date < WINDOW[0]).sum())
        if win_raw.empty:
            win_clean = pre_clean.iloc[0:0]
        else:
            context = pre_clean.tail(daily_update.CONTEXT_BARS)
            to_clean = pd.concat([context, win_raw[context.columns.intersection(win_raw.columns)]], ignore_index=True)
            cleaned = clean_bars(to_clean)
            win_clean = cleaned[cleaned["datetime"] > context["datetime"].max()]
        clean_keep = pd.concat([pre_clean, win_clean], ignore_index=True).drop_duplicates(subset=["datetime"], keep="last") \
                       .sort_values("datetime").reset_index(drop=True)
        print(f"  kept half UNSCALED x{a.unscale:g} on price (4 dp) and /{a.unscale:g} on volume - RAW in place; CLEAN rebuilt: "
              f"pre-window from the 2026-07-13 clean ({len(pre_clean):,} bars; an in-place unscale of the served clean would have "
              f"kept only {inplace_would_keep:,}), window re-cleaned at the original basis ({len(win_clean):,} bars from {len(win_raw):,} raw)")

    rebuilt = pd.DataFrame()
    if a.rebuild_from_cs:
        rows, days_with, days_without = [], 0, []
        for d in _sessions(cut, until):
            r = _bars_from_cs(d, a.rebuild_from_cs, t)
            if r:
                rows += r; days_with += 1
            else:
                days_without.append(d)
        rebuilt = pd.DataFrame(rows)
        if not rebuilt.empty:
            # build_bars stamps minute_start tz-aware in America/New_York; the served parquet
            # carries naive New York wall time (last bar 15:59:00). tz_localize(None) keeps the
            # wall clock and drops the zone, which is exactly the served convention.
            rebuilt["datetime"] = pd.to_datetime(rebuilt["datetime"], utc=True).dt.tz_convert("America/New_York").dt.tz_localize(None)
            rebuilt = rebuilt.sort_values("datetime").reset_index(drop=True)
        print(f"  rebuild {t} from IEX '{a.rebuild_from_cs}' prints {cut}..{until}: {len(rebuilt):,} minute bars over {days_with} sessions; "
              f"{len(days_without)} weekday(s) without prints/file: {[str(x) for x in days_without[:8]]}{'...' if len(days_without) > 8 else ''}")
        if rebuilt.empty:
            print("  rebuild produced no bars - refusing (check E:/iex_hist_backfill and the symbol); aborted before any write"); return 5

    if not rebuilt.empty:
        new_raw = pd.concat([raw_keep, rebuilt[raw_keep.columns.intersection(rebuilt.columns)]], ignore_index=True) \
                    .sort_values("datetime").reset_index(drop=True)
        # clean the rebuilt bars exactly as merge_ticker cleans an incremental day
        context = clean_keep.tail(daily_update.CONTEXT_BARS)
        to_clean = pd.concat([context, rebuilt[context.columns.intersection(rebuilt.columns)]], ignore_index=True)
        cleaned = clean_bars(to_clean)
        new_rows = cleaned[cleaned["datetime"] > context["datetime"].max()]
        new_clean = pd.concat([clean_keep, new_rows], ignore_index=True).drop_duplicates(subset=["datetime"], keep="last") \
                      .sort_values("datetime").reset_index(drop=True)
        print(f"  clean: {len(new_rows):,} of {len(rebuilt):,} rebuilt bars survive the cleaner")
    else:
        new_raw, new_clean = raw_keep.sort_values("datetime").reset_index(drop=True), clean_keep.sort_values("datetime").reset_index(drop=True)
    # THE SERVED CONTRACT (R740): seven columns, in this order, on both files - the 2026-07-13 clean
    # snapshot brings legacy flag columns along, and the window raw brings 'ticker'; neither may reach R2
    try:
        new_raw, new_clean = _standard(new_raw, "raw frame"), _standard(new_clean, "clean frame")
    except SystemExit as e:
        print(f"  {e}"); return 5
    assert list(new_raw.columns) == STANDARD_COLS and list(new_clean.columns) == STANDARD_COLS
    print(f"  after: raw {len(new_raw):,} bars (last {new_raw['datetime'].max()}), clean {len(new_clean):,} bars (last {new_clean['datetime'].max()}); "
          f"columns {list(new_raw.columns)} on both")
    print(f"  bar-count delta for the record (metadata.json counters are increment-only): raw {len(new_raw) - len(raw):+,}  clean {len(new_clean) - len(clean):+,}")

    # THE BASIS GATE - on the frame that would be uploaded, in the dry run as well
    gate_rows, gate_ok = basis_gate(new_raw, t, cut, anchors, a.basis_samples, 3, own_splits, a.rebuild_from_cs)
    _print_gate(gate_rows, f"basis gate ({len(gate_rows)} check(s)) -> {'OK' if gate_ok else 'FAIL'}")
    if not gate_ok:
        print(f"  REFUSED: the kept half is not on the original instrument's basis (or an anchor is unreachable) - "
              f"nothing written. A later owner's corporate action was applied to it (R732: PARA 1/6, IPW 72x, SKK 10x); "
              f"pass --unscale F or --kept-from SNAPDIR and re-run"); return 2
    # clean must sit on the same basis as raw on the anchor sessions (with --unscale the clean is
    # REBUILT - pre-window from the 2026-07-13 clean, window re-cleaned - so this is the check that
    # the rebuilt clean and the unscaled raw agree; without --unscale both halves are the served ones).
    # SAME MINUTE, not "session close": the cleaner drops closing minutes (PARA's clean session ends
    # 15:55, raw 15:59 - 11.01 vs 11.07 is timing, not basis).
    for d, _c, _v in anchors:
        c_day = new_clean[new_clean["datetime"].dt.date == d].sort_values("datetime")
        if c_day.empty:
            # refuse, never skip (AR-037 item iii, R503's class): an anchor session with no clean bars is
            # itself something to explain before writing
            print(f"  REFUSED: the anchor session {d} has no bars in the clean frame - explain before writing"); return 2
        last_dt = c_day["datetime"].iloc[-1]
        r_same = new_raw.loc[new_raw["datetime"] == last_dt, "Close"]
        if r_same.empty:
            print(f"  REFUSED: clean's last bar {last_dt} on {d} has no raw bar at the same minute"); return 2
        if abs(float(c_day["Close"].iloc[-1]) / float(r_same.iloc[0]) - 1) > CLOSE_TOL:
            print(f"  REFUSED: clean {float(c_day['Close'].iloc[-1])} vs raw {float(r_same.iloc[0])} at {last_dt} - "
                  f"the two files are on different bases"); return 2
    # FULL-FRAME EQUALITY of the pre-window half against the 2026-07-13 snapshot, raw and clean (R736).
    # Named tautology (R740): with --unscale the pre-window CLEAN is taken from that very snapshot, so
    # its equality proves only that the projection and concatenation kept it intact; the raw-side
    # equality is the real test there (the unscale arithmetic against the pre-detector raw).
    for version, frame in (("raw", new_raw), ("clean", new_clean)):
        eq = pre_window_equality(frame, version, t, cut)
        if eq is None:
            print(f"  pre-window equality vs 2026-07-13 {version}: no snapshot file for {t} - REFUSED, no oracle for that half"); return 2
        n_f, n_s, n_b, mism, only_f, only_s = eq
        # equal COUNTS too (R740): a duplicated minute merges onto one snapshot bar and left the
        # mismatch counters at zero
        ok_eq = mism == 0 and only_f == 0 and only_s == 0 and n_f == n_s == n_b
        print(f"  pre-window equality vs 2026-07-13 {version}: ours {n_f:,} / snapshot {n_s:,} / common {n_b:,}; "
              f"mismatched {mism:,}; only-ours {only_f:,}; only-snapshot {only_s:,} -> {'OK' if ok_eq else 'FAIL'}")
        if not ok_eq:
            print(f"  REFUSED: the pre-window {version} half is not the pre-detector set bar for bar - nothing written"); return 2
    if not a.apply:
        print("(dry run - pass --apply to write)"); return 0

    st = seam_rebase.daily_run_state()
    if st == "in_progress" or (st == "queued" and not a.allow_queued) or st == "unknown":
        print(f"  REFUSED: Daily Data Update workflow is {st}; a repair inside its window is overwritten"); return 2
    snap_dir = a.snapshot_dir or os.path.join("F:\\", f"hf_r2_snapshot_reassigned_{dt.datetime.now(dt.timezone.utc):%Y%m%d}", t)
    n_snap = seam_rebase.snapshot(client, t, snap_dir)          # exits 5 on any pre-write failure
    print(f"  snapshot: {n_snap} objects -> {snap_dir} (size + MD5/ETag verified)")

    n = 0; sync_failed = []
    seam_rebase._STATE["wrote"] = True                       # from here an escape is exit 4, never 5 (R738/R740)
    stale = []
    unverifiable = None
    try:
        for version, df in (("raw", new_raw), ("clean", new_clean)):
            upload_parquet(client, df, version, t, "1min"); upload_csv(client, df, version, t, "1min"); n += 2
            aggs = aggregate_all(df)
            for tf in TIMEFRAMES:
                if tf in aggs and not aggs[tf].empty:
                    upload_parquet(client, aggs[tf], version, t, tf); n += 1
            for attempt in (1, 2):
                try:
                    sync_ticker_variables(client, version, t, df, force_full=True); n += 2; break
                except Exception as ex:                      # noqa: BLE001
                    if attempt == 2:
                        sync_failed.append(f"{version}: {str(ex)[:120]}")
                        stale += [f"{version}/variables/{t}.parquet", f"{version}/quality/{t}.parquet"]
        print(f"  uploaded {n} objects" + (f"; variables sync FAILED for {sync_failed}" if sync_failed else ""))

        # VERIFY from the served side - inside the try (R732 item 5); an R2 READ failure here is
        # Unverifiable (exit 3, nothing restored - R736), a missing object or a logic error restores.
        d2 = _served_read(client, "raw", t, "daily"); c2 = _served_read(client, "clean", t, "daily")
        rebuilt_days = set(rebuilt["datetime"].dt.date) if not rebuilt.empty else set()
        stray_r = sorted(d for d in set(d2["datetime"].dt.date) if d >= cut and d not in rebuilt_days)
        stray_c = sorted(d for d in set(c2["datetime"].dt.date) if d >= cut and d not in rebuilt_days)
        ok_a = not stray_r and not stray_c
        ok_b = True; matched = total = 0
        if a.verify_against and rebuilt_days:
            try:
                y = _yahoo_close(a.verify_against, min(rebuilt_days), max(rebuilt_days))
            except Exception as ex:                          # noqa: BLE001
                unverifiable = f"Yahoo fetch for {a.verify_against} failed: {type(ex).__name__}: {str(ex)[:160]}"
                y = pd.Series(dtype=float)
            if unverifiable is None and y.empty:
                unverifiable = f"Yahoo returned no sessions for {a.verify_against}"
            dd2 = d2.set_index(d2["datetime"].dt.date)["Close"]
            for d in sorted(rebuilt_days):
                if d in y.index and d in dd2.index:
                    total += 1
                    matched += int(abs(float(dd2[d]) / float(y[d]) - 1) <= 0.01)
            ok_b = unverifiable is None and total > 0 and matched / total >= 0.95
        raw_srv = _served_read(client, "raw", t); clean_srv = _served_read(client, "clean", t)
        expected_last = max(rebuilt_days) if rebuilt_days else last_keep
        ok_c = set(clean_srv["datetime"]).issubset(set(raw_srv["datetime"])) and raw_srv["datetime"].max().date() == expected_last \
            and len(raw_srv) == len(new_raw) and len(clean_srv) == len(new_clean)
        # (d) the anchors and the basis samples on the SERVED file - what a user downloads
        srv_rows, ok_d = basis_gate(raw_srv, t, cut, anchors, a.basis_samples, 3, own_splits, a.rebuild_from_cs)
        # (e) REBUILT sessions on the served file against the class-share prints they came from (R736):
        #     (b) tests them against Yahoo at 1 %; this is the exact test, and it holds on exit 3 too.
        ok_e = True; e_rows = []
        if rebuilt_days and a.rebuild_from_cs:
            # EVERY rebuilt session on the served file vs the in-memory rebuilt frame, all columns (AR-037
            # item ii): the frame is already here, so the whole window costs nothing to compare
            srv_win = raw_srv[raw_srv["datetime"].dt.date >= cut][STANDARD_COLS].sort_values("datetime").reset_index(drop=True)
            exp_win = rebuilt[STANDARD_COLS].sort_values("datetime").reset_index(drop=True)
            full_ok = len(srv_win) == len(exp_win) and srv_win["datetime"].equals(exp_win["datetime"]) and all(
                np.allclose(srv_win[c].astype(float), exp_win[c].astype(float), rtol=0, atol=1e-9) for c in ("Open", "High", "Low", "Close", "Volume"))
            e_rows.append(("all rebuilt sessions", (len(exp_win),), (len(srv_win),), full_ok)); ok_e = ok_e and full_ok
            # plus a re-parse of the class-share prints on --basis-samples sessions (independent of the frame)
            for d in _pick(sorted(rebuilt_days), max(1, a.basis_samples)):
                bars = _bars_from_cs(d, a.rebuild_from_cs, t)
                srv = _session_stats(raw_srv, d)
                if not bars or srv is None:
                    e_rows.append((d, None, srv[0] if srv else None, False)); ok_e = False; continue
                exp_c, exp_v, exp_n = float(bars[-1]["Close"]), int(sum(b["Volume"] for b in bars)), len(bars)
                ok_row = abs(srv[0] / exp_c - 1) <= CLOSE_TOL and srv[1] == exp_v and srv[2] == exp_n
                e_rows.append((d, (exp_c, exp_v, exp_n), (srv[0], srv[1], srv[2]), ok_row)); ok_e = ok_e and ok_row
        print(f"  VERIFY (a) served daily bars dated >= {cut} that are not rebuilt: raw {len(stray_r)} {stray_r[:4]} clean {len(stray_c)} -> {'OK' if ok_a else 'MISMATCH'}")
        print(f"  VERIFY (b) rebuilt sessions vs Yahoo {a.verify_against}: {matched}/{total} within 1 % -> "
              f"{'OK' if ok_b else ('n/a' if not (a.verify_against and rebuilt_days) else ('UNVERIFIABLE' if unverifiable else 'MISMATCH'))}")
        print(f"  VERIFY (c) served clean ⊆ raw, raw ends {raw_srv['datetime'].max().date()} (expected {expected_last}), "
              f"bar counts raw {len(raw_srv):,}/{len(new_raw):,} clean {len(clean_srv):,}/{len(new_clean):,} -> {'OK' if ok_c else 'MISMATCH'}")
        _print_gate(srv_rows, f"VERIFY (d) basis gate on the SERVED 1-minute file -> {'OK' if ok_d else 'MISMATCH'}")
        if e_rows:
            print(f"  VERIFY (e) served rebuilt sessions vs the class-share prints ({len(e_rows)} sampled) -> {'OK' if ok_e else 'MISMATCH'}")
            for d, exp, got, okr in e_rows:
                print(f"    {d} prints (close, volume, bars) {exp} vs served {got} -> {'OK' if okr else 'FAIL'}")
        verified = bool(ok_a and ok_c and ok_d and ok_e and (ok_b or unverifiable))
    except Unverifiable as ex:
        # keep an earlier Yahoo failure text beside the read-back failure (AR-037 item v)
        unverifiable = (unverifiable + " | " if unverifiable else "") + str(ex); verified = None
    except BaseException as ex:                              # noqa: BLE001
        # RESTORE FIRST, RECORD SECOND, PRINT LAST (R735/R738 applied here by R740): a print into a
        # dead console raises before the restore runs. Every print on this path is seam_rebase._say.
        why = f"{type(ex).__name__}: {str(ex)[:200]}"
        try:
            n_back = seam_rebase.restore(client, snap_dir)
        except BaseException as ex2:                         # noqa: BLE001
            seam_rebase._record(snap_dir, f"EXIT 4 RESTORE FAILED after {n} upload(s); cause {why}; restore error {type(ex2).__name__}: {str(ex2)[:200]}")
            seam_rebase._say(f"  FAILED after the snapshot with {n} object(s) uploaded ({why}) and the RESTORE FAILED "
                             f"({type(ex2).__name__}: {str(ex2)[:200]}) - run: python seam_rebase.py {t} --restore \"{snap_dir}\""); return 4
        seam_rebase._record(snap_dir, f"EXIT 1 RESTORED {n_back} objects after {n} upload(s); cause {why}")
        seam_rebase._say(f"  FAILED after the snapshot with {n} object(s) uploaded ({why}) - restored {n_back} objects; "
                         f"served state is the pre-repair state"); return 1
    if verified is None:
        # a served object could not be read back (an R2 error, not a 404): the writes are not known
        # wrong, nothing is restored, a human re-verifies. The stale-variables list is printed FIRST
        # so an exit 3 never hides an exit 6 (R736).
        seam_rebase._record(snap_dir, f"EXIT 3 UNVERIFIABLE (read-back failed): {unverifiable[:200]}" + (f"; STALE {stale}" if sync_failed else ""))
        if sync_failed:
            seam_rebase._say(f"  variables/quality sync failed: {sync_failed}. STALE OBJECTS: {stale}")
        seam_rebase._say(f"  UNVERIFIABLE, DATA LIVE: the served objects could not be read back for verification - {unverifiable}. "
                         f"Nothing restored; re-run the verification for {t} before calling this complete; snapshot kept at {snap_dir}"); return 3
    if not verified:
        try:
            n_back = seam_rebase.restore(client, snap_dir)
        except BaseException as e:                           # noqa: BLE001
            seam_rebase._record(snap_dir, f"EXIT 4 NOT VERIFIED and RESTORE FAILED: {type(e).__name__}: {str(e)[:200]}")
            seam_rebase._say(f"  NOT VERIFIED and restore FAILED ({type(e).__name__}: {e}) - run: python seam_rebase.py {t} --restore \"{snap_dir}\""); return 4
        seam_rebase._record(snap_dir, f"EXIT 1 NOT VERIFIED - restored {n_back} objects")
        seam_rebase._say(f"  NOT VERIFIED - restored {n_back} objects from {snap_dir}; served state is the pre-repair state"); return 1
    if unverifiable:
        seam_rebase._record(snap_dir, f"EXIT 3 UNVERIFIABLE (market fetch): {unverifiable[:200]}" + (f"; STALE {stale}" if sync_failed else ""))
        if sync_failed:
            seam_rebase._say(f"  variables/quality sync failed: {sync_failed}. STALE OBJECTS: {stale}")
        seam_rebase._say(f"  UNVERIFIABLE, DATA LIVE: (a)(c)(d)(e) passed but (b) could not be measured - {unverifiable}. Nothing restored; "
                         f"re-run the Yahoo comparison for {t} vs {a.verify_against} before calling this complete; snapshot kept at {snap_dir}"); return 3
    if sync_failed:
        seam_rebase._record(snap_dir, f"EXIT 6 verified, variables sync failed: {sync_failed}; STALE {stale}")
        seam_rebase._say(f"  PRICES VERIFIED but variables/quality sync failed: {sync_failed}. STALE OBJECTS: {stale}. Not restoring; run "
                         f"sync_ticker_variables(client, version, '{t}', df, force_full=True) for each named version"); return 6
    seam_rebase._record(snap_dir, f"EXIT 0 DONE repaired cut={cut} unscale={a.unscale} kept_from={a.kept_from} rebuilt={len(rebuilt):,}")
    seam_rebase._say(f"  DONE: {t} repaired and verified; snapshot kept at {snap_dir}")
    return 0


def _guarded_main() -> int:
    """Exit 1 means 'written then restored' and nothing else (R738/R740): every upload sits inside
    main()'s guarded block, which returns 1/3/4/6 itself, so an exception escaping main() before
    the written flag is set happened before any write (5); after it, the served state is unknown
    (4). SystemExit with a numeric code (snapshot()'s 5) passes through."""
    st = seam_rebase._STATE
    try:
        return main()
    except SystemExit as ex:
        if isinstance(ex.code, int) or ex.code is None:
            raise
        seam_rebase._say(f"  {ex.code}")
        seam_rebase._say("  ABORTED before any write (exit 5)" if not st["wrote"] else "  ESCAPED AFTER WRITES - served state UNKNOWN (exit 4)")
        return 4 if st["wrote"] else 5
    except KeyboardInterrupt:
        if st["wrote"]:
            seam_rebase._say("  interrupted AFTER writes began and outside the guarded block - served state UNKNOWN; read _RESULT.txt - exit 4"); return 4
        seam_rebase._say("  interrupted before any write - exit 5"); return 5
    except BaseException as ex:                              # noqa: BLE001
        if st["wrote"]:
            seam_rebase._say(f"  ESCAPED AFTER WRITES by an unhandled {type(ex).__name__}: {str(ex)[:300]} - served state UNKNOWN; read _RESULT.txt - exit 4"); return 4
        seam_rebase._say(f"  ABORTED before any write by an unhandled {type(ex).__name__}: {str(ex)[:300]} - exit 5"); return 5


if __name__ == "__main__":
    sys.exit(_guarded_main())

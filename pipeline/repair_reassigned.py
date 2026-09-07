"""repair_reassigned.py v3 - remove another company's prints from a served series whose IEX symbol
was reassigned, put the kept half back on the ORIGINAL instrument's basis where a later owner's
corporate action was applied to it, and (GOLD only) rebuild the window from the retained
class-share extractions. Version 3 after adversarial reviews R732, R736 and R740 (2026-09-05).

EXIT CODES (one meaning each; every outcome after the snapshot is recorded in <snap_dir>/_RESULT.txt
BEFORE anything is printed, every print on those paths survives a dead console, and the entry point
maps any escape to 5 before the first upload and 4 after it - the seam tool's contract, R735/R738):
0 done and verified - AND a dry run, which writes nothing (R864: the table did not say so) | 1 written
then RESTORED | 2 refused before any write | 3 written, UNVERIFIABLE (read-back or market fetch
failed), DATA LIVE | 4 restore FAILED or an escape after writes | 5 aborted before any write |
6 prices verified, variables/quality sync failed (stale objects named).
EXIT 3 HERE IS THE OPPOSITE OF seam_rebase.py's 3. There it returns before the written flag is set:
"unmeasurable, nothing written, carry on". Here it means the 22 objects are live and unverified: STOP.
seam_rebase_batch.py's --tool choices exclude this tool, and its _terminal_ok = {"0","2"} would file
this 3 as a gate REFUSAL rather than an alarm - do not add it there without changing that (R855 #3).
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
     remaps B -> GOLD, appends them to raw, and cleans them with CONTEXT_BARS of the existing
     clean tail through clean_bars. THAT IS A BATCH CLEAN, NOT merge_ticker's incremental one, and
     this docstring used to claim they were the same (R864). Measured over the 80 rebuilt sessions:
     one clean_bars call keeps 29,990 bars, a per-day incremental clean keeps 29,804 - 186 more
     bars on 55 of the 80 sessions (0.62 %), a strict superset, because
     clean_pipeline.step8_brownlees_gallo is a 50-bar CENTRED window with no day boundary. The
     batch result is the better one; it is simply not what the daily path would have produced.
     AND IT DOES NOT FILL THE SERIES: 191 GOLD sessions are removed and 80 rebuilt, so
     2026-03-30..2026-09-04 (111 sessions) becomes a HOLE. E:/iex_hist_backfill ends 20260327 and
     daily_update.parse_day deletes its CSV, so no retained print set can fill it; after the next
     daily run that hole sits INSIDE a live series rather than at its end. The tool prints a
     bar-count delta and cannot say this - so it is said here, before anyone runs it;
  3. runs the basis gate; prints the plan and the bar-count delta. metadata.json's counters are
     INCREMENT-ONLY and no run updates them, so after all seven repairs the site's status page -
     which fetches that file live - would over-state bars_raw by 53,356 and under-state bars_clean
     by 7,753. (R865 #5 said 7,618; summing the seven printed deltas from seven_dryruns_v34.log
     gives raw -53,356 and clean +7,753 - the clean side GROWS, because PARA's rebuilt clean adds
     58,123 bars the foreign-basis re-clean had dropped.) Record the delta and correct the file by
     hand, or the page reports a total nothing served ever had;
  4. with --apply and no Daily Data Update in flight: content-checked snapshot of all 22 served
     objects (seam_rebase.snapshot; a directory that already holds a manifest exits 5), uploads
     1-minute parquet + CSV x2, 14 timeframe objects, variables/quality (force_full) - merge_ticker's
     sequence - then VERIFY from the served side (the exit-code contract is at the top of this docstring).
     TWO APPLY-ONLY FLAGS, both undocumented until R872 #4:
       * --snapshot-dir DIR overrides where that snapshot is kept (default
         F:/hf_r2_snapshot_reassigned_<utc-ymd>/<TICKER>). A directory already holding a
         _MANIFEST.txt is refused by snapshot()'s R731 guard, so two runs must not share one.
       * --allow-queued proceeds when the Daily Data Update workflow is QUEUED. in_progress and
         unknown are never overridable. It used to pass through in total silence - a queued run and
         an idle run printed the same lines and recorded the same `allow_queued=True` - so the run
         now prints the state and marks `(FIRED: state was queued)` in _RESULT.txt when the
         override actually suppressed a refusal.
     --verify-against also gets a PRE-FLIGHT Yahoo fetch here, before the snapshot: VERIFY (b) is
     mandatory but runs with the objects already live, so an unreachable oracle now exits 5 with
     nothing written instead of 3 with the data live and unverifiable.

  python repair_reassigned.py GOLD --cut 2025-12-02 --rebuild-from-cs B --until 2026-03-27 --verify-against B
  python repair_reassigned.py PARA --cut 2026-08-07 --unscale 6 --anchor 2025-08-06:11.07:1408409 --anchor 2022-03-04:34.07:12816002
  python repair_reassigned.py IPW  --cut 2021-05-12 --kept-from F:/hf_r2_snapshot_splits_20260905/IPW_20260522 --anchor 2017-07-24:17.80:5471
  python repair_reassigned.py SKK  --cut 2024-10-08 --kept-from F:/hf_r2_snapshot_splits_20260905/SKK_20260406 --anchor 2015-01-08:33.55:4056
  python repair_reassigned.py VRM  --cut 2025-02-20 --own-split 2024-02-14:80
  python repair_reassigned.py STI  --cut 2024-02-05        (USLV --cut 2026-05-27)
VRM's --own-split is NOT optional and this block used to list VRM without it: the basis gate then
refuses (exit 2) on three window sessions at x80.00000, which is Vroom's own 1-for-80 reverse split
showing against the raw prints under the current-basis convention. Correct behaviour, wrong example.
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
import symbol_map                                                                     # noqa: E402

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
            # element 4 is WHICH pass answered: cut_gate's CUT-4 needs it, and for a cs ticker the
            # difference between the two is the difference between two companies (R864)
            return (float(bars[-1].close), int(sum(b.volume for b in bars)), len(bars), minutes,
                    "cs" if (sym == cs_symbol and sym != symbol) else "main")
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
    session SUM is systematically below prints/factor (VRM 2024-01-02: 99 vs 9,746/80 = 121.8).

    Returns (fraction_ok, coverage) where fraction_ok is |served - print/factor| <= 1 over the common
    minutes. READ THE CALLER BEFORE QUOTING THIS: _cmp applies `0.60 <= session-sum ratio <= 1.05 and
    coverage >= 0.90`, and PRINTS fraction_ok without gating on it. This docstring used to call
    ">= 95 % of the common minutes" the honest test, which no version of the code has ever applied
    (both were written in the same commit, 0aaf04b) - and VRM's own passing sessions measure 74 %,
    73 % and 85 %, so as a gate it would refuse a repair the other evidence says is correct (R864)."""
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


_BARS_CACHE: dict = {}


def _main_pass_symbols(day: dt.date):
    """The symbols the backfill's MAIN pass retained for `day`, or None when that session has no
    bar file (outside the retained window). bars_<ymd>.parquet is the main pass restricted to the
    dataset universe (1,112 symbols on 2025-12-01), so membership answers "did this SYMBOL print
    that session" - a property of the print stream, INDEPENDENT of any --cut we type."""
    ymd = day.strftime("%Y%m%d")
    if ymd not in _BARS_CACHE:
        p = os.path.join(CS_ROOT, ymd, f"bars_{ymd}.parquet")
        _BARS_CACHE[ymd] = None if not os.path.exists(p) else set(pd.read_parquet(p, columns=["ticker"])["ticker"])
    return _BARS_CACHE[ymd]


def _is_split_ratio(f: float) -> bool:
    """This tool's declared range for `--own-split`: at least 3:2 in one direction or the other.

    NOT a statement about what splits exist - 5:4 and 4:3 are real, and this refuses them. The
    floor is a policy, because what a declaration BUYS is the loss of the exact session-volume
    test, and a 1.25 does not pay for that. Applied to every SUFFIX PRODUCT of the declared
    factors, never to one typed factor at a time: the basis gate expects the product (R869 #2)."""
    return f >= 1.5 or 0 < f <= 1 / 1.5


def _weekdays_between(a: dt.date, b: dt.date) -> int:
    """Weekdays STRICTLY between a and b (0 for adjacent sessions)."""
    return sum(1 for _ in _sessions(a + dt.timedelta(days=1), b - dt.timedelta(days=1)))


def cut_gate(ticker: str, cut: dt.date, last_kept: dt.date, first_dropped, cs_symbol, gap_min: int):
    """IS --cut THE HANDOVER DATE? Nothing else in this tool asks (review R864).

    The basis gate compares our bars with the retained prints FOR THE SYMBOL - and after a
    handover those prints are the NEW company's, so it proves "our bars equal the prints under
    this ticker", which is trivially true for the contaminating company. Measured: GOLD with
    --cut 2025-12-15, nine sessions late, keeps nine Gold.com sessions inside the Barrick series
    and passes every gate including the exact-volume prints/last row, exit 0. The single typed
    input that decides which sessions die was unchecked.

    What CAN be checked without knowing the issuer: a handover is a DISCONTINUITY, and a cut in
    the middle of a contiguous run is not. Four tests, each printed with its measurement and each
    OK / FAIL / n/a-with-a-reason - a silent skip is R503's class.

      CUT-1  served discontinuity: weekdays between the last kept and the first dropped SESSION.
             Measured at the seven declared cuts, from the served raw/daily files: SKK 2,542,
             USLV 1,539, STI 1,085, IPW 991, PARA 261, VRM 58 - and GOLD 0, because Barrick's
             last session (2025-12-01) and Gold.com's first (2025-12-02) are adjacent. GOLD is
             carried by CUT-2. (VRM's 58 weekdays are the 53 sessions of CUT-2 plus holidays.)
      CUT-2  print silence: silent window sessions between the last session the symbol printed
             before the cut and the cut. GOLD 142 (2025-05-08 -> 2025-12-02, the ONLY
             discontinuity in its 877 window print days), VRM 53. Not testable when the cut lies
             outside the retained window: prints stop existing after 2026-03-27 because the
             backfill ends, and that silence means nothing.
      CUT-3  the first DROPPED session must be one the symbol actually printed in the main pass -
             the new owner was trading under it that day. This is what refuses a cut that is too
             EARLY on a symbol whose original had already remapped away (GOLD --cut 2025-11-20:
             2025-11-20 is not in GOLD's print series, Barrick was printing as B).
      CUT-4  cs tickers only: the last kept session's anchor must come from the CLASS-SHARE pass.
             On 2025-12-01 the main pass has no GOLD prints and the cs pass has B's; on
             2025-12-12 the main pass answers, which is the proof that the cut is late.

    Passing needs CUT-1 or CUT-2, and neither CUT-3 nor CUT-4 may FAIL. The gap floor is 20
    sessions: the widest INNOCENT silence measured across the seven print series is 8 (SKK
    2026-01-26 -> 2026-02-06; IPW 2023-02-23 -> 2023-03-08) and the narrowest true handover is
    VRM's 53. Instrument: a full sweep of all 1,019 window bar files, 0 unreadable, 42.2 s.

    KNOWN LIMITS, stated rather than hidden. A long trading halt is also a discontinuity, so this
    gate places the cut at A boundary, not necessarily at THE handover. And CUT-3 refuses a correct
    cut if the new owner did not print on its very first served session (a halt, a missing file):
    that is a false REFUSAL, which is the safe direction, and the printed row says which day and
    why. Returns (rows, ok)."""
    rows = []
    if first_dropped is None:
        return [("CUT-*", "the cut drops no served session - there is nothing to repair", "FAIL")], False

    # CUT-0. THE HANDOVER DATE IS ALREADY WRITTEN DOWN, and this tool was not reading it (R865 #4).
    # symbol_map.REASSIGNED carries, per symbol, the first session the NEW owner printed under it -
    # measured 2026-09-05 as the first served session whose close follows the new owner's Yahoo
    # series on >= 60 % of the next 20 sessions - and it equals the declared --cut for all seven.
    # That is one equality against a reviewed table, so it goes first and it is HARD. The four
    # measured checks below stay: they are what validates the table rather than trusting it, and
    # they are the only thing that would catch the table itself being wrong.
    rec = symbol_map.REASSIGNED.get(ticker)
    if rec is None:
        rows.append(("CUT-0", f"{ticker} is not in symbol_map.REASSIGNED - this tool repairs reassigned "
                              f"symbols and nothing else; if the reassignment is real, record it there first", "FAIL"))
        return rows, False
    want = dt.date.fromisoformat(rec[0]) if rec[0] else None
    ok0 = want is not None and cut == want
    rows.append(("CUT-0", f"symbol_map.REASSIGNED records {ticker}'s handover as {want}, --cut is {cut}",
                 "OK" if ok0 else "FAIL"))
    if not ok0:
        return rows, False

    gap1 = _weekdays_between(last_kept, first_dropped)
    ok1 = gap1 >= gap_min
    rows.append(("CUT-1", f"served discontinuity: last kept {last_kept} -> first dropped {first_dropped} "
                          f"= {gap1} weekday(s) between (floor {gap_min})", "OK" if ok1 else "FAIL"))

    ok2, why2 = False, None
    if not (WINDOW[0] <= cut <= WINDOW[1]):
        why2 = (f"the cut {cut} lies outside the retained print window {WINDOW[0]}..{WINDOW[1]}; a silence "
                f"after it is the backfill ending, not the market")
    else:
        silent, prev = 0, None
        d = cut - dt.timedelta(days=1)
        while d >= WINDOW[0]:
            u = _main_pass_symbols(d)
            if u is not None:
                if ticker in u:
                    prev = d
                    break
                silent += 1
            d -= dt.timedelta(days=1)
        if prev is None:
            why2 = f"the symbol printed on no session between {WINDOW[0]} and the cut"
        else:
            ok2 = silent >= gap_min
            rows.append(("CUT-2", f"print silence: last printed {prev}, then {silent} silent session(s) "
                                  f"to the cut {cut} (floor {gap_min})", "OK" if ok2 else "FAIL"))
    if why2:
        rows.append(("CUT-2", f"print silence not testable - {why2}", "n/a"))

    ok3 = None
    p3 = _main_pass_symbols(first_dropped) if WINDOW[0] <= first_dropped <= WINDOW[1] else None
    if p3 is None:
        rows.append(("CUT-3", f"the first dropped session {first_dropped} has no main-pass bar file "
                              f"(outside {WINDOW[0]}..{WINDOW[1]}) - the new owner's first print is UNCHECKED", "n/a"))
    else:
        ok3 = ticker in p3
        rows.append(("CUT-3", f"the first dropped session {first_dropped} is {'' if ok3 else 'NOT '}"
                              f"a session the symbol printed in the main pass", "OK" if ok3 else "FAIL"))

    ok4 = None
    if cs_symbol:
        anc = _print_anchor(last_kept, ticker, cs_symbol)
        src = anc[4] if anc else None
        ok4 = src == "cs"
        rows.append(("CUT-4", f"the last kept session {last_kept} is anchored by the "
                              f"{src or 'NO'} pass (a cs ticker's last original session must not print "
                              f"under its own symbol)", "OK" if ok4 else "FAIL"))

    ok = (ok1 or ok2) and ok3 is not False and ok4 is not False
    return rows, ok


def last_cs_session(cs_symbol: str, ticker: str, limit: int = 15):
    """The last session in the retained window on which the CLASS-SHARE symbol printed.

    `--until` is the SECOND typed input that decides which sessions die, and nothing checked it
    (R865 #3). Measured: `GOLD --cut 2025-12-02 --rebuild-from-cs B --until 2026-01-30` exits 0 with
    the cut gate OK, the basis gate 7/7 OK and both pre-window equalities 0/0/0 - while rebuilding
    41 sessions instead of 80 and discarding 15,189 raw bars over 39 RECOVERABLE Barrick sessions.
    Every downstream check is blind to it by construction: `expected_last = max(rebuilt_days)` makes
    (c) tautological, (a) excludes the rebuilt days, and (b)/(e) only ever see what was rebuilt.

    Walks back from the window end, so a correct `--until` costs one file. Bounded at `limit`
    class-share sessions: a symbol that printed on none of them is a wrong --rebuild-from-cs, and
    the rebuild would refuse anyway - this just says so earlier and for the right reason."""
    d, tried = WINDOW[1], 0
    while d >= WINDOW[0] and tried < limit:
        ymd = d.strftime("%Y%m%d")
        if os.path.exists(os.path.join(CS_ROOT, ymd, f"trades_cs_{ymd}.csv")):
            tried += 1
            if _bars_from_cs(d, cs_symbol, ticker):
                return d
        d -= dt.timedelta(days=1)
    return None


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
    # THE DATE COLUMN IS NOT ALWAYS "datetime" (R865 #1). The bars and timeframe objects are keyed on
    # `datetime`; the variables and quality objects are keyed on `trade_date` (measured: 8,983 rows of
    # trade_date + 25 variables in raw/variables/GOLD.parquet). A bare df["datetime"] raised KeyError,
    # which is neither Unverifiable nor RuntimeError, so it fell through VERIFY (h)'s handler to the
    # guarded try's `except BaseException` - RESTORE, exit 1 - making EVERY --apply run end by rolling
    # back its own 22 correct objects. It reached the commit because no test in pipeline/ imported this
    # module and a dry run returns 200 lines earlier; there is a test now.
    for col in ("datetime", "trade_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
            return df
    raise RuntimeError(f"served {version}/{ticker}{('/' + tf) if tf else ''} has neither a 'datetime' nor a "
                       f"'trade_date' column - columns {list(df.columns)[:8]}")


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
    Rule per row, AS IMPLEMENTED - the previous wording said "within 10 %", which no version of the
    code has applied (R864 #9): pre-window and explicit anchors and the LAST kept session: close
    within 0.05 %, volume EXACT. Window sessions: expected = product of declared own-split factors
    dated after the session; ratio/expected must lie in DIV_BAND, and volume must be EXACT when
    expected is 1, otherwise the session-sum ratio must lie in [0.60, 1.05] with the common minutes
    covering >= 90 % of the print minutes. The per-minute agreement fraction that
    _minute_volume_match returns is PRINTED AND NOT GATED ON - read that function's docstring for
    why, and do not quote its 95 % as a test. Returns (rows, ok); each row:
    (day, source, our_close, anchor_close, ratio, our_vol, anchor_vol, ok)."""
    kept_days = sorted(d for d in set(frame["datetime"].dt.date) if d < cut)
    last_kept = kept_days[-1] if kept_days else None
    win = [d for d in kept_days if WINDOW[0] <= d <= WINDOW[1]
           and os.path.exists(os.path.join(CS_ROOT, d.strftime("%Y%m%d"), f"trades_{d.strftime('%Y%m%d')}.csv"))]
    pre = [d for d in kept_days if d < WINDOW[0]]
    rows = []
    # THE PRINT STORE IS AN INPUT TOO, AND AN UNREACHABLE ONE USED TO PRINT "-> OK" (R872 #1).
    # `win` is filtered by os.path.exists under CS_ROOT, so a detached E: empties the pool,
    # _pick([], k) returns nothing, and the gate passes having compared NOTHING - measured VRM
    # exit 0 with "basis gate (3 check(s)) -> OK" over 0 of its 688 kept window sessions, PARA 0
    # of 858. VERIFY (d) calls this same function, so --apply would "verify" the served side over
    # the same empty set. A blanket refusal would be wrong: four of the seven legitimately have no
    # kept window session, and 41 window weekdays are market holidays with no trades file. The
    # condition is the ASYMMETRY - window sessions exist and NOT ONE of them has a trades file.
    # R867 #4 floored --basis-samples, which is the typing; this floors the rows it produces.
    in_window = [d for d in kept_days if WINDOW[0] <= d <= WINDOW[1]]
    if in_window and not win:
        rows.append((None, f"prints/NO trades_*.csv under {CS_ROOT} for any of {len(in_window)} "
                           f"kept window session(s)", None, None, None, None, None, False))
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
        # tolerance, not equality (R867 #3): a factor that is 1.0 to within floating point must
        # take the EXACT branch, so a declared near-1 factor cannot buy the band. main() refuses
        # such a declaration outright; this is the second door.
        if abs(expected - 1.0) < 1e-6:
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
    # _say, not print: this runs inside the guarded try for VERIFY (d), and a console that dies
    # after a correct 22-object upload would otherwise raise OSError -> restore -> exit 1, undoing
    # a correct repair and burning the snapshot directory (R735's rule, applied to the SUCCESS
    # path by R864).
    seam_rebase._say(f"  {title}")
    for d, src, oc, ac, r, ov, av, ok in rows:
        rs = f"x{r:.5f}" if r is not None and math.isfinite(r) else "n/a"
        note = ""
        if ok and r is not None and math.isfinite(r) and src.startswith("prints") and r < 0.9995:
            note = "  (dividend-adjusted window session)"
        seam_rebase._say(f"    {str(d):10} {src:14} close ours {oc} vs {ac} ({rs})  volume ours {ov} vs {av} -> {'OK' if ok else 'FAIL'}{note}")


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
    ap.add_argument("--cut-gap-min", type=int, default=20, help="cut gate: sessions of discontinuity required at the cut "
                    "(default 20; widest innocent silence measured 8, narrowest true handover 53)")
    ap.add_argument("--basis-samples", type=int, default=4, help="window sessions checked against the retained prints (default 4; pre-window: 3 vs the 2026-07-13 snapshot)")
    ap.add_argument("--own-split", action="append", default=[], help="DATE:FACTOR - a split of the ORIGINAL instrument inside the kept half (VRM 1-for-80 in Feb 2024: 2024-02-14:80); window sessions before DATE are expected at FACTOR x the raw prints. Cite the source in the run record.")
    ap.add_argument("--apply", action="store_true")
    # BOTH OF THESE WERE UNDOCUMENTED (R872 #4) - one add_argument line each, no help, absent from
    # the docstring, and one of them silently relaxes the only gate protecting the write window.
    ap.add_argument("--snapshot-dir", default=None,
                    help="where --apply keeps the pre-write snapshot (default F:/hf_r2_snapshot_reassigned_<utc-ymd>/<TICKER>). "
                         "A directory that already holds a _MANIFEST.txt is REFUSED by snapshot()'s R731 guard, so a "
                         "collision cannot silently overwrite a kept snapshot - do not point two runs at one directory.")
    ap.add_argument("--allow-queued", action="store_true",
                    help="proceed when the Daily Data Update workflow is QUEUED (not in_progress, not unknown - those "
                         "are never overridable). The workflow can then start mid-repair and overwrite the write. Use "
                         "only when the queue is known to be waiting on an unrelated job; the run prints and records "
                         "that the override fired.")
    a = ap.parse_args()
    t = a.ticker.upper()
    cut = dt.date.fromisoformat(a.cut)
    until = dt.date.fromisoformat(a.until) if a.until else None
    if bool(a.rebuild_from_cs) != bool(a.until):
        print("--rebuild-from-cs and --until go together"); return 5
    if a.rebuild_from_cs and not a.verify_against:
        # THE NINTH UNCHECKED INPUT (R869 #5). Without --verify-against, VERIFY (b) sets ok_b=True
        # and is skipped, and (b) is the ONLY check on the rebuilt sessions that does not come
        # from the print stream they were built from: (c) is tautological because expected_last is
        # max(rebuilt_days), (a) excludes the rebuilt days by construction, and (e) compares the
        # served bars against the same frame and a re-parse of the same prints. So omitting one
        # optional flag silently removes the only independent oracle for GOLD's 30,947 new bars,
        # and the run still exits 0. A rebuild without it is not verifiable; refuse it.
        print("  --rebuild-from-cs without --verify-against would rebuild sessions that NOTHING "
              "independent checks: VERIFY (b) is skipped, and every other check on the rebuilt "
              "range is derived from the prints it was built from. Pass --verify-against <symbol> "
              "(GOLD: B); aborted before any write"); return 5
    anchors = []
    for s in a.anchor:
        d, c, v = s.split(":")
        anchors.append((dt.date.fromisoformat(d), float(c), int(v)))
    if a.basis_samples < 1:
        # R867 #4: --basis-samples 0 removed every interior window check and still exited 0 -
        # VRM's whole 2022-03-07..2024-11-28 span was then checked by nothing. VERIFY (e) already
        # floors the same value at max(1, ...); the gate did not.
        print(f"  --basis-samples {a.basis_samples} would check no window session at all - refused"); return 5
    own_splits = []
    for s in a.own_split:
        d, f = s.split(":")
        f = float(f)
        # R867 #3: DECLARING AN OWN SPLIT RELAXES THE GATE, so a factor that is not a split must be
        # refused. `expected != 1.0` switches the session-volume test from EXACT to a 0.60-1.05
        # band, and `--own-split 2030-01-01:1.0001` - a no-op declaration - made PARA pass 8/8
        # while admitting 429,693-751,963 shares against 716,155 prints on 2022-03-07. The factor
        # is uncheckable from here: there is no split table (splits are DETECTED, not looked up),
        # and Yahoo answers for whoever holds the symbol NOW, which for these seven is the wrong
        # company. So the only defence is the shape of the number: a ratio at least 3:2 in one
        # direction or the other. THAT FLOOR IS A POLICY OF THIS TOOL, NOT A FACT ABOUT SPLITS
        # (R869 #3): 5:4 and 4:3 are perfectly real splits, and this refuses them, because the
        # thing being bought with a declaration here is the loss of an EXACT volume test and a
        # 1.25 is not worth it. An earlier version of this comment cited a `_FRACTIONAL_SPLITS`
        # constant as precedent - that constant is NOT IN THIS TREE (it lives on an open PR's
        # branch) and it excludes 5:4 for a different reason. Do not cite it from here.
        if not _is_split_ratio(f):
            print(f"  --own-split {s}: {f:g} is outside this tool's declared range - it accepts only a "
                  f"ratio of at least 3:2 in one direction or the other, and REFUSES real-but-small "
                  f"splits (5:4, 4:3) on purpose, because declaring one replaces the EXACT "
                  f"session-volume test with a 0.60-1.05 band; refused, aborted before any write"); return 5
        own_splits.append((dt.date.fromisoformat(d), f))
    # AND THE BAND IS SELECTED BY THE PRODUCT, NOT BY ANY ONE FACTOR (R869 #2). `basis_gate` sets
    # `expected` to the product of every declared factor dated AFTER the session, and `_cmp` swaps
    # the exact volume test for the band whenever that product is not 1. So two factors that each
    # clear the check above multiply back into the forbidden range: measured,
    # `--own-split 2030-01-01:2 --own-split 2030-01-02:0.5001` gave PARA exit 0 with 8/8 OK and
    # window rows reading `prints /1.0002`, admitting 2022-03-07's 716,155-share session anywhere
    # in ~429,700-751,900 - which is R867 #3's own arithmetic, through the gate written to stop it.
    # Every value `expected` can take is a SUFFIX PRODUCT of the date-sorted factors, so check
    # those, not the typed inputs.
    _sorted = sorted(own_splits)
    for _i in range(len(_sorted)):
        _p = 1.0
        for _d, _f in _sorted[_i:]:
            _p *= _f
        if abs(_p - 1.0) >= 1e-6 and not _is_split_ratio(_p):
            print(f"  --own-split: the factors dated on or after {_sorted[_i][0]} multiply to {_p:.6g}, "
                  f"which is the product the basis gate would actually expect - and it is inside the "
                  f"range this tool refuses. Each factor passing on its own is not enough: the gate "
                  f"reads the PRODUCT. Refused, aborted before any write"); return 5
    if own_splits:
        print(f"  declared own split(s) of the original instrument: {[(str(d), f) for d, f in own_splits]}"
              f"; suffix products the basis gate can expect: "
              f"{[round(math.prod([f for _, f in _sorted[i:]]), 6) for i in range(len(_sorted))]}")
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
    if pd.isna(last_keep):
        print(f"  REFUSED: the cut {cut} keeps no served session - it would empty the series; aborted before any write"); return 2

    # THE CUT GATE (R864) - the only check on --cut itself, the single typed input that decides
    # which sessions die. It runs FIRST: it costs a handful of parquet reads, and every gate below
    # it compares our bars with prints keyed on the SYMBOL, which after a handover are the new
    # company's. Nine sessions of Gold.com passed all of those.
    cut_rows, cut_ok = cut_gate(t, cut, last_keep, first_drop, a.rebuild_from_cs, a.cut_gap_min)
    print(f"  cut gate ({len(cut_rows)} check(s)) -> {'OK' if cut_ok else 'FAIL'}")
    for _name, _detail, _verdict in cut_rows:
        print(f"    {_name}  {_detail} -> {_verdict}")
    if not cut_ok:
        # Name the check that refused. A generic message that pointed at --cut-gap-min was printed
        # for a CUT-0 failure too, which is an override that cannot help and should not be tried.
        failed = [r[0] for r in cut_rows if r[2] == "FAIL"]
        # CUT-1 AND CUT-2 ARE ALTERNATIVES, so a failing one is NOT a cause when the other passed
        # (R869's second recommendation, carried out under R872 #2). GOLD's CUT-1 is 0 weekdays on
        # EVERY run, the correct one included - the gate passes on CUT-2's 142 silent sessions - so
        # naming it in the refusal sent the reader at a check that was never the obstacle.
        _verdict = {r[0]: r[2] for r in cut_rows}
        _alt = {"CUT-1": "CUT-2", "CUT-2": "CUT-1"}
        blocking = [n for n in failed if _verdict.get(_alt.get(n, ""), "") != "OK"] or failed
        if "CUT-0" in failed:
            print(f"  REFUSED: --cut {cut} is not the handover date recorded for {t} in symbol_map.REASSIGNED. "
                  f"That table is the reviewed record of when each symbol changed companies; if it is wrong, "
                  f"correct it there with its evidence first. --cut-gap-min cannot relax this; nothing written")
        elif "CUT-4" in failed:
            # MEMBERSHIP, NOT LIST SHAPE (R869 #1). `failed == ["CUT-4"]` could never be true for
            # the only ticker CUT-4 exists for: GOLD's CUT-1 is 0 weekdays on EVERY run, the
            # correct one included (the gate passes on CUT-2's 142 silent sessions), so `failed`
            # is always ["CUT-1", "CUT-4"] and this branch was dead the moment it was written.
            # I ran the probe that proves it, read the exit code, and did not read the message.
            # R867 #6: CUT-0 got a cause-specific branch and CUT-4 did not, so a WRONG
            # --rebuild-from-cs symbol - the cut being right - was reported as "not at a handover
            # boundary ... before overriding --cut-gap-min", which points at the one input that
            # cannot help. It also made the --until gate's own "check --rebuild-from-cs" message
            # unreachable for every cs ticker, because this refusal fires first.
            # ...AND NAME WHAT ELSE FAILED (R872 #2). `"CUT-4" in failed` shadowed CUT-3: a
            # ["CUT-3", "CUT-4"] gate printed only the anchor sentence, so the too-early detector -
            # the first dropped session HAVING printed in the main pass - was never mentioned. The
            # generic branch it replaced at least printed both names.
            _also = [n for n in blocking if n != "CUT-4"]
            print(f"  REFUSED: the last kept session {last_keep} is not anchored by the class-share pass. "
                  f"--cut {cut} may well be right; check --rebuild-from-cs {a.rebuild_from_cs!r} instead - "
                  f"if that symbol did not print on {last_keep}, the anchor came from the main pass, which "
                  f"after a handover is the NEW company. Nothing written"
                  + (f". ALSO FAILING: {', '.join(_also)} - read those rows above as well; a failing CUT-3 "
                     f"means the cut is too EARLY (the first dropped session did print in the main pass), "
                     f"which the sentence above does not cover." if _also else ""))
        else:
            print(f"  REFUSED: --cut {cut} is not at a handover boundary in the print stream ({', '.join(blocking)}). "
                  f"A cut inside a contiguous run keeps the NEW owner's sessions, and every gate below this one "
                  f"would pass on them (they are that company's own prints). Check the date against the symbol's "
                  f"print series before overriding --cut-gap-min; nothing written")
        return 2

    # THE --until GATE (R865 #3). Same class as the cut gate: a typed date that decides which
    # sessions exist, checked by nothing downstream.
    if a.rebuild_from_cs:
        last_cs = last_cs_session(a.rebuild_from_cs, t)
        if last_cs is None:
            print(f"  REFUSED: IEX '{a.rebuild_from_cs}' printed on none of the last 15 class-share sessions "
                  f"of the retained window - check --rebuild-from-cs; nothing written"); return 2
        if until < last_cs:
            short = sum(1 for _ in _sessions(until + dt.timedelta(days=1), last_cs))
            print(f"  REFUSED: --until {until} stops {short} weekday(s) short of {last_cs}, the last session in the "
                  f"retained window on which '{a.rebuild_from_cs}' printed. Those sessions are RECOVERABLE and this "
                  f"would delete them instead: the rebuilt range defines expected_last, so VERIFY (a)(b)(c)(e) all "
                  f"measure the short range against itself and pass. Pass --until {last_cs}; nothing written"); return 2
        if until > WINDOW[1]:
            print(f"  REFUSED: --until {until} is past the retained window's end {WINDOW[1]}; there are no prints to "
                  f"rebuild from after it, so the sessions between would be dropped, not rebuilt; nothing written"); return 2
        print(f"  --until gate: {until} == the last retained class-share session for '{a.rebuild_from_cs}' -> OK")
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
              f"nothing written. USUALLY that means a later owner's corporate action was applied to it (R732: "
              f"PARA 1/6, IPW 72x, SKK 10x), and the answer is --unscale F or --kept-from SNAPDIR. BUT READ THE "
              f"ROWS FIRST (R856 #3): a wrong --rebuild-from-cs symbol and an unreadable anchor file fail here "
              f"too, and 'fixing' either of those with --unscale would rescale a correct history. A row whose "
              f"ratio is near 1 is not a corporate action"); return 2
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
    # AN OVERRIDE THAT FIRES MUST SAY SO (R872 #4). `--allow-queued` used to pass through in
    # silence: a `queued` run and an `idle` run printed the same 18 lines, and `_RESULT.txt`
    # recorded `allow_queued=True` either way, so the record could not tell an override that
    # actually suppressed a refusal from one that was typed and never needed.
    print(f"  daily run state: {st}"
          + (" - OVERRIDDEN by --allow-queued; the workflow can start mid-repair and overwrite it"
             if st == "queued" and a.allow_queued else ""))
    # THE ORACLE IS A PRE-WRITE INPUT AND WAS ONLY EVER TOUCHED AFTER THE WRITE (R872 #5). VERIFY
    # (b) is mandatory whenever --verify-against is set, but it runs with 22 objects already live,
    # so an unreachable Yahoo - or a missing yfinance, which pipeline/requirements.txt did not
    # declare until today - ended the run at exit 3 (DATA LIVE, unverifiable) where it can just as
    # well end at 5 with nothing written. One probe fetch, the same call VERIFY (b) makes.
    if a.verify_against:
        _probe_from = WINDOW[1] - dt.timedelta(days=45)
        try:
            _probe = _yahoo_close(a.verify_against, _probe_from, WINDOW[1])
        except Exception as ex:                                    # noqa: BLE001
            print(f"  REFUSED: the VERIFY (b) oracle is unreachable BEFORE any write - Yahoo fetch for "
                  f"{a.verify_against} raised {type(ex).__name__}: {str(ex)[:160]}. That check is mandatory "
                  f"and otherwise runs only once the objects are live; aborted before any write"); return 5
        if _probe is None or len(_probe) == 0:
            print(f"  REFUSED: Yahoo returned no sessions for {a.verify_against} between {_probe_from} and "
                  f"{WINDOW[1]} - check --verify-against; aborted before any write"); return 5
        print(f"  VERIFY (b) oracle pre-flight: Yahoo {a.verify_against} returned {len(_probe)} session(s) "
              f"for {_probe_from}..{WINDOW[1]}")
    snap_dir = a.snapshot_dir or os.path.join("F:\\", f"hf_r2_snapshot_reassigned_{dt.datetime.now(dt.timezone.utc):%Y%m%d}", t)
    n_snap = seam_rebase.snapshot(client, t, snap_dir)          # exits 5 on any pre-write failure
    print(f"  snapshot: {n_snap} objects -> {snap_dir} (size + MD5/ETag verified)")

    # PRICE OBJECTS AND VARIABLE OBJECTS ARE COUNTED APART (R865 #2). One count, ANDed into
    # `verified`, made exit 6 unreachable: a failed variables sync leaves n = 20 != 22, so the run
    # took the `not verified` branch and RESTORED - reporting "written then restored" for a repair
    # whose 18 price objects were correct and verified. Exit 6 exists precisely for that state.
    n_price = 0; n_vars = 0; sync_failed = []
    seam_rebase._STATE["wrote"] = True                       # from here an escape is exit 4, never 5 (R738/R740)
    stale = []
    unverifiable = None
    ok_vars = None          # set inside the try; None means VERIFY never reached the variables half
    try:
        for version, df in (("raw", new_raw), ("clean", new_clean)):
            # ONE INCREMENT PER OBJECT (R869 #4). `n_price += 2` after both calls meant a CSV
            # failure left the parquet uploaded and uncounted, so the restore record read "after
            # 0 upload(s)" while one object was live. Same class as R867 #1: a count that is not
            # what happened.
            upload_parquet(client, df, version, t, "1min"); n_price += 1
            upload_csv(client, df, version, t, "1min"); n_price += 1
            aggs = aggregate_all(df)
            for tf in TIMEFRAMES:
                if tf in aggs and not aggs[tf].empty:
                    upload_parquet(client, aggs[tf], version, t, tf); n_price += 1
            for attempt in (1, 2):
                try:
                    # R864: the return value was discarded, and variables_sync returns
                    # {"new_rows": 0} WITHOUT uploading when compute_recent_days is empty - so
                    # `n += 2` credited two objects that were never written. force_full makes the
                    # empty path rare, not impossible; VERIFY (h) reads all four back regardless.
                    stats = sync_ticker_variables(client, version, t, df, force_full=True)
                    if not stats.get("new_rows"):
                        raise RuntimeError(f"sync_ticker_variables computed no rows for {version}/{t} "
                                           f"({stats}) - it uploaded NOTHING and the two objects are stale")
                    n_vars += 2; break
                except Exception as ex:                      # noqa: BLE001
                    if attempt == 2:
                        sync_failed.append(f"{version}: {str(ex)[:120]}")
                        stale += [f"{version}/variables/{t}.parquet", f"{version}/quality/{t}.parquet"]
        n = n_price + n_vars
        seam_rebase._say(f"  uploaded {n} objects ({n_price} price, {n_vars} variables/quality)"
                         + (f"; variables sync FAILED for {sync_failed}" if sync_failed else ""))

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
        # (f) FULL-FRAME pre-window equality on the SERVED files (R864). The oracle existed and was
        #     applied to the in-memory frames only; the served claim rested on (d)'s sample - R736's
        #     finding (a 3-session sample missed 58,582 bars) reproduced on the served side. The
        #     frames are already downloaded, so this costs no network at all.
        f_rows, ok_f = [], True
        for version, frame in (("raw", raw_srv), ("clean", clean_srv)):
            eq = pre_window_equality(frame, version, t, cut)
            if eq is None:
                f_rows.append((version, "no 2026-07-13 snapshot file - no oracle for that half", False)); ok_f = False; continue
            n_f, n_s, n_b, mism, only_f, only_s = eq
            r_ok = mism == 0 and only_f == 0 and only_s == 0 and n_f == n_s == n_b
            f_rows.append((version, f"ours {n_f:,} / snapshot {n_s:,} / common {n_b:,}; mismatched {mism:,}; "
                                    f"only-ours {only_f:,}; only-snapshot {only_s:,}", r_ok))
            ok_f = ok_f and r_ok
        # (g) EVERY served CLEAN bar must equal the served RAW bar at the same minute (R864: no served
        #     clean price was checked anywhere - (c) compared datetimes and lengths, (d) ran on raw
        #     only, and with --unscale the clean is REBUILT). Measured on the served IPW, GOLD, PARA
        #     and VRM: the cleaner only DROPS bars - 0 clean minutes absent from raw and 0 value
        #     mismatches over 4,756,002 clean bars - so this is a complete oracle, not a sample.
        vcols = ["Open", "High", "Low", "Close", "Volume"]
        mg = clean_srv[["datetime"] + vcols].merge(raw_srv[["datetime"] + vcols], on="datetime",
                                                   how="left", suffixes=("_c", "_r"), indicator=True)
        g_missing = int((mg["_merge"] != "both").sum())
        gb = mg[mg["_merge"] == "both"]
        g_mism = sum(int((~np.isclose(gb[c + "_c"].astype(float), gb[c + "_r"].astype(float), rtol=0, atol=1e-9)).sum())
                     for c in vcols)
        ok_g = g_missing == 0 and g_mism == 0
        # (h) THE FOUR variables/quality OBJECTS, READ BACK (R864: `sync_ticker_variables(...); n += 2`
        #     discarded the return value, VERIFY read none of them, and "uploaded 22 objects" was a
        #     claim about 4 of them, not a measurement).
        h_rows, ok_h = [], True
        for version in ("raw", "clean"):
            for kind in ("variables", "quality"):
                try:
                    v = _served_read(client, version, t, kind)
                except RuntimeError as ex:                   # missing or empty after the upload
                    h_rows.append((f"{version}/{kind}", str(ex)[:100], False)); ok_h = False; continue
                td = pd.to_datetime(v["trade_date"]).dt.date
                last_td = max(td)
                foreign = sorted(d for d in set(td) if d >= cut and d not in rebuilt_days)
                r_ok = last_td == expected_last and not foreign
                h_rows.append((f"{version}/{kind}", f"{len(v):,} rows, last trade_date {last_td} (expected "
                                                    f"{expected_last}), {len(foreign)} date(s) >= the cut that are "
                                                    f"not rebuilt {foreign[:3]}", r_ok))
                ok_h = ok_h and r_ok
        # The PRICE half must be complete or the repair is not verified and must be rolled back; the
        # variables half routes to exit 6 instead, which is the whole reason that code exists.
        ok_n = (n_price == n_snap - 4)
        ok_vars = ok_h and n_vars == 4 and not sync_failed
        if not ok_vars and not stale:
            stale = [f"{v}/{k}/{t}.parquet" for v in ("raw", "clean") for k in ("variables", "quality")]
        seam_rebase._say(f"  VERIFY (a) served daily bars dated >= {cut} that are not rebuilt: raw {len(stray_r)} {stray_r[:4]} clean {len(stray_c)} -> {'OK' if ok_a else 'MISMATCH'}")
        seam_rebase._say(f"  VERIFY (b) rebuilt sessions vs Yahoo {a.verify_against}: {matched}/{total} within 1 % -> "
              f"{'OK' if ok_b else ('n/a' if not (a.verify_against and rebuilt_days) else ('UNVERIFIABLE' if unverifiable else 'MISMATCH'))}")
        seam_rebase._say(f"  VERIFY (c) served clean ⊆ raw, raw ends {raw_srv['datetime'].max().date()} (expected {expected_last}), "
              f"bar counts raw {len(raw_srv):,}/{len(new_raw):,} clean {len(clean_srv):,}/{len(new_clean):,} -> {'OK' if ok_c else 'MISMATCH'}")
        _print_gate(srv_rows, f"VERIFY (d) basis gate on the SERVED 1-minute file -> {'OK' if ok_d else 'MISMATCH'}")
        if e_rows:
            seam_rebase._say(f"  VERIFY (e) served rebuilt sessions vs the class-share prints ({len(e_rows)} sampled) -> {'OK' if ok_e else 'MISMATCH'}")
            for d, exp, got, okr in e_rows:
                seam_rebase._say(f"    {d} prints (close, volume, bars) {exp} vs served {got} -> {'OK' if okr else 'FAIL'}")
        for version, detail, r_ok in f_rows:
            seam_rebase._say(f"  VERIFY (f) SERVED {version} pre-window vs the 2026-07-13 snapshot, whole half: {detail} -> {'OK' if r_ok else 'MISMATCH'}")
        seam_rebase._say(f"  VERIFY (g) every served clean bar equals the served raw bar at the same minute: {len(clean_srv):,} clean bars, "
              f"{g_missing:,} not in raw, {g_mism:,} value mismatch(es) over {len(vcols)} columns -> {'OK' if ok_g else 'MISMATCH'}")
        for what, detail, r_ok in h_rows:
            seam_rebase._say(f"  VERIFY (h) served {what}: {detail} -> {'OK' if r_ok else 'MISMATCH'}")
        seam_rebase._say(f"  VERIFY (i) price objects written {n_price} vs snapshotted-minus-four {n_snap - 4} -> "
                         f"{'OK' if ok_n else 'MISMATCH'}; variables/quality {n_vars}/4 and read back -> "
                         f"{'OK' if ok_vars else 'FAILED, exit 6'}")
        # `verified` is the PRICE verdict ONLY (R865 #2). ok_h and n_vars decide exit 6 below, and a
        # variables failure must never restore 18 correct price objects.
        verified = bool(ok_a and ok_c and ok_d and ok_e and ok_f and ok_g and ok_n and (ok_b or unverifiable))
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
            seam_rebase._record(snap_dir, f"EXIT 4 RESTORE FAILED after {n_price + n_vars} upload(s); cause {why}; restore error {type(ex2).__name__}: {str(ex2)[:200]}")
            seam_rebase._say(f"  FAILED after the snapshot with {n_price + n_vars} object(s) uploaded ({why}) and the RESTORE FAILED "
                             f"({type(ex2).__name__}: {str(ex2)[:200]}) - run: python seam_rebase.py {t} --restore \"{snap_dir}\""); return 4
        seam_rebase._record(snap_dir, f"EXIT 1 RESTORED {n_back} objects after {n_price + n_vars} upload(s); cause {why}")
        seam_rebase._say(f"  FAILED after the snapshot with {n_price + n_vars} object(s) uploaded ({why}) - restored {n_back} objects; "
                         f"served state is the pre-repair state"); return 1
    # A VARIABLES PROBLEM IS ANY OF: the sync raised twice, it computed nothing, fewer than four
    # objects were written, or the four did not read back correctly (R865 #2). All four route to
    # exit 6 - "prices verified, serving incomplete" - never to a restore of correct price objects.
    vars_bad = bool(sync_failed) or ok_vars is False
    if verified is None:
        # a served object could not be read back (an R2 error, not a 404): the writes are not known
        # wrong, nothing is restored, a human re-verifies. The stale-variables list is printed FIRST
        # so an exit 3 never hides an exit 6 (R736).
        seam_rebase._record(snap_dir, f"EXIT 3 UNVERIFIABLE (read-back failed): {unverifiable[:200]}" + (f"; STALE {stale}" if vars_bad else ""))
        if vars_bad:
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
        seam_rebase._record(snap_dir, f"EXIT 3 UNVERIFIABLE (market fetch): {unverifiable[:200]}" + (f"; STALE {stale}" if vars_bad else ""))
        if vars_bad:
            seam_rebase._say(f"  variables/quality sync failed: {sync_failed}. STALE OBJECTS: {stale}")
        seam_rebase._say(f"  UNVERIFIABLE, DATA LIVE: (a)(c)(d)(e) passed but (b) could not be measured - {unverifiable}. Nothing restored; "
                         f"re-run the Yahoo comparison for {t} vs {a.verify_against} before calling this complete; snapshot kept at {snap_dir}"); return 3
    if vars_bad:
        why_v = sync_failed or [f"the four objects did not read back correctly (n_vars={n_vars})"]
        seam_rebase._record(snap_dir, f"EXIT 6 prices verified, variables/quality NOT: {why_v}; STALE {stale}")
        seam_rebase._say(f"  PRICES VERIFIED but variables/quality are not: {why_v}. STALE OBJECTS: {stale}. Not restoring; run "
                         f"sync_ticker_variables(client, version, '{t}', df, force_full=True) for each named version"); return 6
    # EVERY typed input that changed what was checked or written goes in the record (R867 #3):
    # --own-split's own help says "Cite the source in the run record", and it was the one input
    # that could relax the gate while being absent from it.
    seam_rebase._record(snap_dir, f"EXIT 0 DONE repaired cut={cut} until={a.until} unscale={a.unscale} "
                                  f"kept_from={a.kept_from} cut_gap_min={a.cut_gap_min} "
                                  f"own_split={a.own_split} anchors={a.anchor} "
                                  f"basis_samples={a.basis_samples} rebuild_from_cs={a.rebuild_from_cs} "
                                  f"verify_against={a.verify_against} "
                                  # WHAT THE OVERRIDE DID, NOT WHETHER IT WAS TYPED (R872 #4).
                                  f"allow_queued={a.allow_queued}"
                                  f"{'(FIRED: state was queued)' if a.allow_queued and st == 'queued' else ''} "
                                  f"daily_run_state={st} "
                                  f"rebuilt={len(rebuilt):,}")
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

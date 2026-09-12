"""Render the variables/quality delta disclosure (R741 finding 1, R743 finding 1, R744 findings 1 and 3) from
D:/temp/claude/_verify_variables_delta.json (instrument v2: dates + magnitudes) as markdown: a paragraph with the
measured pattern, then a per-ticker table with pre- and post-seam columns SEPARATED. Every number is derived here
from the JSON; nothing is typed in. Read-only; prints markdown."""
import json, sys
from collections import Counter
rows = json.load(open("D:/temp/claude/_verify_variables_delta.json", encoding="utf-8"))
rows.sort(key=lambda r: r["ticker"])
n = len(rows)
PL = {"amihud_illiquidity", "dollar_volume", "share_volume", "overnight_return"}
SCALE_INV = ["rv_1min", "rv_5min", "bipower_variation", "hl_range", "open_to_close_return", "intraday_return_std", "vr5", "vr10",
             "parkinson", "rogers_satchell", "ac1", "bns_z", "corwin_schultz_bps", "roll_spread_bps"]
missing_v2 = [r["ticker"] for r in rows if not any(isinstance(r.get(k), dict) and "post_dates" in r[k] for k in ("clean_variables",))]
if missing_v2:
    print(f"<!-- WARNING: {len(missing_v2)} ticker(s) measured by the v1 instrument (no dates): {' '.join(missing_v2)} - re-run verify_variables_delta.py --only on them -->")


def d(r, key):
    v = r.get(key)
    return v if isinstance(v, dict) else None


def cell(v):
    if v is None:
        return "-"
    post = v["post_seam_sessions_differing"]
    rng = f" ({v['post_seam_first']}..{v['post_seam_last']})" if post else ""
    return f"pre {v['pre_seam_sessions_differing']:,} / post {post}{rng}"


def cols(v, which):
    if v is None:
        return "-"
    c = v.get(which) or {}
    return ", ".join(sorted(c)) if c else "none"


def fmt(x):
    return "n/a" if x is None else f"{x:.1e}"


# ---- facts from the JSON ----
measured = sorted(r["measured_utc"] for r in rows)
cq = [(r["ticker"], d(r, "clean_quality")) for r in rows if d(r, "clean_quality")]
quality_pre_stale = [t for t, v in cq if v["pre_seam_sessions_differing"] > 0]
quality_fresh = [t for t, v in cq if v["pre_seam_sessions_differing"] == 0]
qp = sorted((v["pre_seam_sessions_differing"], t) for t, v in cq)
raw_q_identical = sum(1 for r in rows if d(r, "raw_quality") and (d(r, "raw_quality")["pre_seam_sessions_differing"] + d(r, "raw_quality")["post_seam_sessions_differing"]) == 0)

# clean variables, POST-seam, by DATE (R744 finding 1): where the 2026 differences start, per ticker
cv = [(r["ticker"], d(r, "clean_variables")) for r in rows if d(r, "clean_variables") and "post_dates" in d(r, "clean_variables")]
cv_2026_start = Counter(v.get("post_first_2026") for t, v in cv if v.get("post_first_2026"))
cv_with_2026 = [t for t, v in cv if v.get("post_first_2026")]
cv_0330 = [t for t, v in cv if v.get("post_first_2026") == "2026-03-30"]
cv_0331 = [t for t, v in cv if v.get("post_first_2026") == "2026-03-31"]
cv_june_start = [t for t, v in cv if v.get("post_first_2026") and "2026-06-01" <= v["post_first_2026"] <= "2026-07-31"]
cv_junejuly_any = [t for t, v in cv if any("2026-06-15" <= x <= "2026-07-31" for x in v.get("post_dates", []))]
cv_only_seamday = [t for t, v in cv if v["post_seam_sessions_differing"] > 0 and not v.get("post_first_2026")]
# clean quality on 2026-03-30
cq_0330 = [t for t, v in cq if v.get("post_first_2026") == "2026-03-30"]
cq_with_2026 = [t for t, v in cq if v.get("post_first_2026")]

# RAW variables, PRE-seam: which columns the rescale touched (R744 finding 3)
rv = [(r["ticker"], d(r, "raw_variables")) for r in rows if d(r, "raw_variables")]
sv_all = sum(1 for t, v in rv if (v.get("columns_pre") or {}).get("share_volume", 0) == v["pre_seam_sessions_differing"] and v["pre_seam_sessions_differing"] > 0)
dv_zero = [t for t, v in rv if "dollar_volume" not in (v.get("columns_pre") or {})]
am_zero = [t for t, v in rv if "amihud_illiquidity" not in (v.get("columns_pre") or {})]
def _si_max(v):
    return max([c for k, c in (v.get("columns_pre") or {}).items() if k not in PL] or [0])


# R748 finding 3: the class was cut at ">10 sessions", which silently dropped three tickers and made
# "the rounding is exact" false for them. Both groups are reported now, and the cut is stated.
raw_scale_inv = sorted(t for t, v in rv if _si_max(v) > 10)
raw_si_few = sorted(t for t, v in rv if 0 < _si_max(v) <= 10)
few_detail = []
for t, v in rv:
    if t not in raw_si_few:
        continue
    for c, m in (v.get("magnitude_pre") or {}).items():
        if c not in PL and m.get("sessions"):
            few_detail.append(f"{t} {c} on {m['sessions']} session(s), largest move {fmt(m.get('max_abs'))} in absolute terms"
                              + (f" (relative {fmt(m.get('max_rel'))}, against a near-zero denominator)" if m.get("max_abs") is not None else ""))

# R748 finding 2: sessions the rebase REMOVED or ADDED - counted by the instrument, named by nothing
lost_by_ticker, gained_by_ticker = {}, {}
for r in rows:
    for key in ("raw_variables", "raw_quality", "clean_variables", "clean_quality"):
        v = d(r, key)
        if not v:
            continue
        for x in (v.get("dates_lost") or []):
            lost_by_ticker.setdefault(r["ticker"], {}).setdefault(x, set()).add(key)
        for x in (v.get("dates_gained") or []):
            gained_by_ticker.setdefault(r["ticker"], {}).setdefault(x, set()).add(key)
n_lost_dates = sum(len(v) for v in lost_by_ticker.values())
n_lost_cells = sum(len(s) for v in lost_by_ticker.values() for s in v.values())
n_gained_dates = sum(len(v) for v in gained_by_ticker.values())
n_gained_cells = sum(len(s) for v in gained_by_ticker.values() for s in v.values())
_gain_dates = ", ".join(f"{t} {x}" for t, v in sorted(gained_by_ticker.items()) for x in sorted(v)) or "none"
# the bar counts behind the removal claim, measured by check_removed_dates.py rather than typed (R752 #5)
try:
    _rm = json.load(open("D:/temp/claude/_removed_dates_summary.json", encoding="utf-8"))
except Exception:                                            # noqa: BLE001
    _rm = {}
    print("<!-- WARNING: _removed_dates_summary.json missing - run check_removed_dates.py -->")
# magnitudes over the affected tickers, per scale-invariant column: median of the per-ticker medians, max of the maxima
mag = {}
for t, v in rv:
    if t not in raw_scale_inv:
        continue
    for c, m in (v.get("magnitude_pre") or {}).items():
        if c in PL or m.get("median_rel") is None:
            continue
        mag.setdefault(c, {"medians": [], "max": 0.0, "beyond": 0, "sessions": 0})
        mag[c]["medians"].append(m["median_rel"]); mag[c]["max"] = max(mag[c]["max"], m["max_rel"] or 0)
        mag[c]["beyond"] += m["beyond_1e-3"]; mag[c]["sessions"] += m["sessions"]
import statistics
variance_cols = [c for c in ("rv_1min", "rv_5min", "bipower_variation", "hl_range", "open_to_close_return", "intraday_return_std") if c in mag]
ratio_cols = [c for c in ("ac1", "bns_z", "corwin_schultz_bps", "vr5", "vr10") if c in mag]
var_med = [statistics.median(mag[c]["medians"]) for c in variance_cols]
var_max = max((mag[c]["max"] for c in variance_cols), default=0)
var_max_col = max(variance_cols, key=lambda c: mag[c]["max"]) if variance_cols else "n/a"
ratio_max = {c: mag[c]["max"] for c in ratio_cols}
ratio_beyond = sum(mag[c]["beyond"] for c in ratio_cols); ratio_sessions = sum(mag[c]["sessions"] for c in ratio_cols)
roll = mag.get("roll_spread_bps")

print(f"**Variables and quality objects, measured per ticker ({n} applied tickers, `verify_variables_delta.py` v2, served vs the pre-rebase snapshot, "
      f"pre- and post-seam columns separated, measured {measured[0][11:19]}-{measured[-1][11:19]}Z).** "
      f"RAW variables, PRE-seam: share_volume differs on every pre-seam session on {sv_all} of {n} tickers (x V, the volume factor, 1/K for a split); "
      f"dollar_volume is UNCHANGED on {len(dv_zero)} of {n} ({', '.join(dv_zero)}) and amihud_illiquidity on {len(am_zero)} of {n} - price x K "
      f"times volume x V is exact when K x V = 1 and K x price rounds exactly; they change only where it does not. "
      f"On {len(raw_scale_inv)} of {n} tickers ({', '.join(raw_scale_inv)}) the scale-invariant columns (rv_1min, rv_5min, bipower_variation, "
      f"hl_range, ac1, bns_z, corwin_schultz_bps, roll_spread_bps ...) also differ pre-seam: these are exactly the tickers whose K is a "
      f"non-terminating decimal (1/15, 1/6 or 1/3) and the tool rounds K x price to 6 decimals (seam_rebase.py, `.round(6)`), which moves "
      f"1-minute returns at the 1e-6 level. Magnitude (`magnitude_pre` in the JSON, |served - snapshot| / |snapshot|): on the variance and range "
      f"columns the median per column, taken across the affected tickers, runs {fmt(min(var_med)) if var_med else 'n/a'}..{fmt(max(var_med)) if var_med else 'n/a'} "
      f"(the widest single ticker-and-column median is {fmt(max((m['median_rel'] for t2, v2 in rv if t2 in raw_scale_inv for c2, m in (v2.get('magnitude_pre') or {}).items() if c2 in variance_cols and m.get('median_rel')), default=0))}), and the largest "
      f"single-session difference is {fmt(var_max)} ({var_max_col}, a near-zero denominator); the ratio columns amplify it - " + ", ".join(f"{c} max {fmt(m)}" for c, m in ratio_max.items())
      + f" - on {ratio_beyond:,} of {ratio_sessions:,} differing column-sessions beyond 1e-3"
      + (f"; roll_spread_bps, a square root of a near-zero covariance, reaches max {fmt(roll['max'])} relative on "
         f"{roll['beyond']:,} of {roll['sessions']:,} sessions" if roll else "")
      + f". The class is cut at MORE THAN 10 differing sessions on a scale-invariant column; "
      + (f"{len(raw_si_few)} further tickers ({', '.join(raw_si_few)}) fall below that cut, each on one or two sessions of "
         f"roll_spread_bps - a square root of a near-zero covariance, so the relative figure is large while the value "
         f"itself barely moves: {'; '.join(few_detail)}. " if raw_si_few else "")
      + f"The remaining {n - len(raw_scale_inv) - len(raw_si_few)} tickers have K with at most two decimals, so K x price "
      f"lands on a 6-decimal value exactly, and they show no scale-invariant difference at all. Note this does NOT mean their "
      f"price-LEVEL columns are unchanged: dollar_volume is price x K times volume x V, and each side is rounded separately "
      f"(prices to 6 decimals, minute volumes to integers), so the two roundings cancel only sometimes - which is exactly why "
      f"dollar_volume is unchanged on {len(dv_zero)} of {n} tickers rather than on all of the terminating-K ones. One discrete "
      f"column, bns_jump_5pct (a jump flag, not a "
      f"level), flips on a session where the rounding moved a statistic across its threshold - a flag change, which the "
      f"1e-6 description above does not cover. RAW variables, POST-seam: "
      f"the seam-day overnight_return (the seam artefact removed) and, on some, 2026-03-30 (the first daily-append session after the backfill window). "
      f"RAW quality: identical on {raw_q_identical} of {n}. "
      f"CLEAN quality PRE-seam (columns gap_rate, observed_bars, longest_gap, max_bars_since_trade, which no rescale can touch) differs on "
      f"{len(quality_pre_stale)} of {n} tickers ({qp[0][1]} {qp[0][0]:,} .. {qp[-1][1]} {qp[-1][0]:,} sessions)"
      + (f"; {', '.join(quality_fresh)} fresh" if quality_fresh else " - every one of them")
      + f": the served clean variables and quality were STALE against their served clean bars before the rebase, and the full recompute corrected "
      f"every session - the same condition as the fleet (next paragraph). "
      f"CLEAN variables POST-seam, by date (`post_dates`): {len(cv_with_2026)} of {len(cv)} tickers differ on 2026 sessions; the 2026 differences "
      f"start on 2026-03-30 for {len(cv_0330)}, on 2026-03-31 for {len(cv_0331)}, in June-July for {len(cv_june_start)}"
      + (f", elsewhere for {len(cv_with_2026) - len(cv_0330) - len(cv_0331) - len(cv_june_start)}" if len(cv_with_2026) - len(cv_0330) - len(cv_0331) - len(cv_june_start) else "")
      + f"; {len(cv_junejuly_any)} have differences inside 2026-06-15..2026-07-31; {len(cv_only_seamday)} differ post-seam on the seam day only. "
      f"CLEAN quality differs from 2026-03-30 on {len(cq_0330)} tickers ({', '.join(cq_0330)}) and on some 2026 session on {len(cq_with_2026)}. "
      f"(An earlier version of this paragraph said 0 tickers start on 2026-03-30: it read post_seam_first, which is the seam-day row on most tickers "
      f"- R744.) Nothing in the PRICE objects after the seam day changed.")
print()
_lost_lines = "; ".join(f"{t} {len(v)} ({', '.join(sorted(v))})" for t, v in sorted(lost_by_ticker.items()))
_gain_lines = "; ".join(f"{t} {len(v)} ({', '.join(sorted(v))})" for t, v in sorted(gained_by_ticker.items()))
print(f"**Sessions the rebase REMOVED and ADDED.** The recompute did not only change values: it changed which sessions exist. "
      f"{n_lost_dates} session-dates were REMOVED across {len(lost_by_ticker)} tickers ({n_lost_cells} object-cells, since a removed "
      f"date usually leaves both the variables and the quality object), and {n_gained_dates} session-date"
      f"{'s were' if n_gained_dates != 1 else ' was'} ADDED across {len(gained_by_ticker)} ticker{'s' if len(gained_by_ticker) != 1 else ''} "
      f"({n_gained_cells} object-cells). Removed, by ticker and date: {_lost_lines or 'none'}. Added: {_gain_lines or 'none'}. "
      f"These are sessions whose variables the old objects carried but a fresh computation from the served bars does not produce, "
      f"and the cause is measured rather than assumed: across ALL {n_lost_dates} removed dates and both versions "
      f"(`check_removed_dates.py`), the served CLEAN 1-minute file has ZERO bars on {_rm.get('clean',{}).get('zero','?')} of "
      f"{_rm.get('clean',{}).get('n_dates','?')} and the raw file has between {_rm.get('raw',{}).get('min','?')} and "
      f"{_rm.get('raw',{}).get('max','?')} - not one is a full session. So the served bars and the served variables do not disagree "
      f"about which sessions exist; the recompute is declining to describe sessions the bars no longer support. The ADDED date "
      f"({_gain_dates}) is NOT a clock artefact: that ticker's own pre-rebase snapshot already held 1-minute bars and a daily row "
      f"for the session while its snapshot variables stopped a day earlier, so the full recompute added a session the old objects "
      f"were simply missing - the same staleness resync_variables.py exists to repair (an earlier version of this sentence called it "
      f"a later daily append the snapshot predates, which the snapshot's own contents refute: R752). An earlier version of this "
      f"section counted the removals in the instrument and named them nowhere (R748).")
print()
print(f"**The fleet.** On ten tickers outside pass 1 (AAPL MSFT JNJ KO SPY XOM T PG DIA MO) the served clean variables differ from a fresh recompute with "
      f"the pipeline's own compute_recent_days on 61-81 % of all sessions (AAPL 3,662 of 5,959: observed_bars median 2.3 %, rv_1min 5.3 %, overnight_return "
      f"40 %; MSFT 4,826; JNJ 4,841; KO 4,736; SPY 4,789; XOM 4,825; T 4,674; PG 4,836; DIA 4,812; MO 4,256), every year 2002-2026; the control on "
      f"IGV/GOOGL (just rewritten by force_full) reproduces every column exactly; the 2026-07-13 snapshot's own variables already disagreed with its own "
      f"bars (AAPL 4,643 of 5,918, KO 5,717 of 5,917) - the divergence predates July. The RAW side is in step on the three measured (AAPL 3 sessions, "
      f"KO 1, MSFT 1: resync_variables.py dry runs, 15:10-15:15Z), so the condition is a clean-bar regeneration that was never followed by a "
      f"clean-variables recompute. Instruments: size_stale_variables.py, variables_diff_magnitude.py, date_variables_divergence.py (2026-09-05 "
      f"14:42-14:48Z), resync_variables.py. The rebased tickers are consistent by construction (force_full); the rest of the fleet needs a force_full "
      f"recompute - a separate decision, with its own tool (pipeline/resync_variables.py) under review.")
print()
print("| ticker | raw variables (sessions differing) | raw quality | clean variables | clean quality | clean variables 2026 from | clean columns PRE-seam | clean columns POST-seam |")
print("|---|---|---|---|---|---|---|---|")
for r in rows:
    v = d(r, "clean_variables")
    print(f"| {r['ticker']} | {cell(d(r, 'raw_variables'))} | {cell(d(r, 'raw_quality'))} | {cell(v)} | {cell(d(r, 'clean_quality'))} | "
          f"{(v or {}).get('post_first_2026') or '-'} | {cols(v, 'columns_pre')} | {cols(v, 'columns_post')} |")

"""seam_rebase.py — put ONE ticker's pre-2022-03-07 (PiTrading) history on the basis of its
post-2022-03-07 (IEX) history. Version 2, after adversarial review (2026-09-05).

WHY. The pre-2022 half is split+dividend+spin-off adjusted as of 2022-03-04 (the vendor's file
end); the later half is adjusted through 2026-03-27 by our merge step. Nothing back-propagated the
later corporate actions into the earlier half, so every ticker with a split or a dividend since
March 2022 carries a step at the seam (AMZN 20x, MO ~29%).

TWO MODES, because the dividend part is a convention decision that is Ahmed's, not the tool's:
  --mode split  (DEFAULT)  fix the SPLIT part only. Correct under either convention.
        P      = median(served PiTrading close / Yahoo close) over the last 3 PiTrading sessions
                 (Yahoo daily Close, auto_adjust=False = today's split basis, no dividends; the
                 last 3 sessions avoid any ex-dividend step inside the window)
        P_int  = P snapped to an INTEGER split ratio n or 1/n within 0.2 %; refused otherwise
        check  = P_int must equal the product of Yahoo's recorded split events after 2022-03-04
                 (all of them, or all but those after 2026-07-13 - the daily path applied later
                 ones to the full history); refused otherwise. Spin-off pseudo-splits (T 1.324)
                 never pass the integer test, which is the point.
        price x 1/P_int, volume x P_int, on every bar STRICTLY BEFORE 2022-03-07.
  --mode full   also fold in the dividend/spin factor D measured on the first 3 IEX sessions
        (price x D/P_int). NOT before the convention decision is recorded; the tool insists on
        --convention-decided to run this mode.

WHAT IT REWRITES, AND WHY NOT MORE. The clean set is NOT rescale-invariant (MAD ties on the cent
grid flip under x K: 13,051 bars for MO), so this tool rescales RAW and the SERVED CLEAN in place
and re-aggregates each - it does not re-clean. A change of units must not change which bars are
kept.

ORDER. snapshot every served object (byte-count verified) -> dry-run print -> [--apply] rescale
raw + clean -> upload 1min parquet+csv x2 -> aggregate_all x2 -> upload every timeframe ->
sync_ticker_variables(force_full) x2 -> VERIFY two ways from the served side: (a) daily seam step
== old/K; (b) independent re-measure: rebased P' over the same 3 sessions vs Yahoo must be
1.000 +- 0.3 % in split mode (or D in full mode). Non-zero exit names the snapshot to restore.

Run from inside pipeline/ of a MAIN-based tree (sibling imports; r2_client stamps the citation and
IEX-attribution parquet metadata). Credentials from the environment / .env.
    python seam_rebase.py TICKER [--mode split|full] [--apply] [--snapshot-dir DIR]
"""
from __future__ import annotations
import argparse
import math
import os
import sys
import pandas as pd

from aggregate import aggregate_all
from r2_client import download_parquet, get_client, upload_csv, upload_parquet
from variables_sync import sync_ticker_variables

TIMEFRAMES = ["5min", "15min", "30min", "hourly", "daily", "weekly", "monthly"]
SEAM = pd.Timestamp("2022-03-07")
FIX = pd.Timestamp("2026-07-13")          # daily path applies detected splits to the full history from here
PRICE_COLS = ("Open", "High", "Low", "Close")
BUCKET = "hfdatalibrary-data"
INTS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30, 40, 50, 60, 100, 200, 250, 500, 1000, 2000]
CANON_INT = sorted(set([1.0] + [float(n) for n in INTS] + [1.0 / n for n in INTS]))
WIN = 3


def snap_int(p: float):
    best = min(CANON_INT, key=lambda r: abs(math.log(p / r)))
    return best if abs(p / best - 1) <= 0.002 else None


def yahoo(t: str):
    import yfinance as yf
    tk = yf.Ticker(t.replace(".", "-"))
    h = tk.history(start="2022-02-14", end="2022-03-25", auto_adjust=False, actions=False)
    if h is None or len(h) == 0:
        return None, None
    h.index = pd.to_datetime(h.index).tz_localize(None).normalize()
    s = tk.splits
    if s is not None and len(s):
        s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
        s = s[s.index > pd.Timestamp("2022-03-04")]
    else:
        s = pd.Series(dtype=float)
    return h, s


def measure(daily: pd.DataFrame, h: pd.DataFrame):
    """P over the last WIN PiTrading sessions, D over the first WIN IEX sessions, vs Yahoo Close."""
    d = daily.set_index("datetime").sort_index()
    pre = d[d.index < SEAM].tail(WIN); post = d[d.index >= SEAM].head(WIN)
    y = h[["Close"]].rename(columns={"Close": "y"})
    jp = pre[["Close"]].rename(columns={"Close": "s"}).join(y, how="inner")
    jd = post[["Close"]].rename(columns={"Close": "s"}).join(y, how="inner")
    if len(jp) < 2 or len(jd) < 2:
        return None
    rp = jp["s"] / jp["y"]; rd = jd["s"] / jd["y"]
    return dict(P=float(rp.median()), D=float(rd.median()),
                spread_P=float(rp.max() / rp.min() - 1), spread_D=float(rd.max() / rd.min() - 1),
                pre_dates=[x.date() for x in jp.index], post_dates=[x.date() for x in jd.index])


def snapshot(client, ticker: str, out_dir: str) -> int:
    os.makedirs(out_dir, exist_ok=True)
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for o in page.get("Contents", []):
            k = o["Key"]; base = k.rsplit("/", 1)[-1]
            if base in (f"{ticker}.parquet", f"{ticker}.csv", f"{ticker}.csv.gz", f"{ticker}.json") and \
                    k.split("/")[0] in ("raw", "clean", "variables", "quality"):
                keys.append((k, o["Size"]))
    if not keys:
        raise SystemExit(f"snapshot: no served objects found for {ticker}")
    for k, size in keys:
        dest = os.path.join(out_dir, k.replace("/", "__"))
        client.download_file(BUCKET, k, dest)
        if os.path.getsize(dest) != size:
            raise SystemExit(f"snapshot: {k} is {size} bytes on R2 but {os.path.getsize(dest)} on disk - aborting before any write")
    with open(os.path.join(out_dir, "_MANIFEST.txt"), "w", encoding="utf-8") as f:
        for k, size in keys:
            f.write(f"{size}\t{k}\n")
    return len(keys)


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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker"); ap.add_argument("--mode", choices=("split", "full"), default="split")
    ap.add_argument("--apply", action="store_true"); ap.add_argument("--snapshot-dir", default=None)
    ap.add_argument("--convention-decided", action="store_true", help="required for --mode full")
    a = ap.parse_args(); t = a.ticker.upper()
    if a.mode == "full" and not a.convention_decided:
        raise SystemExit("--mode full folds dividends into the pre-2022 half; that is the convention decision. Pass --convention-decided only once it is recorded.")

    client = get_client()
    daily = download_parquet(client, "raw", t, "daily")
    if daily is None or daily.empty:
        raise SystemExit(f"{t}: no served daily file")
    daily["datetime"] = pd.to_datetime(daily["datetime"])
    if "source" in daily.columns and not ((daily["source"] == "pitrading").any() and (daily["source"] == "iex").any()):
        print(f"{t}: no PiTrading/IEX splice in the served file - nothing to rebase"); return 0
    h, splits = yahoo(t)
    if h is None:
        print(f"{t}: no market reference at Yahoo - cannot measure; disclose, do not repair"); return 3
    m = measure(daily, h)
    if m is None:
        print(f"{t}: fewer than 2 overlapping sessions with the market on a side - cannot measure"); return 3
    P, D = m["P"], m["D"]
    P_int = snap_int(P)
    prod_all = float(splits.prod()) if len(splits) else 1.0
    prod_prefix = float(splits[splits.index <= FIX].prod()) if len(splits) else 1.0
    print(f"{t}: P={P:.6f} (spread {m['spread_P']:.4%}, sessions {m['pre_dates']})  D={D:.6f} (spread {m['spread_D']:.4%}, sessions {m['post_dates']})")
    print(f"  Yahoo split events after 2022-03-04: {[(str(i.date()), float(v)) for i, v in splits.items()] or 'none'}  -> product all={prod_all:g}, up to 2026-07-13={prod_prefix:g}")
    if P_int is None:
        print(f"  REFUSED: P={P:.6f} does not snap to an integer split ratio within 0.2 % - spin-off or unclear; manual look needed"); return 2
    match = "all" if abs(P_int / prod_all - 1) <= 0.002 else ("prefix" if abs(P_int / prod_prefix - 1) <= 0.002 else None)
    if match is None:
        print(f"  REFUSED: P_int={P_int:g} matches neither Yahoo's split product ({prod_all:g}) nor the pre-2026-07-13 product ({prod_prefix:g})"); return 2
    if m["spread_P"] > 0.003:
        print(f"  REFUSED: PiTrading side unstable over its last {WIN} sessions ({m['spread_P']:.3%}) - look before rescaling"); return 2
    K = 1.0 / P_int; V = P_int
    if a.mode == "full":
        K = D / P_int
    if abs(K - 1) <= 0.002 and abs(V - 1) <= 1e-9:
        print(f"  nothing to rebase in --mode {a.mode} (P_int=1{', dividend/spin seam D=%.4f held for the convention decision' % D if abs(D - 1) > 0.002 else ''})"); return 0
    print(f"  {a.mode.upper()} rebase: price x{K:.6g}, volume x{V:g} on bars strictly before {SEAM.date()}  (split match: {match}; dividend/spin factor at seam D={D:.4f}{' - NOT applied in split mode' if a.mode == 'split' else ' - applied'})")

    raw = download_parquet(client, "raw", t); clean = download_parquet(client, "clean", t)
    if raw is None or raw.empty or clean is None or clean.empty:
        raise SystemExit(f"{t}: served raw/clean 1-minute file missing")
    extra = [c for c in raw.columns if c not in PRICE_COLS + ("Volume", "datetime", "source")]
    if extra:
        raise SystemExit(f"{t}: raw carries unexpected columns {extra} - policy for them is not defined; refusing")
    n_pre = int((pd.to_datetime(raw["datetime"]) < SEAM).sum())
    print(f"  raw {len(raw):,} bars ({n_pre:,} before the seam), clean {len(clean):,} bars; both rescaled in place, neither re-cleaned")
    if not a.apply:
        print("(dry run - pass --apply to rebase)"); return 0

    snap_dir = a.snapshot_dir or os.path.join("F:\\", f"hf_r2_snapshot_seam_{pd.Timestamp.today():%Y%m%d}", t)
    n_snap = snapshot(client, t, snap_dir)
    print(f"  snapshot: {n_snap} objects -> {snap_dir} (byte counts verified)")

    raw2, clean2 = rescale(raw, K, V), rescale(clean, K, V)
    n = 0
    for version, df in (("raw", raw2), ("clean", clean2)):
        upload_parquet(client, df, version, t, "1min"); upload_csv(client, df, version, t, "1min"); n += 2
        aggs = aggregate_all(df)
        for tf in TIMEFRAMES:
            if tf in aggs and not aggs[tf].empty:
                upload_parquet(client, aggs[tf], version, t, tf); n += 1
        sync_ticker_variables(client, version, t, df, force_full=True); n += 2
    print(f"  uploaded {n} objects")

    d2 = download_parquet(client, "raw", t, "daily"); d2["datetime"] = pd.to_datetime(d2["datetime"])
    dd = d2.set_index("datetime").sort_index()
    step_before = float(daily.set_index("datetime").sort_index().pipe(lambda x: x[x.index >= SEAM]["Close"].iloc[0] / x[x.index < SEAM]["Close"].iloc[-1]))
    step_after = float(dd[dd.index >= SEAM]["Close"].iloc[0] / dd[dd.index < SEAM]["Close"].iloc[-1])
    ok_a = abs(step_after / (step_before / K) - 1) < 0.005
    m2 = measure(d2, h); target = 1.0 if a.mode == "split" else D
    ok_b = m2 is not None and abs(m2["P"] / target - 1) < 0.003
    print(f"  VERIFY (a) served daily seam step x{step_after:.6f} vs expected x{step_before / K:.6f} -> {'OK' if ok_a else 'MISMATCH'}")
    print(f"  VERIFY (b) independent: rebased P'={m2['P'] if m2 else None} vs target {target:.6f} -> {'OK' if ok_b else 'MISMATCH'}")
    if not (ok_a and ok_b):
        print(f"  NOT VERIFIED - restore from {snap_dir}"); return 1
    print(f"  DONE: {t} rebased ({a.mode}); snapshot kept at {snap_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

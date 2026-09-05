"""scan_unapplied_splits.py — the full population of the live defect the seam review exposed:
served tickers whose data carries a stock split UNAPPLIED (the raw price step is still in the file).

For every served ticker with a Yahoo split event after 2022-03-04, download the served raw daily
file and run seam_rebase.unapplied_splits(): the served close step across each event, divided by
Yahoo's (adjusted, ~1.0) step, within 5 % of the raw factor 1/s means the split was never applied.
Also reports P and D at the seam so the reader sees which half is on which basis.

READ ONLY. Output: D:\\temp\\claude\\unapplied_splits.csv + a summary.
    python scan_unapplied_splits.py [--candidates D:\\...\\scan_actions.csv] [TICKER ...]
"""
from __future__ import annotations
import argparse, os, sys, time
import pandas as pd

from r2_client import download_parquet, get_client
from seam_rebase import SEAM, WIN, ratio_over, unapplied_splits, yahoo

DEFAULT = r"D:\temp\claude\D--research-hfdatalibrary\cbd00e2e-9905-4a18-b264-d63efdb58109\scratchpad\scan_actions.csv"
OUT = r"D:\temp\claude\unapplied_splits.csv"


def main() -> int:
    ap = argparse.ArgumentParser(); ap.add_argument("--candidates", default=DEFAULT); ap.add_argument("tickers", nargs="*")
    a = ap.parse_args()
    if a.tickers:
        tickers = [t.upper() for t in a.tickers]
    else:
        c = pd.read_csv(a.candidates)
        tickers = sorted(c[c.splits_after.notna()].ticker)
    print(f"tickers with a Yahoo split after 2022-03-04: {len(tickers)}", flush=True)
    client = get_client(); rows = []
    for i, t in enumerate(tickers, 1):
        rec = {"ticker": t, "events": "", "unapplied": "", "n_unapplied": 0, "P": None, "D": None, "note": ""}
        try:
            d = download_parquet(client, "raw", t, "daily")
            if d is None or d.empty:
                rec["note"] = "no served daily"; rows.append(rec); continue
            d["datetime"] = pd.to_datetime(d["datetime"]); dd = d.set_index("datetime").sort_index()
            h, spl, div = yahoo(t)
            if h is None:
                rec["note"] = "no yahoo"; rows.append(rec); continue
            y = h["Close"]
            rec["events"] = ";".join(f"{i2.date()}:{v:g}" for i2, v in spl.items())
            una = unapplied_splits(dd["Close"], y, spl)
            rec["unapplied"] = ";".join(f"{ev}:{s:g}(rel {rel:.3f})" for ev, s, rel in una); rec["n_unapplied"] = len(una)
            pre = [x for x in dd.index if x < SEAM][-WIN:]; post = [x for x in dd.index if x >= SEAM][:WIN]
            P, _, _ = ratio_over(dd["Close"], y, pre); D, _, _ = ratio_over(dd["Close"], y, post)
            rec["P"], rec["D"] = (round(P, 6) if P else None), (round(D, 6) if D else None)
        except Exception as ex:
            rec["note"] = f"ERROR:{str(ex)[:60]}"
        rows.append(rec)
        if i % 20 == 0 or i == len(tickers):
            print(f"  {i}/{len(tickers)} {t}: unapplied={rec['n_unapplied']} {rec['unapplied'][:60]}", flush=True)
            pd.DataFrame(rows).to_csv(OUT, index=False)
        time.sleep(0.4)
    df = pd.DataFrame(rows); df.to_csv(OUT, index=False)
    bad = df[df.n_unapplied > 0]
    print(f"\nUNAPPLIED SPLITS IN SERVED DATA: {len(bad)} tickers of {len(df)} scanned")
    print(bad[["ticker", "unapplied", "P", "D"]].to_string(index=False))
    print(f"written: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

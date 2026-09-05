"""unapplied_repair_list.py — turn scan_unapplied_splits.py's CSV into a repair list.

Keeps only INTEGER-ratio events (n:1 or 1:n) — genuine stock splits. Yahoo also records spin-off
exchange ratios and stock dividends as "splits" (BDX 1.272 Embecta, SCCO 1.005); the served data
is right not to apply those under a split-only convention, and whether to apply them at all is the
price-convention decision. They are listed separately, not repaired.

For each kept event prints the exact proven command (manual_split from the PR #7 worktree, which
takes a CA_DATE and rescales only bars STRICTLY BEFORE it):
    python manual_split.py TICKER RATIO CA_DATE      # RATIO = new_price/old_price = 1/s ; CA_DATE = event date
and the check that must hold afterwards: served close step across the event ~ Yahoo's step (rel ~ 1.0).
Nothing is executed here.
"""
from __future__ import annotations
import math, sys
import pandas as pd

SRC = r"D:\temp\claude\unapplied_splits.csv"
INTS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30, 40, 50, 60, 100, 200, 250, 500, 1000, 2000]
CANON = sorted(set([float(n) for n in INTS] + [1.0 / n for n in INTS]))


def is_int_ratio(s: float) -> bool:
    return any(abs(math.log(s / r)) <= math.log(1.002) for r in CANON)


def main() -> int:
    import os, sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from symbol_map import REASSIGNED
    d = pd.read_csv(SRC); d = d[d.n_unapplied > 0]
    # R732, the class fix: an event recorded by Yahoo under a REASSIGNED symbol is the new owner's
    # (iPower's x8/x9 were applied to a 2008-2017 instrument served as IPW this way). Never listed.
    dropped = sorted(set(d.ticker) & set(REASSIGNED))
    if dropped:
        print(f"REASSIGNED symbols excluded (their Yahoo events belong to another issuer): {dropped}\n")
        d = d[~d.ticker.isin(REASSIGNED)]
    repair, pseudo = [], []
    for r in d.itertuples():
        for ev in str(r.unapplied).split(";"):
            if not ev or ":" not in ev:
                continue
            date, rest = ev.split(":", 1)
            s = float(rest.split("(")[0])
            rel = float(rest.split("rel ")[1].rstrip(")")) if "rel " in rest else float("nan")
            (repair if is_int_ratio(s) else pseudo).append((r.ticker, date, s, rel, r.P, r.D))
    print(f"scanned rows with unapplied events: {len(d)}; integer-ratio (repair): {len(repair)}; pseudo-splits (convention decision): {len(pseudo)}\n")
    print("REPAIR LIST — run from D:\\temp\\claude\\hf_wt_split_fix\\pipeline (PR #7 manual_split with CA_DATE), oldest event first:")
    for t, date, s, rel, P, D in sorted(repair, key=lambda x: x[1]):
        ratio = 1.0 / s
        print(f"  python manual_split.py {t} {ratio:.6g} {date}     # {s:g} split on {date}; served step/Yahoo step now {rel:.3f}; P={P} D={D}")
    print("\nPSEUDO-SPLITS (spin-off / stock-dividend ratios) — NOT repaired; part of the price-convention decision:")
    for t, date, s, rel, P, D in sorted(pseudo, key=lambda x: x[1]):
        print(f"  {t:6} {date} ratio {s:g}  rel {rel:.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

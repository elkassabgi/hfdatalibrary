"""For applied tickers: the SNAPSHOT's raw variables (pre-rebase, F:/hf_r2_snapshot_seam_20260905/<T>/...) vs the
SERVED raw variables (post-rebase, written by force_full), PRE-seam sessions only, on the scale-invariant columns
(a rescale cannot change them). Reports per column: sessions differing, median and max |relative difference|, and
how many exceed 1e-3 - separating a rounding artefact of the rescale (tiny) from staleness (large). Read-only."""
import glob, io, os, sys
sys.path.insert(0, "D:/temp/claude/hf_wt_main/pipeline")
import numpy as np
import pandas as pd
from r2_client import get_client, download_to_buffer

SEAM = pd.Timestamp("2022-03-07")
COLS = ["ac1", "rv_1min", "rv_5min", "bipower_variation", "bns_z", "corwin_schultz_bps", "vr5", "open_to_close_return", "hl_range"]
client = get_client()
for t in sys.argv[1:]:
    snap = glob.glob(f"F:/hf_r2_snapshot_seam_20260905/{t}/**/raw*variables*{t}*.parquet", recursive=True) or \
           glob.glob(f"F:/hf_r2_snapshot_seam_20260905/{t}/raw/variables/*.parquet")
    if not snap:
        print(f"{t}: snapshot raw variables not found under F:/hf_r2_snapshot_seam_20260905/{t}");
        print("   tree:", [os.path.relpath(p, f'F:/hf_r2_snapshot_seam_20260905/{t}') for p in glob.glob(f'F:/hf_r2_snapshot_seam_20260905/{t}/**/*', recursive=True)][:24]); continue
    a = pd.read_parquet(snap[0]); b = pd.read_parquet(io.BytesIO(download_to_buffer(client, f"raw/variables/{t}.parquet")))
    for d in (a, b):
        d["trade_date"] = pd.to_datetime(d["trade_date"]).dt.normalize()
    a = a.set_index("trade_date").sort_index(); b = b.set_index("trade_date").sort_index()
    idx = a.index.intersection(b.index); idx = idx[idx < SEAM]
    print(f"{t}: {len(idx):,} common pre-seam sessions (snapshot {os.path.basename(snap[0])})")
    for c in COLS:
        if c not in a.columns or c not in b.columns:
            continue
        x = a.loc[idx, c].astype(float).to_numpy(); y = b.loc[idx, c].astype(float).to_numpy()
        ok = np.isfinite(x) & np.isfinite(y)
        rel = np.abs(y[ok] - x[ok]) / np.maximum(np.abs(x[ok]), 1e-12)
        ne = rel > 1e-9
        nan_mismatch = int((np.isfinite(x) != np.isfinite(y)).sum())
        if ne.any() or nan_mismatch:
            print(f"   {c:22s} differ {int(ne.sum()):>5,}  median|rel| {np.median(rel[ne]) if ne.any() else 0:.2e}  max {rel[ne].max() if ne.any() else 0:.2e}  >1e-3: {int((rel > 1e-3).sum()):,}  nan-mismatch {nan_mismatch}")

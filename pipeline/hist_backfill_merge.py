"""hist_backfill_merge.py — stage 3 of the IEX HIST re-derivation (rev 2,
post adversarial review wf_f14d3686): per-ticker WINDOW REPLACEMENT (not the
daily upsert — old window rows are dropped and re-derived HIST bars take their
place), full re-clean, re-aggregation, verification gates, full-window
variables/quality recompute, and (only in --mode execute) the R2 upload sweep.

Fixes from review wf_f14d3686 folded in:
  B1 tz: R2 raw parquets are tz-NAIVE ET wall time; new bars are NY-aware.
     Conversion = .dt.tz_convert('America/New_York').dt.tz_localize(None)
     (production daily_update.py:253-255 semantics, empirically verified).
     _et_dates treats naive input as ET wall time.
  B3 variables: full WINDOW recompute via compute_variables.compute_df
     (+1 prior trading day context), merged into existing variables file with
     window rows dropped first; uploads variables + quality; counted, not silent.
  B4 freshness: execute re-downloads raw, asserts outside-window hash AND
     max(datetime) unchanged since verify; on ANY change it recomputes inline
     (verify path + upload) instead of uploading the stale stage. run_id binds
     _SHARD_OK -> metrics.json -> _VERIFY_OK.
  M: SKIP (no writes) for tickers with no shard data AND no old-window rows;
     STANDARD_COLS filtering mirrors production; coverage gates
     (window_days_new >= window_days_old and volume_corr >= VCORR_MIN);
     clean-drift metric outside the window (pre/post separately); atomic
     staging (tmp dir swap, metrics last, stale metrics invalidated);
     execute SKIPs never-staged tickers; timestamped run summaries;
     single --mode CLI.

Modes:
  python pipeline/hist_backfill_merge.py --mode verify  [--tickers A B ...]
  python pipeline/hist_backfill_merge.py --mode execute [--tickers A B ...]
      (requires _VERIFY_OK gate file whose run_id matches — placed manually
       after the verification report passes adversarial review)
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date

PIPELINE = os.path.dirname(os.path.abspath(__file__))
ROOT = r"E:\iex_hist_backfill"
BYTICKER = os.path.join(ROOT, "_byticker")
STAGED = os.path.join(ROOT, "_staged")
WIN_START, WIN_END = date(2022, 3, 7), date(2026, 3, 27)
WORKERS = 32
VCORR_MIN = 0.98          # volume correlation gate when common_days >= 10
STANDARD_COLS = ["datetime", "Open", "High", "Low", "Close", "Volume", "source"]
QUALITY_COLS = ["trade_date", "ticker", "gap_rate", "observed_bars",
                "longest_gap", "max_bars_since_trade"]  # = variables_sync.QUALITY_COLS


def log(m: str) -> None:
    print(f"[{time.strftime('%m-%d %H:%M:%S')}] {m}", flush=True)


def _shard_run_id() -> str:
    with open(os.path.join(BYTICKER, "_SHARD_OK")) as f:
        return json.load(f)["run_id"]


def _et_dates(dt_series):
    """ET session date. Naive input IS ET wall time (production convention)."""
    if dt_series.dt.tz is None:
        return dt_series.dt.date
    return dt_series.dt.tz_convert("America/New_York").dt.date


def _to_naive_et(series):
    """Production conversion (daily_update.py:253-255 semantics)."""
    import pandas as pd
    s = pd.to_datetime(series)
    if s.dt.tz is not None:
        s = s.dt.tz_convert("America/New_York").dt.tz_localize(None)
    return s


def _frame_hash(df) -> str:
    b = io.BytesIO()
    df.sort_values("datetime").reset_index(drop=True).to_parquet(b, index=False)
    return hashlib.sha256(b.getvalue()).hexdigest()


def _load_shard(ticker: str):
    import pandas as pd
    shard_path = os.path.join(BYTICKER, f"ticker={ticker}")
    if not os.path.isdir(shard_path):
        return None
    df = pd.read_parquet(shard_path)
    df["datetime"] = _to_naive_et(df["datetime"])
    if "ticker" in df.columns:
        df = df.drop(columns=["ticker"])
    df["source"] = "iex"
    return df.reindex(columns=STANDARD_COLS)


def _recompute(ticker: str, existing_raw, existing_clean, new_win, run_id: str) -> dict:
    """Window replacement + full re-clean + aggregate + gates. Returns dict with
    metrics and in-memory frames (not staged here)."""
    import pandas as pd

    sys.path.insert(0, PIPELINE)
    from aggregate import aggregate_all
    from clean_pipeline import clean_bars

    m: dict = {"ticker": ticker, "run_id": run_id}
    existing_raw = existing_raw[[c for c in STANDARD_COLS if c in existing_raw.columns]]
    ex_dates = _et_dates(existing_raw["datetime"])
    in_window = (ex_dates >= WIN_START) & (ex_dates <= WIN_END)
    outside = existing_raw.loc[~in_window]
    old_window = existing_raw.loc[in_window]

    m["outside_rows"] = int(len(outside))
    m["outside_hash_before"] = _frame_hash(outside)
    m["existing_max_datetime"] = str(existing_raw["datetime"].max())
    m["old_window_rows"] = int(len(old_window))
    if "source" in old_window.columns:
        m["old_window_sources"] = {str(k): int(v) for k, v in
                                   old_window["source"].value_counts().items()}
    m["new_window_rows"] = int(len(new_win)) if new_win is not None else 0

    parts = [outside] + ([new_win] if new_win is not None and len(new_win) else [])
    merged_raw = (pd.concat(parts, ignore_index=True)
                  .sort_values("datetime", kind="stable")
                  .drop_duplicates(subset=["datetime"], keep="last")
                  .reset_index(drop=True))

    # hard gate 1: outside-window preservation
    mg_dates = _et_dates(merged_raw["datetime"])
    mg_out = merged_raw.loc[(mg_dates < WIN_START) | (mg_dates > WIN_END)]
    m["outside_preserved"] = (len(mg_out) == len(outside)
                              and _frame_hash(mg_out) == m["outside_hash_before"])

    # window comparison metrics (old vendor-derived vs new HIST-derived)
    m["volume_corr"] = None
    m["window_days_old"] = int(pd.Series(list(ex_dates[in_window])).nunique()) if len(old_window) else 0
    if new_win is not None and len(new_win):
        nw_dates = _et_dates(new_win["datetime"])
        m["window_days_new"] = int(pd.Series(list(nw_dates)).nunique())
    else:
        m["window_days_new"] = 0
    if len(old_window) and new_win is not None and len(new_win):
        ow = old_window.assign(d=list(ex_dates[in_window])).groupby("d")["Volume"].sum()
        nw = new_win.assign(d=list(_et_dates(new_win["datetime"]))).groupby("d")["Volume"].sum()
        common = ow.index.intersection(nw.index)
        m["common_days"] = int(len(common))
        if len(common) >= 10:
            m["volume_corr"] = float(ow.loc[common].corr(nw.loc[common]))
            ratio = (nw.loc[common] / ow.loc[common].replace(0, pd.NA))
            m["volume_ratio_median"] = float(ratio.median())
        j = old_window.set_index("datetime")["Close"].to_frame("old").join(
            new_win.set_index("datetime")["Close"].to_frame("new"), how="inner")
        m["common_minutes"] = int(len(j))
        if len(j):
            m["close_median_absdiff"] = float((j["old"] - j["new"]).abs().median())
            m["close_p99_absdiff"] = float((j["old"] - j["new"]).abs().quantile(0.99))

    # full re-clean (production backfill path) + aggregation
    merged_clean = clean_bars(merged_raw)
    m["raw_total"], m["clean_total"] = int(len(merged_raw)), int(len(merged_clean))
    raw_aggs = aggregate_all(merged_raw)
    clean_aggs = aggregate_all(merged_clean)
    m["agg_timeframes"] = sorted(raw_aggs.keys())

    # clean-drift metric outside the window (pre/post separately)
    if existing_clean is not None and len(existing_clean):
        existing_clean = existing_clean[[c for c in STANDARD_COLS if c in existing_clean.columns]]
        ecd = _et_dates(existing_clean["datetime"])
        ncd = _et_dates(merged_clean["datetime"])
        m["clean_pre_before"] = int((ecd < WIN_START).sum())
        m["clean_pre_after"] = int((ncd < WIN_START).sum())
        m["clean_post_before"] = int((ecd > WIN_END).sum())
        m["clean_post_after"] = int((ncd > WIN_END).sum())

    # gates
    schema_ok = (list(merged_raw.columns) == STANDARD_COLS
                 and str(merged_raw["datetime"].dtype) == "datetime64[ns]"
                 and str(merged_raw["Volume"].dtype) == "int64"
                 and list(merged_clean.columns) == STANDARD_COLS)
    m["schema_match"] = bool(schema_ok)
    coverage_ok = m["window_days_new"] >= m["window_days_old"]
    m["coverage_ok"] = bool(coverage_ok)
    vcorr_ok = (m["volume_corr"] is None or m.get("common_days", 0) < 10
                or m["volume_corr"] >= VCORR_MIN)
    m["vcorr_ok"] = bool(vcorr_ok)
    m["gate_pass"] = bool(m["outside_preserved"] and schema_ok and coverage_ok and vcorr_ok
                          and (m["new_window_rows"] > 0 or m["old_window_rows"] == 0))
    return {"metrics": m, "merged_raw": merged_raw, "merged_clean": merged_clean,
            "raw_aggs": raw_aggs, "clean_aggs": clean_aggs}


def _stage(ticker: str, res: dict) -> None:
    """Atomic staging: write into tmp dir, metrics last, swap into place."""
    t_final = os.path.join(STAGED, ticker)
    t_tmp = t_final + ".tmp"
    if os.path.exists(t_tmp):
        shutil.rmtree(t_tmp)
    os.makedirs(t_tmp)
    res["merged_raw"].to_parquet(os.path.join(t_tmp, "raw_1min.parquet"), index=False)
    res["merged_clean"].to_parquet(os.path.join(t_tmp, "clean_1min.parquet"), index=False)
    for tf, dfa in res["raw_aggs"].items():
        dfa.to_parquet(os.path.join(t_tmp, f"raw_{tf}.parquet"), index=False)
    for tf, dfa in res["clean_aggs"].items():
        dfa.to_parquet(os.path.join(t_tmp, f"clean_{tf}.parquet"), index=False)
    with open(os.path.join(t_tmp, "metrics.json"), "w") as f:
        json.dump(res["metrics"], f, indent=1, default=str)
    if os.path.exists(t_final):
        shutil.rmtree(t_final)
    os.replace(t_tmp, t_final)


def _sync_variables_full_window(client, version: str, ticker: str, bars, r2c) -> int:
    """Full-window variables recompute (review fix B3): compute the window (+1
    prior trading day context) via compute_df, drop existing window rows, merge,
    upload variables + quality. Returns rows replaced. Raises on failure."""
    import pandas as pd

    from compute_variables import compute_df

    bdates = _et_dates(bars["datetime"])
    prior = bars.loc[bdates < WIN_START, "datetime"]
    ctx_start = _et_dates(prior.iloc[[-1]]).iloc[0] if len(prior) else WIN_START
    sel = (bdates >= ctx_start) & (bdates <= WIN_END)
    window_bars = bars.loc[sel]
    if window_bars.empty:
        return 0
    new_vars = compute_df(window_bars, ticker)
    if new_vars.empty:
        return 0
    new_vars["trade_date"] = pd.to_datetime(new_vars["trade_date"]).dt.normalize()
    new_vars = new_vars[(new_vars["trade_date"].dt.date >= WIN_START)
                        & (new_vars["trade_date"].dt.date <= WIN_END)]

    existing = r2c.download_parquet(client, version, ticker, timeframe="variables")
    if existing is not None and not existing.empty:
        existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.normalize()
        keep = existing[(existing["trade_date"].dt.date < WIN_START)
                        | (existing["trade_date"].dt.date > WIN_END)]
        merged = pd.concat([keep, new_vars], ignore_index=True)
    else:
        merged = new_vars
    merged = (merged.sort_values("trade_date")
                    .drop_duplicates(subset=["trade_date"], keep="last")
                    .reset_index(drop=True))
    r2c.upload_parquet(client, merged, version, ticker, timeframe="variables")
    qcols = [c for c in QUALITY_COLS if c in merged.columns]
    r2c.upload_parquet(client, merged[qcols], version, ticker, timeframe="quality")
    return int(len(new_vars))


def process_ticker(ticker: str, mode: str, run_id: str) -> tuple[str, str]:
    import pandas as pd  # noqa: F401

    sys.path.insert(0, PIPELINE)
    import r2_client

    t_dir = os.path.join(STAGED, ticker)
    metrics_path = os.path.join(t_dir, "metrics.json")

    try:
        if mode == "execute":
            if not os.path.exists(metrics_path):
                return (ticker, "SKIP (not staged)")
            with open(metrics_path) as f:
                m = json.load(f)
            if m.get("run_id") != run_id:
                return (ticker, f"SKIP (stale run_id {m.get('run_id')})")
            if not m.get("gate_pass"):
                return (ticker, "SKIP (gate_pass false)")
            client = r2_client.get_client()

            # freshness re-check (review fix B4)
            fresh = r2_client.download_parquet(client, "raw", ticker)
            fresh = fresh[[c for c in STANDARD_COLS if c in fresh.columns]]
            f_dates = _et_dates(fresh["datetime"])
            f_out = fresh.loc[(f_dates < WIN_START) | (f_dates > WIN_END)]
            changed = (_frame_hash(f_out) != m["outside_hash_before"]
                       or str(fresh["datetime"].max()) != m["existing_max_datetime"])
            if changed:
                # recompute inline against the CURRENT file, then stage + upload
                existing_clean = r2_client.download_parquet(client, "clean", ticker)
                new_win = _load_shard(ticker)
                res = _recompute(ticker, fresh, existing_clean, new_win, run_id)
                if not res["metrics"]["gate_pass"]:
                    _stage(ticker, res)
                    return (ticker, "GATE-FAIL (on execute-time recompute)")
                _stage(ticker, res)
                with open(metrics_path) as f:
                    m = json.load(f)

            n_up = 0
            for version in ("raw", "clean"):
                df1 = pd.read_parquet(os.path.join(t_dir, f"{version}_1min.parquet"))
                r2_client.upload_parquet(client, df1, version, ticker, "1min")
                # regenerate the served csv/{version}/{ticker}.csv (Ahmed 2026-07-12:
                # fixes the platform-wide CSV staleness; daily pipeline now maintains
                # these too)
                r2_client.upload_csv(client, df1, version, ticker, "1min")
                n_up += 2
                for tf in m["agg_timeframes"]:
                    dfa = pd.read_parquet(os.path.join(t_dir, f"{version}_{tf}.parquet"))
                    r2_client.upload_parquet(client, dfa, version, ticker, tf)
                    n_up += 1
            # variables/quality: full-window recompute, gated + counted
            var_status = ""
            try:
                for version in ("raw", "clean"):
                    dfv = pd.read_parquet(os.path.join(t_dir, f"{version}_1min.parquet"))
                    _sync_variables_full_window(client, version, ticker, dfv, r2_client)
            except Exception as ve:  # noqa: BLE001
                var_status = f", VARIABLES-FAILED: {type(ve).__name__}"
                with open(os.path.join(t_dir, "variables_failed.txt"), "w") as f:
                    f.write(str(ve))
            return (ticker, f"UPLOADED {n_up} objects{var_status}")

        # -------- verify mode (no R2 writes) --------
        # invalidate any stale stage for this ticker first
        if os.path.exists(t_dir):
            shutil.rmtree(t_dir)
        client = r2_client.get_client()
        existing_raw = r2_client.download_parquet(client, "raw", ticker)
        if existing_raw is None or len(existing_raw) == 0:
            return (ticker, "SKIP (no existing raw on R2)")
        new_win = _load_shard(ticker)

        # universe scope (review major): untouched tickers are not rewritten
        ex_dates = _et_dates(existing_raw["datetime"])
        has_old_window = bool(((ex_dates >= WIN_START) & (ex_dates <= WIN_END)).any())
        if new_win is None and not has_old_window:
            return (ticker, "SKIP (outside backfill scope)")

        existing_clean = r2_client.download_parquet(client, "clean", ticker)
        res = _recompute(ticker, existing_raw, existing_clean, new_win, run_id)
        _stage(ticker, res)
        m = res["metrics"]
        tag = "PASS" if m["gate_pass"] else "GATE-FAIL"
        return (ticker, f"{tag} old_win={m['old_window_rows']:,} new_win={m['new_window_rows']:,} "
                        f"days {m['window_days_old']}->{m['window_days_new']} "
                        f"vcorr={m.get('volume_corr')}")
    except Exception as e:
        os.makedirs(t_dir, exist_ok=True)
        with open(os.path.join(t_dir, "error.txt"), "w") as f:
            f.write(f"{type(e).__name__}: {e}")
        return (ticker, f"ERROR {type(e).__name__}: {str(e)[:120]}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["verify", "execute"], required=True)
    ap.add_argument("--tickers", nargs="*", help="subset for smoke runs")
    a = ap.parse_args()

    if not os.path.exists(os.path.join(BYTICKER, "_SHARD_OK")):
        raise SystemExit("shard stage not complete (_SHARD_OK missing)")
    run_id = _shard_run_id()

    gate = os.path.join(STAGED, "_VERIFY_OK")
    if a.mode == "execute":
        if not os.path.exists(gate):
            raise SystemExit("--mode execute requires the _VERIFY_OK gate file")
        with open(gate) as f:
            if json.load(f).get("run_id") != run_id:
                raise SystemExit("_VERIFY_OK run_id does not match current shard — re-verify")
    else:
        if os.path.exists(gate):
            os.remove(gate)  # a new verify invalidates any earlier approval

    with open(os.path.join(os.path.dirname(PIPELINE), "data", "tickers.json")) as f:
        universe = sorted(json.load(f))
    tickers = a.tickers or universe
    os.makedirs(STAGED, exist_ok=True)
    log(f"mode={a.mode} run_id={run_id} over {len(tickers)} tickers, {WORKERS} workers")

    results = {}
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(process_ticker, t, a.mode, run_id): t for t in tickers}
        for i, f in enumerate(as_completed(futs), 1):
            try:
                t, status = f.result()
            except Exception as e:  # worker crash
                t, status = futs[f], f"ERROR worker: {type(e).__name__}"
            results[t] = status
            if "ERROR" in status or "GATE-FAIL" in status or "VARIABLES-FAILED" in status:
                log(f"  !! {t}: {status}")
            if i % 50 == 0 or i == len(futs):
                log(f"{i}/{len(futs)} tickers done")

    summary = {"mode": a.mode, "run_id": run_id,
               "pass": sum(1 for s in results.values() if s.startswith("PASS")),
               "uploaded": sum(1 for s in results.values() if s.startswith("UPLOADED")),
               "variables_failed": sum(1 for s in results.values() if "VARIABLES-FAILED" in s),
               "gate_fail": sum(1 for s in results.values() if "GATE-FAIL" in s),
               "error": sum(1 for s in results.values() if s.startswith("ERROR")),
               "skip": sum(1 for s in results.values() if s.startswith("SKIP"))}
    out = os.path.join(STAGED, f"_run_summary_{a.mode}_{time.strftime('%Y%m%d_%H%M%S')}.json")
    with open(out, "w") as f:
        json.dump({"summary": summary, "results": results}, f, indent=1)
    log(f"DONE: {json.dumps(summary)}  -> {out}")


if __name__ == "__main__":
    main()

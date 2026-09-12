# Instruments behind `docs/VENDOR_SEAM_20260905.md`

Every number in that disclosure is produced by one of these, and three reviews in a row recorded the
same lesser finding: the doc cited instruments that existed only in a scratch directory, so a reader
could not re-derive anything. They live here now. Paths inside them still point at the scratch
working copies of the JSON they read; run them from a checkout with those present, or repoint the
constants at the top.

| file | what it establishes |
|---|---|
| `seam_K.csv` | the fleet measurement: per ticker `P`, `D`, `K`, `V`, spreads and a flag, over all 1,391 served tickers. Everything about seam SIZE derives from this. |
| `seam_coverage.py` | the coverage accounting: of the 1,391, which are applied, planned, flagged, or in no bucket. Found the three tickers pass 2b exists for. |
| `verify_prose_distribution.py` | re-derives the disclosure's headline distribution (216 / 54 / 401 / 325 / 122) from `seam_K.csv`. |
| `find_prose_quantity.py` | which column and banding reproduce that distribution — the answer is `K`, cut at 0.2 %, 2 %, 10 %, 50 %. Written after a first attempt banded `P` and got the same total with different buckets. |
| `verify_variables_delta.py` | per applied ticker, the served variables and quality objects against the pre-rebase snapshot: sessions differing pre- and post-seam, columns, the DATES removed and added, and per-column magnitudes in both relative and absolute terms. |
| `variables_delta_table.py` | renders the disclosure's variables section from that JSON. Nothing in it is typed. |
| `seam_applied_table.py` | renders the applied table, labelling each row's tool by evidence: the child's own hash line, else a pid-bearing record, else a file-time inference with its bound's provenance stated. |
| `check_removed_dates.py` | for every session the rebase removed from the variables, how many bars the served files still hold. Establishes that none was a full session. |
| `raw_prestale_magnitude.py` | the size of the raw pre-seam differences on scale-invariant columns, which separates the 6-decimal rounding artefact from staleness. |
| `check_later_events_applied.py` | for each ticker, whether every split after the seam is applied in the served daily data — measured across the event date itself, the R719 rule. |

The verification instruments for the rebase itself (`verify_applied_seam.py`, `verify_k_vs_events.py`,
`verify_market_truth.py`, `verify_aggregates.py`) are not here yet; they read snapshot directories on
`F:` that only the workstation has.

## Did the repair work? An INDEPENDENT check, added 2026-09-05

Everything above measures the seam or renders the disclosure. These answer a different question: after
pass 1 was applied to served data, is the defect actually gone? They do not read the tool's own VERIFY
line; they re-measure served objects against the pre-repair anchor `F:\hf_r2_snapshot_20260713`.

| file | what it establishes |
|---|---|
| `seam_step_now.py` | the headline. Per rebased ticker, the SEAM STEP itself — first post-seam session close over last pre-seam session close, where 1.0 means continuous — before and after. Writes `seam_step_now.csv`. |
| `seam_step_control.py` | **the control, and it changes the headline.** The seam spans 2022-03-04 to 03-07, a weekend in a violent week, so part of any residual is real market movement. Measures the same step on tickers the tool flagged as having NO seam and never touched. Writes `seam_step_control.csv`. |
| `residual_is_dividend.py` | tests the CAUSAL story for what is left, instead of asserting it: does each ticker's residual step equal the dividend factor `D` that `seam_K.csv` recorded independently? Mostly yes, and it names the four where it does not. |
| `verify_seam_live.py` | the bookkeeping cross-check: pre-seam bars must be exactly `K` times the anchor and volumes exactly `1/K`, post-seam bars unchanged. |
| `check_sti_uslv.py`, `sti_uslv_profile.py` | not about the seam. They test whether the reassigned-symbol split hazard (R732) has fired on STI and USLV, and what would make it fire. See R761 — a deadline was given for this hazard before its precondition was measured. |

What they measured on 2026-09-05T20:34-20:39Z, over all 67 rebased tickers, with none unmeasurable:

| | median seam step | within 10 % of continuous |
|---|---|---|
| before the repair | 0.2964 | 0 of 67 |
| after the repair | 0.9357 | 54 of 67 |
| control, never had a seam | 0.9520 | 33 of 38 |

Read that as: repaired tickers are now indistinguishable from tickers that never carried the defect.
The gap that remains is the DIVIDEND factor, which split-mode deliberately leaves in place pending the
price-basis convention decision. Do not quote the "after" column against a naive 1.0 — the control is
the comparison, and quoting 1.0 overstates what is left by about five percentage points.

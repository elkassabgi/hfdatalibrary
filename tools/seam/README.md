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

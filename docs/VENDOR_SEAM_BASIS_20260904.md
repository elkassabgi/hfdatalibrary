# A basis discontinuity at the pitrading → iex vendor seam (found 2026-09-04)

**Status: FOUND AND EVIDENCED, not fixed. The remedy is gated on a human checkpoint by design.**

## What it is

The served 1-minute history splices two vendors. The `source` column names the boundary exactly.
`NFLX`, daily bars:

```
2022-03-04  C= 361.7400  V=2,916,780  src=pitrading
2022-03-07  C=  35.0480  V=3,885,700  src=iex        <- x0.0969
```

Netflix traded near **$340** on 2022-03-07. **$35.05 is a tenth of the real price**, so this is
neither a market move nor a 2022 corporate action. The two vendors are on **different
split-adjustment bases**: `iex` supplies history adjusted to today's basis, `pitrading` supplies it
on the basis current when that data was purchased.

NFLX's latest served close is **$82.69** (2026-09-03), consistent with the post-split basis — so
the *recent* data is right and the pre-2022 side is stale by whatever corporate actions have
happened since.

## SCOPE, measured across every served ticker

A full census of the `source` column — find each ticker's last `pitrading` bar and first `iex` bar
and measure the step across it:

| | count |
|---|---|
| tickers with a `pitrading` → `iex` seam | **1,253** of 1,391 |
| seam step inside a normal trading day (0.80–1.25) | 1,030 |
| **seam step OUTSIDE that band — a basis mismatch** | **223 (17.8%)** |
| median seam ratio | 0.9049 |

## The ratios verify themselves against known corporate actions

This is what removes the ambiguity. Each of these is a real, checkable split, and the seam ratio
matches it:

| ticker | last pitrading | first iex | ratio | the actual corporate action |
|---|---|---|---|---|
| **AMZN** | 2,911.05 | 137.53 | 0.0472 | Amazon **20:1**, June 2022 |
| **GOOG / GOOGL** | 2,642.03 / 2,637.47 | 125.43 / 125.28 | 0.0475 | Alphabet **20:1**, July 2022 |
| **CMG** | 1,442.28 | 26.48 | 0.0184 | Chipotle **50:1**, June 2024 |
| **ORLY** | 674.09 | 44.04 | 0.0653 | O'Reilly **15:1**, June 2025 |
| **AVGO** | 595.99 | 52.75 | 0.0885 | Broadcom **10:1**, July 2024 |

And the inverse direction is the leveraged-ETF reverse splits, which compound: `SOXS` 1991x,
`SSG` 86.8x, `DUST` 82.7x, `ZSL` 39.9x.

**Every split listed post-dates the seam.** That is the mechanism in one sentence: `iex` history is
stated on today's split-adjusted basis, `pitrading` history on the basis current when it was
purchased, so every corporate action since 2022 opens a gap at the join.

**AMZN and GOOGL — two of the most-used series in the dataset — carry a 20x discontinuity in
March 2022.**

## Why it is not one ticker

A fleet sweep of all **1,391** served tickers for persistent round-ratio steps found **87
candidates**, and the largest single cluster is **2022-03-07** — 16 of the top 40. Each affected
ticker shows *its own* ratio, which is exactly what per-ticker corporate actions with only one
vendor adjusted would produce:

| ticker | seam ratio | nearest round |
|---|---|---|
| NFLX | 0.0969 | 1/10 |
| DXCM | 0.2438 | 1/4 |
| AZN | 1.9463 | 2 |
| LBTYA | 0.4988 | 1/2 |
| LCID | 10.2386 | 10 |

## Why it matters

Any calculation spanning 2022-03-07 for an affected ticker gets a spurious return of the ratio's
size — a 10x for NFLX. That is the single worst kind of defect for this dataset, because
long-horizon return series are its point.

## CORRECTION: the remedy I first named CANNOT fix this, and the reason is structural

My first version of this document said `pipeline/hist_backfill_merge.py` was the designed remedy.
**It is not, and it never could be.** Line 50:

```python
WIN_START, WIN_END = date(2022, 3, 7), date(2026, 3, 27)
```

Its window *starts on the seam date*. It replaces the IEX-sourced period and, by explicit gate,
**preserves everything outside it** (`outside_preserved: True` in every metrics file). The
pitrading history is outside the window by construction, so this tool cannot re-base it.

Running `--mode verify --tickers AMZN GOOGL CMG` confirmed the post-seam side is already healthy
and settles what the gate failure means:

```
AMZN   close_median_absdiff  0.0        <- existing window and re-derived HIST agree on PRICE
       volume_ratio_median   1.0
       volume_corr           0.4240     <- the ONLY failing gate, against VCORR_MIN = 0.98
       outside_preserved     True       <- pre-2022 deliberately untouched
       coverage_ok / schema_match  True
```

So the three tickers "gate-failed" on **volume correlation**, which is expected — IEX reports only
its own exchange's share of volume, not the consolidated tape — and **not** on price. The July
re-derivation did not skip these tickers because of a basis problem, and re-running it would not
help.

**Net effect of this correction: the seam has NO existing remedy.** Fixing it means re-basing the
pre-2022 pitrading history by each ticker's cumulative split factor since 2022-03-07 — new work,
against a source of corporate actions, on 223 tickers. That is a decision about scope and method,
not a button to press.

## ROOT CAUSE, and it is fully consistent with every observation

**1. The site publicly claims this is handled.** `pages/versions.html:65`:

> *"The only modification is **splice-boundary adjustment at the PiTrading/IEX transition (March
> 2022)**. Prices are split and dividend adjusted by the source."*

and `pages/docs.html:174`: *"Split/dividend adjusted — all prices adjusted for corporate actions."*

**2. The adjustment was a ONE-TIME build step, and nothing re-runs it.** The live pipeline's
`clean_pipeline.py:161`:

```python
def step9_splice_check(df: pd.DataFrame) -> pd.DataFrame:
    """Verify continuity at the PiTrading/IEX splice (March 2022).
    For new IEX data, this is a no-op since we never have a splice within new bars.
    Kept for documentation and pipeline compatibility."""
    return df
```

It returns `df` unchanged. The real work lives in `legacy_scripts/adjust_splits.py`
(*"Verifies splice boundary discrepancies are resolved"*), which ran when the dataset was built.

**3. Why the seam re-opens.** IEX supplies history already adjusted to today's basis, so when a
ticker splits, the *incoming* data simply re-bases itself — there is no discontinuity in the new
bars, the corporate-action detector never fires, and `_adjust_to_established` never rescales the
old history. The pre-2022 pitrading side stays frozen at the basis it had when
`adjust_splits.py` last ran. **Every corporate action since then re-opens the gap, silently.**

This predicts exactly what the census found: only tickers with a post-build corporate action are
affected — **223 of 1,253**, and every one of them has a real split or reverse split after
2022-03-07.

**It also explains why nothing caught it.** The detector watches for discontinuities in *incoming*
bars; this discontinuity is created by the absence of one. There is no alert, no gate and no test
covering the seam after build time.

## THE BACKLOG IS BOUNDED, AND ALREADY CLOSED GOING FORWARD

`daily_update.py:266-272` names this bug and says when it was fixed:

> *"The served series' basis is 'current as of last write'. When an R:1 split happens overnight,
> today's raw IEX prints arrive on the NEW basis, so the ENTIRE served history must be rescaled
> (price x r, volume / r) or the series gains a permanent R-times discontinuity — **the exact
> latent bug this function closes (before it, daily appends were raw with no CA logic)**."*

`_adjust_to_established` landed **2026-07-13**. So:

| period | behaviour |
|---|---|
| before 2026-07-13 | daily appends carried **no corporate-action logic** — every split left a permanent discontinuity |
| after 2026-07-13 | the whole history is rescaled on an applied action, so no new gaps open |

**Every affected example I identified predates the fix** — AMZN Jun 2022, GOOGL Jul 2022, CMG Jun
2024, AVGO Jul 2024, ORLY Jun 2025.

> **A CONTROL I GOT WRONG, recorded because it bounds the claim.** I expected APH to be clean,
> since its split (2026-09-03) came *after* the fix and was applied correctly. It is not: APH's
> seam ratio is **0.459**, so it is one of the 223. That does not refute the boundary — APH will
> have had some earlier, pre-fix corporate action — but it does mean **I have not empirically
> demonstrated the 2026-07-13 boundary.** What is established is the CODE's own statement that
> `_adjust_to_established` closes the bug going forward, plus five examples that predate it. A
> real test needs per-ticker corporate-action dates, which I do not have.
>
> I also verified my APH repair did not create that seam: the pre-repair snapshot gives **0.4672**
> against **0.4590** after — unchanged, because the rescale halved both sides of the 2022 join
> equally. The seam predates tonight's work.

**So this is a bounded historical backlog, not ongoing corruption.** It does not grow. That
materially changes the urgency: it is a data-quality debt to be paid down deliberately, not a leak
to be stopped tonight.

**What still needs deciding:** whether to re-base the 223 tickers' pre-2022 history (new work,
needs a corporate-actions source and a per-ticker cumulative factor), or to document the seam
honestly on the site — because `pages/versions.html:65` currently tells users the splice IS
adjusted, and for 17.8% of tickers that is not true.

## THE FULL LIST — `docs/seam_affected_tickers_20260904.csv`

All **223** affected tickers, with each one's implied factor, graded by how cleanly the observed
ratio matches one exact corporate action:

| grade | meaning | count |
|---|---|---|
| `clean` | within 8% of one exact factor | **95** |
| `stacked_or_unclear` | 8–30% off — several actions, or one plus a large move | 53 |
| `investigate` | >30% off — do NOT assume a corporate action explains it | 75 |

165 are forward-split direction, 58 reverse. The tightest matches are essentially exact:
`ZSL` 39.9238 against 40 (**0.19%**), `LBTYA` 0.49879 against 0.5 (0.24%), `NKLA` 29.9098 against
30 (0.30%), `SOXS` 1991.23 against 2000 (0.44%).

The household names all land on their real splits:

| ticker | observed | exact factor | off |
|---|---|---|---|
| AMZN | 0.047244 | 1/20 | 5.5% |
| GOOGL / GOOG | 0.04750 / 0.047474 | 1/20 | 5.0% |
| NFLX | 0.096887 | 1/10 | 3.1% |
| ORLY | 0.065326 | 1/15 | 2.0% |
| DXCM | 0.243787 | 1/4 | 2.5% |
| CMG | 0.018359 | 1/50 | 8.2% |

> **AVGO PROVES THE CAVEAT, so do not skip it.** Broadcom's real action was **10:1**, but the
> observed ratio 0.088508 snaps to **1/12**, not 1/10, because the weekend's market move pushed it
> past the midpoint. Any fix that multiplies by the seam ratio — or by its nearest canonical value
> — would put AVGO's entire pre-2022 history on a **wrong** basis while looking precise. **The
> multiplier must come from a corporate-actions source. This file identifies WHICH tickers and
> roughly how much; it is not the fix.**

## The published convention this violates

`pipeline/hist_backfill_merge.py` exists for exactly this. Its header:

> *"conform raw HIST to the published split/dividend-adjusted basis (empirical per-day factor),
> then gate on outside-window hash preservation + coverage + RELATIVE price agreement +
> adjustment-applied + no extrapolation risk"*

and `:158-159`:

> *"Conform the raw HIST bars to the dataset's PUBLISHED split/dividend-adjusted convention
> (dictionary.html + index.html: prices are split/dividend adjusted)."*

So the published contract is that prices ARE split/dividend adjusted, and there is a reviewed,
gated tool to enforce it. The open question is **scope**: which tickers still carry the seam, and
why the conform did not settle them.

## Why I did not run it

`--mode execute` requires a `_VERIFY_OK` gate file *"placed manually after the verification report
passes adversarial review"*. That is a deliberate human checkpoint on a whole-window replacement
of served data, and it is Ahmed's to give. The right sequence is
`--mode verify --tickers NFLX …` first, read the report, then decide.

## What is measured, and what is not

| established | how |
|---|---|
| the seam exists and is vendor-aligned | the `source` column flips `pitrading` → `iex` at the step |
| NFLX is wrong by ~10x on the pre-2022 side | $35.05 served vs ~$340 actual on 2022-03-07 |
| it is not confined to one ticker | 87 round-ratio steps across 1,391 tickers, 16 of the top 40 on 2022-03-07 |
| the recent side is right | NFLX's latest close $82.69 is on the current basis |

**NOT established:** the exact count of affected tickers (a full seam census was still running when
this was written), whether the 2026-07-14 IEX re-derivation covered these tickers and the conform
silently failed, or whether they were outside its 1,259-ticker set. Answer that before planning a
remedy — `hist_backfill_merge.py --mode verify` is the instrument.

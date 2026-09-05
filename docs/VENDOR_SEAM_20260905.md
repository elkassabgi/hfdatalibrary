# The March-2022 price seam — mechanism settled, repair started (2026-09-05)

Supersedes the root-cause section of `VENDOR_SEAM_BASIS_20260904.md`, which is kept for its
inventory and its record of the corrections made while measuring.

## What the served series is made of

| segment | source | price basis (measured) |
|---|---|---|
| through 2022-03-04 | PiTrading | **split-, dividend- and spin-off-adjusted as of 2022-03-04**, the vendor's file end. At 2016/2018/2020/2021 dates the served close equals the market close × Π(1 − D/P) over dividends paid between that date and 2022-03-04, to four decimals (MO 0.7050 vs 0.7048; KSS 0.7897 vs 0.7894; COO 0.9983 vs 0.9984; TSLA ≤0.14 %, IBM ≤0.03 %, PFE ≤0.17 %, T ≤0.11 % on the correct basis). |
| 2022-03-07 → 2026-03-27 | IEX trade records | raw prints **conformed by our merge step** (`pipeline/hist_backfill_merge.py:157-159`, "the dataset's PUBLISHED split/dividend-adjusted convention") to the previously served series — i.e. adjusted through 2026-03-27 (`WIN_END`). The served/market ratio steps at every ex-dividend date and stops at the last one ≤ 2026-03-27. |
| from 2026-03-28 | IEX daily feed | **raw** — the daily updater has no dividend step; each ex-date since then is a small unadjusted break on every payer. |

Nothing ever carried a corporate action after 2022-03-04 back across the seam. So every ticker
with a split **or a dividend** since March 2022 shows a step at 2022-03-07 that is not a market
move: AMZN 20× (split), MO ≈ 29 % (seventeen quarterly dividends), AVGO both. The 2026-09-04
inventory's 223 tickers were only those with a step beyond a 0.80–1.25 band; measured across all
1,391 served tickers on 2026-09-05, **1,118 could be measured and roughly three-quarters carry a
seam** (216 none, 54 within 2 %, 401 at 2–10 %, 325 at 10–50 %, 122 beyond 50 %).

The published sentences (`index.html` "Prices are split/dividend adjusted"; `pages/versions.html`
"The only modification is splice-boundary adjustment … adjusted by the source") are therefore false
for the IEX half and for every daily append since 2026-03-28. PR #10 carries the corrected wording.

## Two mistaken inferences, corrected on the record

- "PFE deviates because of its Viatris spin-off" — no: my basis conversion was backwards for split
  tickers; PiTrading adjusted the spin too (ledger R717).
- "The 2026-07-13 daily fix is empirically supported by four tickers" — no: MNST, SOXS and TECS
  matched the arithmetic because their later split was applied to **neither** half; residual
  arithmetic cannot distinguish "applied to both" from "applied to none" (ledger R719). Only APH,
  applied by hand, supports it.

## The repair

`pipeline/seam_rebase.py` (PR #8), split mode: measure the missing factor on the three
seam-adjacent sessions against the market, snap to an integer ratio, require the recorded events
to explain it and to be measured as applied in the served series, refuse any ticker whose later
half still carries an unapplied split, rescale raw **and** the served clean in place (the clean set
is not rescale-invariant; a re-clean would change bar membership), verify three ways from the
served side, keep a content-checked snapshot of all 22 objects. AMZN was rebased on 2026-09-05
01:54–01:57 and verified at bar level by an independent reviewer (1,871,960 pre-seam bars exactly
old × 0.05, 438,045 post-seam bars byte-identical). The rest of the split-component population runs
after the daily update of 2026-09-05; the dividend component waits for a decision on the
library's price convention (keep "dividend-adjusted", which requires rewriting every payer's
history at every ex-date, or move to split-adjusted only).

## Related

- 23 splits left unapplied in served 2026 data, repaired the same night: `UNAPPLIED_SPLITS_20260905.md` (PR #9).
- The detector fix that stops it recurring: PR #11.

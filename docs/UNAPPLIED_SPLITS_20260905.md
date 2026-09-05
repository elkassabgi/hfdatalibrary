# Stock splits left unapplied in served data — found, measured and repaired 2026-09-05

## What was wrong

Between 2026-04-06 and 2026-08-14, **23 stock splits on 22 tickers** happened while the served
1-minute history kept its pre-split price basis. A reader of BKNG saw its close fall from ~4,200 to
~168 on 6 April 2026 with no corresponding market event; the same in kind for KLAC (10:1), CVNA
(5:1), CRWD (4:1), IWF (4:1), MNST (2:1), DD (1:3), BYND (1:30), CDLX (1:10), IPW (1:8 then 1:9), and
eleven leveraged/inverse ETF reverse splits (EFZ, REW, RXD, SBB, SDP, SIJ, SMN, SZK 1:2 and SCO 1:4
on 2026-05-28; SKK 1:10 on 2026-04-06; SOXS and TECS 1:10 on 2026-07-15).

## Why

`pipeline/daily_update.py:_detect_and_apply_split` applies a split only when the overnight ratio
snaps to a round split ratio within 3 % **and** the open-period and late-day ratios snap to the same
ratio **and** the ratio is at least 3:1. Thin and sub-$1 names fail the first two tests on tick noise
alone — the log of the 2026-08-15 run reads

    [split_detect] !! BYND: overnight x31.860 vs 2026-08-13 (open x29.720, late x31.800) is
    split-sized but inconsistent/non-round — NOT applied, review

— and 2:1 sits under the floor by design (MNST: "BELOW the 3:1 auto-apply floor"). Each alert
reached the daily email under "⚠ Corporate actions" and was not actioned.

## How the population was measured

`pipeline/scan_unapplied_splits.py` (branch `fix/seam-rebase-tool`): for every served ticker with a
recorded split after 2022-03-04 (164 tickers), the served daily close step across each event divided
by the market's adjusted step; a value within 5 % of the raw factor means the split was never
applied. 31 tickers had such an event; `unapplied_repair_list.py` kept the 23 integer-ratio events
(real splits) and set aside 16 recorded spin-off / stock-dividend adjustment factors (BDX, GSK, JEF,
ILMN, SCCO ×8, J, FDX, HON, SPGI), which a split-only price convention correctly leaves alone.

## How they were repaired

`pipeline/repair_unapplied_splits.py` (this branch), one event at a time, in date order:

1. pre-check the served daily step against the market (must equal the listed ratio within 5 %), and
   the raw and clean 1-minute files' own step across the event on common bars (10-bar medians; a
   raw file already on the new basis refuses — the double-scaling guard);
2. snapshot every served object for the ticker (raw/, clean/, variables/, quality/, csv/ — 22
   objects) to `F:\hf_r2_snapshot_splits_20260905\<TICKER>_<DATE>` with size and MD5/ETag checks;
3. `manual_split TICKER RATIO CA_DATE` (PR #7's cutoff form): price × RATIO and volume ÷ RATIO on
   every bar strictly before the split session, then the pipeline's own re-clean, re-aggregate,
   re-upload and variables/quality recompute;
4. post-check, snapshot vs served: raw must be exactly × RATIO on every pre-event session and × 1 on
   every post-event session with no session dropped or added; clean's pre-event median must be exact
   with at most 1 % of sessions beyond 0.5 %; clean's post-event deviations and dropped/added sessions
   are logged, not failed (see below).

Result: **all 23 events applied and verified** (raw exact on every session for every ticker). Log:
`D:\temp\claude\repair_unapplied_splits.log`; the two applied by an independent reviewer's after-check
(BKNG, AMZN's seam rebase) and the 21 that followed are each recorded with their before/after step.

## What the repair does NOT change, and what it revealed about the clean version

The clean version is produced by a full re-clean of the rescaled raw. That re-clean is
**path-dependent on thin sessions**: REW lost two sparse post-event sessions and re-admitted 14
others; KLAC's clean closes moved on 1 of 5,900 pre-event sessions (0.53 %); CDLX's clean close on
2026-08-06 moved 11 %. On the thinnest inverse ETFs (RXD, SBB, SDP, SIJ, SZK) the served **clean
file has no bars at all after the split** — the cleaner rejects every sparse post-split session, so
`clean/RXD` ends on 2026-05-21 while raw runs on. None of this is introduced by the repair; it is how
the cleaner behaves on a full pass versus an incremental one, and the daily pipeline takes the same
full pass after its own automatic splits. It is recorded here so the Clean version's page can say so.

## Preventing recurrence

PR #11 (`fix/ca-alert-confirmation`) makes the detector confirm an alerted, split-sized move against a
**recorded split event on the same session** (±1 business day) whose factor is split-shaped (an
integer n:1 or 1:n, or 3:2 / 5:2 / 7:2 — never a spin-off adjustment factor such as DD's 2.39) and
matches the observed move within 20 %; when it does, the recorded ratio is applied; when it does not,
or the lookup fails, the alert stands and says which. Every alert and applied action is appended to
`data/ca_alerts.jsonl`, committed by the daily run. Reviewed adversarially; needs a canary run before
merge.

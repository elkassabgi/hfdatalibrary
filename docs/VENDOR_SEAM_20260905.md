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
06:54–06:57Z (the snapshot manifest's measured mtime is 06:54:47Z; an earlier draft wrote the
local-clock 01:54, UTC−5) and verified at bar level by an independent reviewer (1,871,960 pre-seam bars exactly
old × 0.05, 438,045 post-seam bars byte-identical). The rest of the split-component population runs
after the daily update of 2026-09-05; the dividend component waits for a decision on the
library's price convention (keep "dividend-adjusted", which requires rewriting every payer's
history at every ex-date, or move to split-adjusted only).

**Recorded events beyond Yahoo (added 2026-09-05, after reviews R725, R728 and R731).** Yahoo's
split record is empty for symbols renamed at their split (it does carry RZG's 3:1 of 2023-07-17),
so the tool takes an `--events-file` (ticker, date, ratio, source — the Invesco wave is SEC 497
accession 0001104659-23-077058) unioned with Yahoo's events. A recorded-event product within 1 %
of P is accepted as K only under three conditions: every event in it is a canonical split ratio
(spin-off factors such as MMM 1.196 and T 1.324 refuse); its newest event lies on or before the
last served session; and the post-seam half itself sits nearer the market than P does
(|ln D| < |ln(D/P_int)|) — a recorded split that reached neither half leaves the post half on the
pre-seam basis, and a date alone cannot tell "applied" from "never happened" (a phantom event one
session inside a dead series' range had planned a 10× cliff). That last test is silent when
P_int = 1 (dividend-only payers have no split to test), and a post half more than 1.5× off the
market refuses outright as a defect of its own. P/P_int is tested at 0.3 % before any write, a
window spread above 0.6 % refuses, and everything from the first upload to the last verification
line runs inside one guard: a failure or a mismatch restores the snapshot automatically (exit 1),
a failed restore stops everything (exit 4). Of the eight tickers first refused for "no recorded
event", five are dead series whose post half never carried the split (RYH, RYT, RTM, RGI, RYU),
two are real K=3 seams on series dead since 2023-08 (PSJ, PWC — their post halves do carry the
3:1), and RZG is a real K=3 seam that Yahoo records.

## Related

- 23 splits left unapplied in served 2026 data, repaired the same night: `UNAPPLIED_SPLITS_20260905.md` (PR #9).
- The detector fix that stops it recurring: PR #11.

## What was applied on 2026-09-05

Pass 1 (`D:/temp/claude/seam_pass1_plan67.txt`, 66 PLAN tickers + RZG), `--mode split` only: the pre-2022-03-07 half of each served series (raw and clean 1-minute files, the 14 timeframe aggregates, variables and quality) was multiplied by K on price and divided by K on volume, where K is the integer split ratio the tool measured between the two halves and matched to the recorded split events after the seam; nothing after 2022-03-07 changed; the dividend/spin factor D is NOT applied (Ahmed's convention decision). Each ticker ran under the tool version named, in bounded batches, each batch after an adversarial review of the tool (ledger R719/R720, R725, R728, R731, R735, R738); every ticker has a content-checked snapshot of its 22 objects under `F:/hf_r2_snapshot_seam_20260905/<T>` (with `_RESULT.txt` from v5.1 on) and, from 13:18Z, its full run output under `D:/temp/claude/seam_detail_<UTC>_<T>.txt`. The table is generated from the batch log by `D:/temp/claude/seam_applied_table.py` and regenerated at the end of the pass; "verified bar for bar" means `verify_applied_seam.py` read the served 1-minute raw and daily back and found every pre-seam bar equal to snapshot x K (prices to 6 dp, volumes rounded) and every post-seam bar identical. Held back for a pass 1b: BIRD, EVX, PCAR, RENT (their PLAN came from code changes in R725/R728 and has no independent confirmation yet); pass 2 = 27 ETFs (`seam_pass2_plan27.txt`).

| UTC | ticker | exit | secs | K (price) | pre-seam 1-min bars | mismatches price/volume/post | tool | outcome |
|---|---|---|---|---|---|---|---|---|
| 2026-09-05T12:48:41Z | AMCR | 0 | 74s | - | - | - | v5 (9ba6a3f) |   DONE: AMCR rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T12:49:55Z | AMWL | 0 | 61s | - | - | - | v5 (9ba6a3f) |   DONE: AMWL rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T12:50:56Z | ANET | 0 | 100s | - | - | - | v5 (9ba6a3f) |   DONE: ANET rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:18:12Z | APH | 0 | 154s | - | - | - | v5.1 (a38144c) |   DONE: APH rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T13:20:46Z | AVGO | 0 | 126s | - | - | - | v5.1 (a38144c) |   DONE: AVGO rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:22:52Z | AZN | 0 | 158s | - | - | - | v5.1 (a38144c) |   DONE: AZN rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T13:25:30Z | CHPT | 0 | 62s | - | - | - | v5.1 (a38144c) |   DONE: CHPT rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:26:32Z | CMG | 0 | 137s | - | - | - | v5.1 (a38144c) |   DONE: CMG rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T13:28:49Z | COO | 0 | 154s | - | - | - | v5.1 (a38144c) |   DONE: COO rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T13:31:23Z | CPRT | 0 | 161s | - | - | - | v5.1 (a38144c) |   DONE: CPRT rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:34:04Z | CTAS | 0 | 152s | - | - | - | v5.1 (a38144c) |   DONE: CTAS rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:36:37Z | DECK | 0 | 194s | - | - | - | v5.1 (a38144c) |   DONE: DECK rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:39:50Z | DXCM | 0 | 144s | - | - | - | v5.2 (daef642) |   DONE: DXCM rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:43:07Z | ETR | 0 | 158s | - | - | - | v5.2 (daef642) |   DONE: ETR rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T13:45:45Z | FAST | 0 | 150s | - | - | - | v5.2 (daef642) |   DONE: FAST rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:48:15Z | FTNT | 0 | 127s | - | - | - | v5.2 (daef642) |   DONE: FTNT rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:50:23Z | FUBO | 0 | 68s | - | - | - | v5.2 (daef642) |   DONE: FUBO rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:51:31Z | GME | 0 | 152s | - | - | - | v5.2 (daef642) |   DONE: GME rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T13:54:03Z | GOOG | 0 | 105s | - | - | - | v5.2.1 (fc9d33a) |   DONE: GOOG rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T13:55:48Z | GOOGL | 0 | 160s | - | - | - | v5.2.1 (fc9d33a) |   DONE: GOOGL rebased (split); snapshot kept at F:\hf_r2_sna |
| 2026-09-05T13:58:28Z | IBKR | 0 | 135s | - | - | - | v5.2.1 (fc9d33a) |   DONE: IBKR rebased (split); snapshot kept at F:\hf_r2_snap |
| 2026-09-05T14:00:43Z | IGM | 0 | 103s | - | - | - | v5.2.1 (fc9d33a) |   DONE: IGM rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T14:02:26Z | IGV | 0 | 116s | - | - | - | v5.2.1 (fc9d33a) |   DONE: IGV rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T14:04:22Z | IHE | 0 | 88s | - | - | - | v5.2.1 (fc9d33a) |   DONE: IHE rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T14:05:50Z | IHF | 0 | 95s | - | - | - | v5.2.1 (fc9d33a) |   DONE: IHF rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T14:07:25Z | IJH | 0 | 139s | - | - | - | v5.2.1 (fc9d33a) |   DONE: IJH rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T14:09:44Z | IYG | 0 | 112s | 0.333333 | 602,483 | 0/0/0 | v5.2.1 (fc9d33a) |   DONE: IYG rebased (split); snapshot kept at F:\hf_r2_snaps |
| 2026-09-05T14:11:36Z | IYH | 0 | 110s | 0.2 | 583,425 | 0/0/0 | v5.2.1 (fc9d33a) |   DONE: IYH rebased (split); snapshot kept at F:\hf_r2_snaps |

28 rebased (exit 0 DONE), 2 of them verified bar for bar by verify_applied_seam.py at the time of this table; 0 other exit(s). Generated 2026-09-05T14:14:19Z from D:/temp/claude/seam_rebase_batch_pass1.log.

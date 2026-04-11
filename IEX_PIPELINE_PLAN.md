# IEX HIST Weekly Data Pipeline — Build Plan

**Goal:** Fully automated pipeline that pulls fresh trade data from IEX Exchange HIST every Sunday, processes it, and updates the HF Data Library on R2 — without any manual intervention.

**Constraint:** Must run within GitHub Actions free public-repo limits:
- 6 hours max per run
- 14 GB disk
- 7 GB RAM
- 2 CPU cores

**Legal:** IEX HIST data is public and redistributable with attribution. Alpaca is NOT used.

---

## Architecture overview

```
                      ┌──────────────────────────────┐
                      │  GitHub Actions (Sunday cron) │
                      └──────────────┬───────────────┘
                                     │
                  ┌──────────────────┴───────────────────┐
                  │   1. fetch_iex_manifest.py            │
                  │   Get list of pcap files for the     │
                  │   past 7 trading days from IEX HIST   │
                  └──────────────────┬───────────────────┘
                                     │
                  ┌──────────────────┴───────────────────┐
                  │   2. download_and_parse_day.py        │
                  │   For each day:                       │
                  │   - Stream-download pcap (no full DL) │
                  │   - Parse IEX-TP/TOPS messages        │
                  │   - Filter to 1,391 ticker universe   │
                  │   - Aggregate trades → 1-min bars     │
                  │   - Save day's bars to local disk     │
                  │   - DELETE pcap to free disk          │
                  └──────────────────┬───────────────────┘
                                     │
                  ┌──────────────────┴───────────────────┐
                  │   3. merge_with_existing.py           │
                  │   For each ticker:                    │
                  │   - Download existing parquet from R2 │
                  │   - Append new days' bars             │
                  │   - Re-apply 9-step cleaning          │
                  │   - Upload back to R2 (raw + clean)   │
                  └──────────────────┬───────────────────┘
                                     │
                  ┌──────────────────┴───────────────────┐
                  │   4. regenerate_aggregates.py         │
                  │   For each ticker:                    │
                  │   - Re-aggregate to 5min/15min/...    │
                  │   - Upload aggregated parquets        │
                  └──────────────────┬───────────────────┘
                                     │
                  ┌──────────────────┴───────────────────┐
                  │   5. update_metadata.py               │
                  │   - Update metadata.json (timestamps,│
                  │     ticker counts, bar counts)        │
                  │   - Commit + push to git              │
                  │   - Triggers Pages deploy             │
                  └──────────────────┬───────────────────┘
                                     │
                            ┌────────┴─────────┐
                            │   Email summary  │
                            │   (success/fail) │
                            └──────────────────┘
```

---

## Stage 1 — IEX HIST manifest fetcher

**File:** `scripts/fetch_iex_manifest.py`

**What it does:**
- Calls `https://iextrading.com/api/1.0/hist?date=YYYYMMDD` for each of the past 7 trading days
- Parses the JSON response to find the **TOPS** pcap download URL for each day
- Returns a list: `[(date, pcap_url, size_bytes), ...]`

**Notes:**
- IEX HIST API is free, no key required
- Response format: `{"YYYYMMDD": [{"link": "...", "feed": "TOPS", "version": "...", "protocol": "IEXTP1"}]}`
- Some days won't have pcap files (weekends, holidays — skip them)

---

## Stage 2 — IEX TOPS parser

**File:** `scripts/iex_tops_parser.py`

**What it does:**
- Streams a pcap file from a URL (or local path)
- Parses the IEX-TP transport protocol headers
- Extracts **Trade Report Messages** (message type `T`)
- For each trade, emits `(symbol, timestamp_ns, price, size)`

**IEX TOPS message types we care about:**
- `T` — Trade Report Message (the only one we need for OHLCV)
- We can ignore: quote updates, system events, security event messages

**Trade Report Message format (binary, little-endian):**
| Offset | Size | Field |
|---|---|---|
| 0 | 1 | Message type ('T' = 0x54) |
| 1 | 1 | Sale Condition Flags |
| 2 | 8 | Timestamp (nanoseconds since epoch) |
| 10 | 8 | Symbol (8-byte right-padded ASCII) |
| 18 | 4 | Size (uint32, shares) |
| 22 | 8 | Price (int64, fixed-point with 4 decimal places) |
| 30 | 8 | Trade ID |

**pcap parsing:**
- pcap global header: 24 bytes
- pcap record header: 16 bytes (timestamp + length)
- Then the IEX message
- IEX-TP transport header wraps multiple messages per UDP packet

**Library options:**
- Pure Python `struct` module (no dependencies) — slowest but works in GitHub Actions out of the box
- `dpkt` — faster pcap parsing, pure Python
- Custom approach: skip pcap headers entirely, treat the file as a stream of IEX-TP segments

**Decision:** Start with pure Python `struct` for portability. Optimize later if too slow.

---

## Stage 3 — Minute bar aggregator

**File:** `scripts/build_bars.py`

**What it does:**
- Takes a stream of trades from the parser
- Filters to our 1,391 ticker universe (load from `tickers.json`)
- Buckets trades into 1-minute windows by Eastern Time
- Computes OHLCV for each bucket: `Open=first, High=max, Low=min, Close=last, Volume=sum`
- Outputs one parquet file per ticker per day

**Time zone handling:**
- IEX timestamps are nanoseconds since Unix epoch (UTC)
- Convert to America/New_York for the bar bucketing
- Only keep bars during trading hours (09:30–15:59 ET)

**Output:** `data/new_bars/{date}/{ticker}.parquet`

---

## Stage 4 — Merge with existing R2 data

**File:** `scripts/merge_into_r2.py`

**What it does:**
- For each ticker that has new bars:
  1. Download the existing `raw/{ticker}.parquet` from R2 (if exists)
  2. Append the new bars (deduplicated by timestamp)
  3. Upload back to R2 as `raw/{ticker}.parquet`
  4. Apply the 9-step cleaning pipeline to produce `clean/{ticker}.parquet`
  5. Upload the cleaned version

**Storage strategy:**
- Always full-file replace (no partial updates)
- Each ticker's parquet is rewritten with the latest data appended
- Same approach for raw and clean

---

## Stage 5 — Regenerate aggregations

**File:** `scripts/regenerate_aggregates.py`

**What it does:**
- For each updated ticker, re-runs the existing aggregation logic (5-min/15-min/.../monthly)
- Uploads each timeframe back to R2
- Same paths we already use: `{version}/{timeframe}/{ticker}.parquet`

---

## Stage 6 — Update metadata + redeploy

**File:** Inline in the GitHub Actions workflow

**What it does:**
- Update `data/metadata.json`:
  - `data_updated`: today's date
  - `update_summary`: "Weekly update: added trading data through {latest_date}"
  - Recompute bar counts if changed
- Commit and push to git
- Pages auto-deploys on push

---

## Stage 7 — Email notification

**What it does:**
- On success: send a brief summary email to admin
- On failure: send error log + which step failed
- Uses Resend API (already configured)

---

## GitHub Actions workflow

**File:** `.github/workflows/weekly-update.yml` (replaces the existing stub)

```yaml
name: Weekly Data Update
on:
  schedule:
    - cron: '0 6 * * 0'  # Sunday 6 AM UTC
  workflow_dispatch:

jobs:
  update:
    runs-on: ubuntu-latest
    timeout-minutes: 350  # Just under 6 hours
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install requests pandas pyarrow boto3

      - name: Run weekly update
        env:
          R2_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
          R2_ACCESS_KEY: ${{ secrets.R2_ACCESS_KEY_ID }}
          R2_SECRET_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}
          RESEND_API_KEY: ${{ secrets.RESEND_API_KEY }}
        run: |
          python scripts/weekly_update.py

      - name: Commit and push metadata
        run: |
          git config user.name "HF Data Library Bot"
          git config user.email "admin@hfdatalibrary.com"
          git add data/metadata.json
          git diff --cached --quiet || git commit -m "Weekly data update: $(date -u +%Y-%m-%d)"
          git push

      - name: Deploy site
        uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          accountId: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
          command: pages deploy . --project-name=hfdatalibrary --branch=main
```

---

## Disk usage analysis (the critical constraint)

**Per day of IEX TOPS data:**
- Compressed pcap: ~1-3 GB
- Uncompressed: ~5-15 GB

**Strategy to fit in 14 GB:**
- Process ONE day at a time
- Stream-decompress (don't write full uncompressed file to disk)
- After processing each day, delete the pcap file
- Keep only the parsed bars (a few MB per ticker)

**Pseudocode:**
```python
for day in past_7_trading_days:
    stream = download(day_url)  # generator, not full file
    for trade in parse_pcap_stream(stream):  # generator
        if trade.symbol in TICKER_UNIVERSE:
            add_to_bar(trade)
    save_day_bars_to_disk(day)  # ~50-100 MB total
    # No pcap file ever fully on disk
```

---

## R2 access from GitHub Actions

We need to give GitHub Actions write access to R2. Options:

**Option A — R2 S3-compatible API (recommended)**
- Create R2 API tokens in Cloudflare dashboard
- Get `Access Key ID` and `Secret Access Key`
- Use `boto3` (Python S3 client) to talk to R2
- Endpoint: `https://{account_id}.r2.cloudflarestorage.com`
- More secure, scoped to specific buckets

**Option B — Cloudflare API token** (current approach in our scripts)
- Less standard, but works
- Already have the token

**Decision:** Use Option A for the production pipeline. Better tooling, easier to monitor.

**You'll need to:**
1. Go to Cloudflare R2 dashboard
2. Create an R2 API token with **Object Read & Write** permissions for the `hfdatalibrary-data` bucket
3. Save the Access Key ID and Secret Access Key
4. Add both as GitHub Actions secrets: `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`

---

## Test plan

Before wiring into GitHub Actions, validate locally:

1. **Manifest test:** Fetch the manifest for one day, verify pcap URL is reachable
2. **Parser test:** Parse one day's pcap, count total trade messages, sanity-check vs IEX volume
3. **Bar test:** Build bars for AAPL, compare to existing AAPL Clean parquet for the same dates — should match closely
4. **End-to-end test:** Run the full pipeline locally for one day, verify the output is correct
5. **GitHub Actions test:** Trigger the workflow manually with `workflow_dispatch`, monitor the run

---

## Realistic timeline for this session

| Stage | Estimated time |
|---|---|
| 1. Manifest fetcher | 30 min |
| 2. TOPS parser | 2-3 hours (the hard part) |
| 3. Bar aggregator | 1 hour |
| 4. R2 merger | 1 hour |
| 5. Aggregation regen | 30 min (reuses existing code) |
| 6. GitHub Actions workflow | 30 min |
| 7. Local testing | 1 hour |
| **Total** | **6-7 hours of focused work** |

**This is a substantial build.** It's doable in one long session, but we may not finish testing today. I can build all the scripts, then we test stage by stage.

---

## What you'll need to do (one-time setup)

1. **Create R2 API token** in Cloudflare dashboard (Object Read & Write for `hfdatalibrary-data` bucket)
2. **Add 2 GitHub secrets:** `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`

That's it. After that, the pipeline runs automatically every Sunday.

---

## Approval needed

If this plan looks good, I'll start with Stage 1 (manifest fetcher) and work through them one by one, verifying each works before moving on.

Open questions:
1. Are you OK with this scope and approach?
2. Do you have any concerns about the GitHub Actions disk constraint?
3. Should the email summary go to your Gmail (admin@) or somewhere else?

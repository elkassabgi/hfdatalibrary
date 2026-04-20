---
license: cc-by-4.0
language:
  - en
pretty_name: HF Data Library
tags:
  - finance
  - high-frequency
  - intraday
  - OHLCV
  - market-microstructure
  - financial-econometrics
  - equity
  - ETF
  - realized-volatility
size_categories:
  - 1B<n<10B
task_categories:
  - time-series-forecasting
  - tabular-regression
---

# HF Data Library: High-Frequency U.S. Equity Data

[![Website](https://img.shields.io/badge/Website-hfdatalibrary.com-2563eb)](https://hfdatalibrary.com) [![DOI](https://img.shields.io/badge/DOI-10.5281%2Fzenodo.19501605-blue)](https://doi.org/10.5281/zenodo.19501605) [![License: CC BY 4.0](https://img.shields.io/badge/License-CC_BY_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)

Free, research-grade collection of OHLCV (Open-High-Low-Close-Volume) data for **1,391 U.S. equities and ETFs**, covering December 2002 through the present (45 tickers extending to January 1991). Data is available in multiple timeframes from 1-minute up to monthly. Updated weekly via automated pipeline.

**Maintainer:** Ahmed Elkassabgi, University of Central Arkansas
**ORCID:** [0000-0002-5926-7493](https://orcid.org/0000-0002-5926-7493)
**Permanent DOI:** [10.5281/zenodo.19501605](https://doi.org/10.5281/zenodo.19501605)

## Where to download

**This Hugging Face repository contains documentation only.** The actual data is hosted at:

➡️ **https://hfdatalibrary.com**

Free registration required (email, ORCID, or Google). Data is available as direct downloads (Parquet or CSV) or via REST API at `https://api.hfdatalibrary.com`.

## What's in the dataset

- **1,391 tickers** of U.S. equities and ETFs
- **1.53 billion** 1-minute bars (clean version)
- **December 2002 – present** (with 45 tickers extending to January 1991)
- **Weekly automated updates**

### Cleaning versions

Two cleaning versions are provided:

- **Raw:** as received from the source, no modifications
- **Clean:** nine-step cleaning pipeline applied (outside-hours removal, OHLC violations, duplicates, Brownlees-Gallo outlier filter, splice-boundary adjustment)

A gap-filled version is intentionally **not** distributed — see the accompanying paper for documented biases introduced by LOCF gap-filling. Researchers who need a regular grid can apply LOCF to the Clean version themselves.

### Available timeframes

All cleaning versions are aggregated into multiple timeframes:

| Timeframe | Description |
|---|---|
| 1-minute | Base data (highest resolution) |
| 5-minute | Aggregated from 1-minute |
| 15-minute | Aggregated from 1-minute |
| 30-minute | Aggregated from 1-minute |
| Hourly | Aggregated from 1-minute |
| Daily | Open-to-close per trading day |
| Weekly | Aggregated to trading weeks |
| Monthly | Aggregated to calendar months |

### Pre-computed academic variables

25 variables computed daily for each ticker in each cleaning version:

**Volatility (5):** Realized variance (1-min and 5-min sampling), bipower variation (BNS 2004), Parkinson (1980), Yang-Zhang (2000)

**Spreads (2):** Roll (1984) implied spread, Corwin-Schultz (2012) high-low spread

**Autocorrelation (3):** First-order return autocorrelation, variance ratio (5-min), variance ratio (10-min)

**Jump detection (3):** BNS z-statistic, BNS jump indicators at 1% and 5% levels

**Liquidity (4):** Amihud (2002) illiquidity ratio, daily dollar volume, share volume, observed trade count

**Data quality (4):** Gap rate, observed bars per day, longest gap, max bars since last trade

**Returns (4):** Open-to-close return, overnight return, daily high-low range, intraday return standard deviation

## Data sources

- **Pre-March 2022:** PiTrading, derived from the consolidated tape (CTA/UTP)
- **Post-March 2022:** IEX Exchange HIST

## Quick start (Python)

```python
import requests
import pandas as pd
from io import BytesIO

# Register at https://hfdatalibrary.com to get an API key
API_KEY = "your-key-here"

# Get a download token (links expire after 10 minutes)
r = requests.get(
    "https://api.hfdatalibrary.com/v1/download-token/AAPL",
    params={"version": "clean", "format": "parquet", "timeframe": "1min"},
    headers={"X-API-Key": API_KEY}
)
url = r.json()["url"]

# Download the file
data = requests.get(url).content
df = pd.read_parquet(BytesIO(data))
print(df.head())
```

## File schema

Each ticker is a single Parquet (or CSV) file. For 1-minute data:

| Column | Type | Description |
|---|---|---|
| datetime | datetime64 | Bar timestamp (Eastern Time) |
| Open | float64 | Opening price (split/dividend adjusted) |
| High | float64 | Highest price during the bar |
| Low | float64 | Lowest price during the bar |
| Close | float64 | Closing price |
| Volume | int64 | Shares traded |
| source | string | "pitrading" (pre-2022) or "iex" (post-2022) |

Higher timeframes (5-min, 15-min, daily, etc.) follow the same schema but with the `datetime` column resampled to the chosen interval.

## License

This dataset is licensed under [Creative Commons Attribution 4.0 International](https://creativecommons.org/licenses/by/4.0/).

You are free to share and adapt the material for any purpose, including commercially, provided you give appropriate credit.

## How to cite

```bibtex
@dataset{elkassabgi2026hfdatalibrary,
  author    = {Elkassabgi, Ahmed},
  title     = {{HF Data Library: High-Frequency U.S. Equity Data (1-Minute OHLCV)}},
  year      = {2026},
  version   = {1.0},
  publisher = {Zenodo},
  doi       = {10.5281/zenodo.19501605},
  url       = {https://hfdatalibrary.com}
}
```

## Links

- **Website:** https://hfdatalibrary.com
- **API:** https://api.hfdatalibrary.com
- **Documentation:** https://hfdatalibrary.com/pages/docs
- **Data dictionary:** https://hfdatalibrary.com/pages/dictionary
- **Code samples:** https://hfdatalibrary.com/pages/code
- **GitHub:** https://github.com/elkassabgi/hfdatalibrary
- **Zenodo:** https://zenodo.org/records/19501605
- **Contact:** admin@hfdatalibrary.com

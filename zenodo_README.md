# HF Data Library — High-Frequency U.S. Equity Data

**Author:** Ahmed Elkassabgi
**Affiliation:** University of Central Arkansas
**ORCID:** 0000-0002-5926-7493
**Version:** 1.0
**Release date:** 2026-04-10
**License:** Creative Commons Attribution 4.0 International (CC BY 4.0)
**DOI:** 10.5281/zenodo.19501605

---

## About this dataset

The HF Data Library is a free, research-grade collection of 1-minute OHLCV (Open-High-Low-Close-Volume) data for 1,391 U.S. equities and ETFs. The library is designed for academic research in market microstructure, financial econometrics, and empirical finance.

**Coverage:**
- 1,391 U.S. equities and ETFs
- December 30, 2002 through the present (45 tickers extending back to January 1991)
- Updated weekly via automated pipeline

**Data sources:**
- Pre-March 2022: PiTrading, derived from the consolidated tape (CTA/UTP)
- Post-March 2022: IEX Exchange HIST

---

## Accessing the data

**This Zenodo record serves as a citable reference for the dataset.** The actual data is hosted separately at:

**https://hfdatalibrary.com**

Download options:
- **Browser downloads**: individual tickers, pre-packaged bundles (S&P 500, Nasdaq 100, sector), or the full dataset
- **REST API**: https://api.hfdatalibrary.com/v1
- **Formats**: Parquet (recommended for Python/R) and CSV (for Stata/Excel)

Registration is free; users are authenticated via email, ORCID, or Google.

---

## Cleaning versions

Two cleaning versions are provided:

### Raw
Data as received from the source. No outlier removal, no gap-filling. Prices are split and dividend adjusted.

### Clean
Nine-step cleaning pipeline applied:
1. Remove bars outside 09:30–16:00 ET
2. Remove bars with non-positive prices
3. Remove OHLC violations (High < Low)
4. Remove bars where Open or Close falls outside [Low, High]
5. Remove duplicate timestamps
6. Remove bars with zero volume
7. Remove extreme outliers (|log return| > 25%)
8. Brownlees-Gallo adaptive outlier filter (3 × MAD over 50-bar window)
9. Splice-boundary adjustment (verify continuity at PiTrading/IEX transition)

A gap-filled version is intentionally not distributed. See the paper for discussion of the biases introduced by LOCF gap-filling.

---

## Pre-computed academic variables

25 variables computed daily for each ticker in each cleaning version:

**Volatility (5 measures)**
- Realized variance (5-minute sampling)
- Realized variance (1-minute sampling)
- Bipower variation (Barndorff-Nielsen and Shephard 2004)
- Parkinson (1980) range-based volatility
- Yang-Zhang (2000) OHLC-based volatility

**Spreads (2 measures)**
- Roll (1984) implied spread
- Corwin-Schultz (2012) high-low spread

**Autocorrelation (3 measures)**
- First-order return autocorrelation
- Variance ratio (5-minute)
- Variance ratio (10-minute)

**Jump detection (3 measures)**
- BNS z-statistic
- BNS jump indicator (1% level)
- BNS jump indicator (5% level)

**Liquidity (4 measures)**
- Amihud (2002) illiquidity ratio
- Daily dollar volume
- Daily share volume
- Number of trades (observed bars)

**Data quality (4 measures)**
- Gap rate
- Observed bars per day
- Longest gap (consecutive missing bars)
- Max bars since last trade

**Returns (4 measures)**
- Open-to-close return
- Overnight return
- Daily high-low range
- Intraday return standard deviation

---

## File format

One Parquet file per ticker. Columns:

| Column | Type | Description |
|---|---|---|
| datetime | datetime64 | Bar timestamp (Eastern Time) |
| Open | float64 | Opening price (split/dividend adjusted) |
| High | float64 | Highest price during the 1-minute bar |
| Low | float64 | Lowest price during the 1-minute bar |
| Close | float64 | Closing price |
| Volume | int64 | Shares traded during the bar |
| source | string | "pitrading" (pre-2022) or "iex" (post-2022) |

---

## How to cite

If you use this data in your research, please cite it as:

> Elkassabgi, Ahmed. 2026. *HF Data Library: High-Frequency U.S. Equity Data* (version 1.0) [Dataset]. Zenodo. https://doi.org/10.5281/zenodo.19501605

### BibTeX

```bibtex
@dataset{elkassabgi2026hfdatalibrary,
  author    = {Elkassabgi, Ahmed},
  title     = {{HF Data Library: High-Frequency U.S. Equity Data}},
  year      = {2026},
  version   = {1.0},
  publisher = {Zenodo},
  doi       = {10.5281/zenodo.19501605},
  url       = {https://hfdatalibrary.com}
}
```

---

## License

This dataset is licensed under the Creative Commons Attribution 4.0 International License (CC BY 4.0). You are free to:

- **Share** — copy and redistribute the material in any medium or format
- **Adapt** — remix, transform, and build upon the material for any purpose, including commercially

Under the following terms:

- **Attribution** — you must give appropriate credit to Ahmed Elkassabgi and the HF Data Library, provide a link to the license, and indicate if changes were made.

Full license text: https://creativecommons.org/licenses/by/4.0/

---

## Contact

**Email:** admin@hfdatalibrary.com
**Website:** https://hfdatalibrary.com
**GitHub:** https://github.com/elkassabgi/hfdatalibrary
**ORCID:** https://orcid.org/0000-0002-5926-7493

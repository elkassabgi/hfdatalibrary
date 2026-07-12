# HF Data Library

Free 1-minute OHLCV bars for 1,391 U.S. stocks and ETFs, December 2002 → present, updated daily.

**Website / downloads:** https://hfdatalibrary.com · **API:** https://api.hfdatalibrary.com · **DOI:** [10.5281/zenodo.19501605](https://doi.org/10.5281/zenodo.19501605)

## Data sources

| Period | Source | Coverage |
|---|---|---|
| Dec 2002 – Mar 2022 | PiTrading (consolidated tape, CTA/UTP) | All U.S. equity exchanges |
| Mar 2022 – present | IEX Exchange market data (HIST pcap files) | IEX exchange only, ~2–3% of consolidated volume — a reference point, not the full tape |

Post–March 2022 bars reflect IEX-only trading activity. Some tickers have no IEX trades on a given day. See [Known Issues](https://hfdatalibrary.com/pages/issues) before using the post-2022 segment for research.

## Upstream source attribution (required)

Bars dated 2022-03-07 onward are derived from IEX market data. If you redistribute or provide access to this data, include:

> Data provided for free by IEX. By accessing or using IEX Historical Data, you agree to the [IEX Historical Data Terms of Use](https://www.iex.io/legal/hist-data-terms).

IEX retains all rights in the underlying securities information.

## License

The compilation, documentation, and pre-2022 portion are licensed [CC BY 4.0](https://hfdatalibrary.com/pages/license) — cite *Elkassabgi, A. (2026). HF Data Library: Free 1-Minute Intraday U.S. Equity Data. Zenodo. https://doi.org/10.5281/zenodo.19501605*. Post–March 2022 bars additionally remain subject to the IEX Historical Data Terms of Use above.

## Repository contents

This repo holds the website (`index.html`, `pages/`), the Cloudflare Worker API (`api/`), and the daily data pipeline (`pipeline/` — IEX HIST pcap download, Go TOPS parser, bar building, 9-step cleaning, R2 upload). The data itself is distributed from https://hfdatalibrary.com, not from this repository.

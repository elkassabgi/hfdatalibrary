# hfdatalibrary (Python)

Python client for the [HF Data Library](https://hfdatalibrary.com) — free,
research-grade 1-minute OHLCV data for ~1,391 U.S. equities and ETFs, in
both `raw` and `clean` versions across eight timeframes.

## Install

```bash
pip install hfdatalibrary          # add pyarrow for parquet: pip install hfdatalibrary[parquet]
```

## Authenticate

Get a free API key at <https://hfdatalibrary.com/pages/account>, then:

```python
import hfdatalibrary as hfdl
hfdl.set_key("YOUR_API_KEY")        # or set the HFDL_API_KEY environment variable
```

## Use

```python
hfdl.symbols()                                   # -> ['A', 'AA', 'AAPL', ...]
df = hfdl.get("AAPL")                             # clean 1-minute bars (pandas DataFrame)
df = hfdl.get("AAPL", version="raw", timeframe="daily")
panel = hfdl.get(["AAPL", "MSFT", "SPY"])         # -> {ticker: DataFrame}
df = hfdl.get("AAPL", fmt="csv")                  # CSV instead of parquet (no pyarrow needed)
```

Parameters: `version` ∈ {`clean`, `raw`}; `timeframe` ∈ {`1min`,`5min`,`15min`,`30min`,`hourly`,`daily`,`weekly`,`monthly`}.

## Important: survivorship bias

The universe is a fixed snapshot (~2023) carried back to 2002, so pre-2022
history is **survivor-conditioned** — companies that delisted before ~2021 are
absent. Not suitable for survivorship-sensitive backtests over 2002–2021
without adjustment. See the [methodology docs](https://hfdatalibrary.com/pages/docs)
for full limitations (including the post-March-2022 IEX-only volume caveat).

## License

MIT (client code). Data is CC BY 4.0 — cite per <https://hfdatalibrary.com/pages/cite>.

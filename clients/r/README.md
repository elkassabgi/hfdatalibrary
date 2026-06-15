# hfdatalibrary (R)

R client for the [HF Data Library](https://hfdatalibrary.com) — free,
research-grade 1-minute OHLCV data for ~1,391 U.S. equities and ETFs, in
`raw` and `clean` versions across eight timeframes.

## Install

```r
# install.packages("remotes")
remotes::install_github("elkassabgi/hfdatalibrary", subdir = "clients/r/hfdatalibrary")
```

Requires `httr` and `arrow` (for parquet). `arrow` is optional if you only use `format = "csv"`.

## Authenticate

Get a free API key at <https://hfdatalibrary.com/pages/account>:

```r
library(hfdatalibrary)
hfdl_set_key("YOUR_API_KEY")        # or set the HFDL_API_KEY environment variable
```

## Use

```r
hfdl_symbols()                                       # -> c("A", "AA", "AAPL", ...)
aapl  <- hfdl_get("AAPL")                            # clean 1-minute bars (data.frame)
daily <- hfdl_get("AAPL", version = "raw", timeframe = "daily")
panel <- hfdl_get(c("AAPL", "MSFT", "SPY"))          # named list of data.frames
csv   <- hfdl_get("AAPL", format = "csv")            # CSV (no arrow needed)
```

`version` ∈ {`clean`, `raw`}; `timeframe` ∈ {`1min`,`5min`,`15min`,`30min`,`hourly`,`daily`,`weekly`,`monthly`}.

## Important: survivorship bias

The universe is a fixed snapshot (~2023) carried back to 2002, so pre-2022
history is **survivor-conditioned** — companies that delisted before ~2021 are
absent. Not suitable for survivorship-sensitive backtests over 2002–2021
without adjustment. See the [methodology docs](https://hfdatalibrary.com/pages/docs).

## License

MIT (client code). Data is CC BY 4.0 — cite per <https://hfdatalibrary.com/pages/cite>.

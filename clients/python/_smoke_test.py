"""End-to-end smoke test for the Python client.

Reads HFDL_API_KEY from D:/research/econfindatalibrary/.env (gitignored),
then exercises symbols() + the authenticated get() paths. Run:

    python clients/python/_smoke_test.py
"""
import os
import sys
from pathlib import Path

ENV = Path(r"D:/research/econfindatalibrary/.env")
if ENV.exists():
    for line in ENV.read_text().splitlines():
        line = line.strip()
        if line.startswith("HFDL_API_KEY") and "=" in line:
            os.environ["HFDL_API_KEY"] = line.split("=", 1)[1].strip().strip('"').strip("'")

sys.path.insert(0, str(Path(__file__).parent))
import hfdatalibrary as hfdl  # noqa: E402

print("client version:", hfdl.__version__)

# 1) public endpoint (no key)
syms = hfdl.symbols()
print(f"symbols(): {len(syms)} tickers; first 5 = {syms[:5]}")
assert 1000 < len(syms) < 2000, f"unexpected symbol count {len(syms)}"
assert not any("/" in s for s in syms), "timeframe-prefixed entries leaked into symbols()"

if not os.environ.get("HFDL_API_KEY"):
    print("\nNO HFDL_API_KEY found in .env — skipping authenticated get() tests.")
    print("Add HFDL_API_KEY=... to D:/research/econfindatalibrary/.env and re-run.")
    sys.exit(0)

# 2) single ticker, default clean 1min parquet
df = hfdl.get("AAPL")
print(f"\nget('AAPL'): {df.shape[0]:,} rows x {df.shape[1]} cols; columns = {list(df.columns)}")
assert df.shape[0] > 0, "empty AAPL frame"

# 3) raw + daily
d2 = hfdl.get("AAPL", version="raw", timeframe="daily")
print(f"get('AAPL', raw, daily): {d2.shape[0]:,} rows")

# 4) multi-ticker
panel = hfdl.get(["AAPL", "MSFT"], timeframe="daily")
print(f"get(['AAPL','MSFT'], daily): { {k: v.shape[0] for k, v in panel.items()} }")

# 5) csv format (will fail cleanly if CSV files aren't generated server-side)
try:
    c = hfdl.get("AAPL", timeframe="daily", fmt="csv")
    print(f"get('AAPL', daily, csv): {c.shape[0]:,} rows  [CSV path works]")
except hfdl.HFDLError as e:
    print(f"csv path NOT available yet (expected until Worker CSV is added): {str(e)[:90]}")

print("\nSMOKE TEST PASSED")

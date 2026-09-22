"""upload_parquet must serve string columns as large_string whatever pandas/pyarrow wrote them (ledger R938).

The desktop (pandas 2.3 / pyarrow 23) turns an object column into Arrow `string`; CI (pandas 3 / pyarrow 25)
produces `large_string`. Before the pin, the served schema of the same ticker depended on which machine
last wrote it. These tests capture the bytes upload_parquet would send and read the schema back.
"""
import io
import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import r2_client  # noqa: E402


def _captured_schema(monkeypatch, df):
    sent = {}

    def fake_upload(client, key, data, content_type=None, **kw):
        sent["key"], sent["data"] = key, data
        return len(data)

    monkeypatch.setattr(r2_client, "upload_from_buffer", fake_upload)
    r2_client.upload_parquet(None, df, "raw", "TEST", "1min")
    return pq.read_table(io.BytesIO(sent["data"]))


def _bars(string_dtype):
    return pd.DataFrame({
        "datetime": pd.to_datetime(["2026-09-08 09:30", "2026-09-08 09:31"]),
        "Close": [44.10, 44.12],
        "Volume": [100, 200],
        "source": pd.Series(["iex", "iex"], dtype=string_dtype),
    })


@pytest.mark.parametrize("string_dtype", ["object", "string"])
def test_string_columns_are_served_as_large_string(monkeypatch, string_dtype):
    table = _captured_schema(monkeypatch, _bars(string_dtype))
    assert pa.types.is_large_string(table.schema.field("source").type), table.schema


def test_pin_changes_nothing_else(monkeypatch):
    df = _bars("object")
    table = _captured_schema(monkeypatch, df)
    unpinned = pa.Table.from_pandas(df, preserve_index=False)
    for f in unpinned.schema:
        got = table.schema.field(f.name).type
        want = pa.large_string() if pa.types.is_string(f.type) or pa.types.is_large_string(f.type) else f.type
        assert got == want, (f.name, got, want)
    back = table.to_pandas()
    assert list(back["source"]) == ["iex", "iex"]
    assert list(back["Close"]) == [44.10, 44.12]
    assert table.schema.metadata[b"citation"].startswith(b"Elkassabgi")


def test_the_pin_is_what_makes_the_difference_here(monkeypatch):
    """Planted control: an `object`-typed column converts to Arrow `string` - on pandas 2, and on pandas 3 too
    when the column is explicitly object dtype, as this fixture's is (review AR-079 checked it) - so the served
    `large_string` above can only come from the pin. A pa.Table method cannot be monkeypatched; a run with the
    pin reverted, where all four tests here fail, is the other half of this proof. If some future pandas or
    pyarrow already yields large_string for an object column, the control cannot fail and is skipped rather
    than passing vacuously."""
    df = _bars("object")
    unpinned = pa.Table.from_pandas(df, preserve_index=False).schema.field("source").type
    if not pa.types.is_string(unpinned):
        pytest.skip("this pandas/pyarrow already produces large_string; the control needs a pandas 2 machine")
    served = _captured_schema(monkeypatch, df).schema.field("source").type
    assert pa.types.is_string(unpinned) and pa.types.is_large_string(served), (unpinned, served)

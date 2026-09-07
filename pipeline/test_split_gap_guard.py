"""The split detector compares consecutive sessions only (R732).

WHY. `_detect_and_apply_split` divides today's median print by the LAST SERVED session's median
close. When the last served session is not yesterday but a year ago - an IEX symbol reassigned to
another issuer after a gap - the ratio is a comparison between two companies. On 2026-08-12 it
rescaled Paramount's whole history (and the PiTrading half) by 1/6 because Banzai's first day under
PARA printed at $1.84 against Paramount's last close of $11.07 a year earlier; on the next run it
would have done the same to STI (7.69 vs 70.16, snapped 1/9) and USLV (16.85 vs 66.59, 1/4).

The negative control (R346): a genuine overnight 6:1 forward split on consecutive sessions must
still be applied, so a regression that disables the detector fails this file too.
"""
from __future__ import annotations
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import daily_update  # noqa: E402


def _bars(day: str, close: float, n: int = 60) -> pd.DataFrame:
    idx = pd.date_range(f"{day} 09:30", periods=n, freq="min")
    return pd.DataFrame({"datetime": idx, "Open": close, "High": close, "Low": close, "Close": close,
                         "Volume": 600, "source": "iex", "ticker": "PARA"})


def test_a_year_long_gap_never_applies_a_split():
    existing = pd.concat([_bars("2025-08-05", 11.10), _bars("2025-08-06", 11.07)], ignore_index=True)
    new = _bars("2026-08-07", 1.845)                       # Banzai's first day under PARA: 11.07 / 6
    stats = {}
    out, rescaled = daily_update._detect_and_apply_split(existing, new, "PARA", stats)
    assert rescaled is False
    assert out is existing
    assert "ca_applied" not in stats
    assert "gap" in stats["ca_alert"] and "366 days" in stats["ca_alert"]


def test_a_gap_just_over_the_limit_alerts_and_a_gap_within_it_measures():
    existing = _bars("2026-06-01", 60.0)
    stats = {}
    _, rescaled = daily_update._detect_and_apply_split(existing, _bars("2026-06-15", 10.0), "STI", stats)
    assert rescaled is False and "ca_alert" in stats and "14 days" in stats["ca_alert"]
    stats = {}
    _, rescaled = daily_update._detect_and_apply_split(existing, _bars("2026-06-08", 10.0), "STI", stats)
    # within the limit the detector measures: 60 -> 10 is a consistent 6:1 forward split, applied
    assert rescaled is True and "ca_applied" in stats


def test_negative_control_consecutive_sessions_still_apply_a_real_split():
    existing = pd.concat([_bars("2026-08-11", 66.0), _bars("2026-08-12", 66.6)], ignore_index=True)
    new = _bars("2026-08-13", 11.1)                        # 6:1 forward split overnight
    stats = {}
    out, rescaled = daily_update._detect_and_apply_split(existing, new, "XYZ", stats)
    assert rescaled is True
    assert abs(float(out["Close"].iloc[-1]) - 66.6 / 6) < 1e-6
    assert stats["ca_applied"].startswith("XYZ: 6:1 forward split")

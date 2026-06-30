"""The guardrails must be loud and never silent: survivorship is a HARD STOP for
cumulative pre-2022 backtests unless acknowledged; the IEX-volume caveat fires
only when a strategy actually uses volume."""
import warnings

import pandas as pd
import pytest

from hfdatalibrary.lab import (
    audit_panel, enforce_survivorship,
    CaveatWarning, SurvivorshipBiasError, CoverageReport,
)


def _panel(start, end, n=3):
    idx = pd.date_range(start, end, freq="B")
    return pd.DataFrame({f"T{i}": [100.0] * len(idx) for i in range(n)}, index=idx)


def test_pre2022_is_severe_and_hard_stops():
    rep = audit_panel(_panel("2008-01-01", "2020-12-31"))
    assert rep.survivorship == "severe"
    assert rep.pre_2022 and not rep.point_in_time_safe
    with pytest.raises(SurvivorshipBiasError):
        enforce_survivorship(rep, acknowledge=False, cumulative=True)


def test_acknowledge_allows_and_records():
    rep = audit_panel(_panel("2008-01-01", "2020-12-31"))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = enforce_survivorship(rep, acknowledge=True, cumulative=True)
    assert out.acknowledged
    assert any("SURV_ACK" in n for n in out.notes)


def test_non_cumulative_does_not_hard_stop():
    rep = audit_panel(_panel("2008-01-01", "2020-12-31"))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        enforce_survivorship(rep, acknowledge=False, cumulative=False)  # must not raise


def test_post2022_is_clean():
    rep = audit_panel(_panel("2022-06-01", "2024-01-01"))
    assert rep.survivorship == "none"
    assert rep.point_in_time_safe
    enforce_survivorship(rep)  # no raise


def test_iex_volume_warns_only_when_volume_used():
    with_vol = audit_panel(_panel("2022-06-01", "2023-06-01"), uses_volume=True)
    assert any("IEX VOLUME" in w for w in with_vol.warnings)

    no_vol = audit_panel(_panel("2022-06-01", "2023-06-01"), uses_volume=False)
    assert not any("IEX VOLUME" in w for w in no_vol.warnings)
    assert any("IEX SEGMENT" in n for n in no_vol.notes)


def test_source_column_overrides_date_inference():
    # window is entirely pre-break by date, but the source column says IEX
    rep = audit_panel(start="2021-01-01", end="2021-06-01", n_tickers=2,
                      sources={"iex"})
    assert rep.iex_segment is True


def test_missing_tickers_recorded():
    rep = audit_panel(_panel("2023-01-01", "2023-06-01"),
                      missing_tickers=["XYZ", "ABC"])
    assert "XYZ" in rep.missing_tickers
    assert any("skipped" in n for n in rep.notes)


def test_caveat_warning_can_escalate_to_error():
    rep = audit_panel(_panel("2022-06-01", "2023-06-01"), uses_volume=True)
    with warnings.catch_warnings():
        warnings.simplefilter("error", CaveatWarning)
        with pytest.raises(CaveatWarning):
            enforce_survivorship(rep, acknowledge=False, cumulative=False)

"""Tests for drift detection and history-store round-trip."""
from __future__ import annotations

from pathlib import Path

import pytest

from quality import baseline, drift, types


def test_compute_band_returns_none_below_three_samples():
    assert drift.compute_band([1.0]) is None
    assert drift.compute_band([1.0, 2.0]) is None


def test_compute_band_basic():
    band = drift.compute_band([10.0, 11.0, 9.0, 10.0, 12.0, 8.0, 10.0])
    assert band is not None
    assert band.median == 10.0
    # MAD of [10,11,9,10,12,8,10] = median([0,1,1,0,2,2,0]) = 1
    assert band.mad == pytest.approx(1.0)
    # Width = max(5*1, 0.05*10) = 5
    assert band.lower == pytest.approx(5.0)
    assert band.upper == pytest.approx(15.0)
    assert band.n_samples == 7


def test_compute_band_floors_constant_series():
    """Constant series → MAD=0; floor kicks in at 5% of median."""
    band = drift.compute_band([1000.0] * 7)
    assert band is not None
    assert band.mad == 0
    assert band.lower == pytest.approx(950.0)
    assert band.upper == pytest.approx(1050.0)


def test_history_round_trip(tmp_path: Path):
    """append_measurements + historical_values round-trips. Uses
    today-relative timestamps so all 7 stay inside the 7-day query
    window."""
    from datetime import datetime, timedelta, timezone
    db = tmp_path / "history.db"

    def make_run(run_at, value):
        return types.CheckRunReport(
            run_at=run_at, db_path="x.db", duration_seconds=0.1,
            results=[
                types.CheckResult(
                    name="dates.year_0000", severity=types.Severity.CRITICAL,
                    passed=True, metric_value=value, threshold=0,
                    message="ok",
                ),
            ],
        )

    today = datetime.now(timezone.utc).date()
    values_in = [10, 11, 9, 10, 12, 8, 10]
    for i, v in enumerate(values_in):
        ts = (today - timedelta(days=i)).isoformat() + "T00:00:00+00:00"
        baseline.append_measurements(make_run(ts, v), db=db)

    values = baseline.historical_values(
        "dates.year_0000", court=None, db=db,
    )
    assert sorted(values) == sorted([float(v) for v in values_in])


def test_detect_no_drift_when_no_history(tmp_path, monkeypatch):
    """detect() returns (False, None) when history is empty."""
    monkeypatch.setattr(baseline, "HISTORY_DB", tmp_path / "absent.db")
    is_drift, band = drift.detect("nonexistent.check", court=None,
                                  current_value=999.0)
    assert is_drift is False
    assert band is None


def test_detect_flags_outlier(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone

    db = tmp_path / "history.db"
    monkeypatch.setattr(baseline, "HISTORY_DB", db)
    today = datetime.now(timezone.utc).date()

    # Seed 7 stable runs at value ≈ 100 across the last 7 days
    for i, v in enumerate([100, 101, 99, 100, 102, 98, 100]):
        ts = (today - timedelta(days=i)).isoformat() + "T00:00:00+00:00"
        report = types.CheckRunReport(
            run_at=ts, db_path="x.db", duration_seconds=0.1,
            results=[types.CheckResult(
                name="schema.corpus_total_count",
                severity=types.Severity.CRITICAL,
                passed=True, metric_value=v, threshold=950_000,
                message="x",
            )],
        )
        baseline.append_measurements(report, db=db)

    # Today's value: 50 — well outside median ± 5×MAD ≈ 100 ± 5
    # (MAD floor: max(5*1, 0.05*100) = 5 → band 95..105)
    is_drift, band = drift.detect(
        "schema.corpus_total_count", court=None, current_value=50.0,
    )
    assert band is not None
    assert is_drift is True

    # Today's value: 100 — within band
    is_drift2, _ = drift.detect(
        "schema.corpus_total_count", court=None, current_value=100.0,
    )
    assert is_drift2 is False

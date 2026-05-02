"""Per-court drift detection — every court's metrics over the last
7 days, flagged when current value falls outside median ± 5×MAD.

Catches the SG-anomaly class: a sudden drop of 1,450 rows in one
nightly. After 7 stable nights the MAD is small, any change > 0
trips. (drift.py's `MIN_BAND_FRACTION` floor prevents 0 MAD from
flagging on legitimate single-digit changes.)
"""
from __future__ import annotations

import sqlite3

from quality.checks._common import iter_active_courts
from quality.drift import detect
from quality.types import CheckResult, Severity

MODULE_NEVER_CRITICAL = True  # WARNING-only; runner skips in --critical-only


def check_per_court_row_count_drift(conn: sqlite3.Connection, **_):
    """For every active court, record + check today's row count vs
    the 7-day rolling median ± 5×MAD."""
    for court in iter_active_courts(conn, min_rows=10):
        n = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=?", (court,)
        ).fetchone()[0]
        is_drift, band = detect(
            check_name="per_court_drift.row_count",
            court=court, current_value=float(n),
        )
        msg = f"{court}: {n:,} rows"
        if band:
            msg += f" (7d band {band.lower:.0f}..{band.upper:.0f}, n={band.n_samples})"
        yield CheckResult(
            name=f"per_court_drift.row_count.{court}",
            severity=Severity.WARNING,
            passed=(not is_drift),
            metric_value=float(n),
            threshold=band.upper if band else None,
            message=msg,
            court=court,
            extra={
                "median": band.median if band else None,
                "mad": band.mad if band else None,
            } if band else {},
            fix_advice="if drift down, scraper or import lost decisions; "
                       "if drift up, investigate dedup pass",
        )


def check_per_court_null_date_drift(conn: sqlite3.Connection, **_):
    """NULL-date count drift per court. Often paired with row-count
    drift but specifically catches date-extraction regressions."""
    for court in iter_active_courts(conn, min_rows=100):
        n = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=? AND "
            "(decision_date IS NULL OR decision_date='')", (court,)
        ).fetchone()[0]
        is_drift, band = detect(
            check_name="per_court_drift.null_date_count",
            court=court, current_value=float(n),
        )
        msg = f"{court}: {n} NULL dates"
        if band:
            msg += f" (7d band {band.lower:.0f}..{band.upper:.0f})"
        yield CheckResult(
            name=f"per_court_drift.null_date_count.{court}",
            severity=Severity.WARNING,
            passed=(not is_drift),
            metric_value=float(n),
            threshold=band.upper if band else None,
            message=msg,
            court=court,
        )

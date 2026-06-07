"""Date-quality checks (König P4 + P6 + plausibility).

Catches:
- NULL/empty dates outside known floor
- Year-0000 placeholder dates (build_fts5 _normalize_dates auto-fixes;
  this check ensures it actually ran)
- Future dates beyond the next 30 days (typo / parser bug)
- Pre-1700 dates (parser bug — earliest legitimate decision is BGE 1875)
- Far-future dates beyond next year (catastrophic typo)
"""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta

from quality.types import CheckResult, Severity


# Known "source-data floor" — these residual NULL counts are not bugs;
# they reflect court archives that genuinely lack machine-readable dates.
# Floors codified in docs/MIGRATIONS.md (2026-04-30).
KNOWN_NULL_DATE_FLOORS = {
    "mkg": 542,            # 1914-2010 archive; dates only in scanned images
    "ti_gerichte": 549,    # PDFs truncated at ~1.5K chars (no body)
    "hudoc_ch": 246,       # ECHR metadata-only docs
    "sav_kantone": 36,     # Aufsichtsbehörden — no PDF, only metadata
    "fr_gerichte": 80,     # post-recovery residual
    "gr_gerichte": 80,     # post-recovery residual
}


def check_year_0000_dates(conn: sqlite3.Connection, **_) -> CheckResult:
    """Build_fts5._normalize_dates() converts year-0000 placeholders to
    NULL. Any row with `decision_date LIKE '0000%'` means the auto-fix
    didn't run."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date LIKE '0000%'"
    ).fetchone()[0]
    return CheckResult(
        name="dates.year_0000",
        severity=Severity.QUARANTINE,  # auto-NULLed by _normalize_dates; warn-not-block
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} rows with year-0000 dates" if n else
                "no year-0000 placeholder dates",
        fix_advice="build_fts5._normalize_dates() auto-converts to NULL; "
                   "verify it ran on this build",
    )


def check_far_future_dates(conn: sqlite3.Connection, **_) -> CheckResult:
    """Dates beyond today+365 are catastrophic typos — never legitimate."""
    cutoff = (date.today() + timedelta(days=365)).isoformat()
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date > ?", (cutoff,)
    ).fetchone()[0]
    return CheckResult(
        name="dates.far_future",
        severity=Severity.QUARANTINE,  # auto-NULLed by _normalize_dates; warn-not-block
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} rows dated > today+365d" if n else
                "no far-future dates",
        fix_advice="build_fts5._normalize_dates() auto-NULLs these; "
                   "check scraper date-parsing for German/French month confusion",
    )


def check_future_dates_window(conn: sqlite3.Connection, **_) -> CheckResult:
    """Dates within today+30 ≤ d ≤ today+365 are sometimes legit (pending
    publication), but >50 means parser bug. König 2026-04-30 measured
    12 such rows in production (fr_gerichte+sz_gerichte month-name parse
    bug)."""
    cutoff = (date.today() + timedelta(days=30)).isoformat()
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date > ?", (cutoff,)
    ).fetchone()[0]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT decision_id, court, decision_date FROM decisions "
            "WHERE decision_date > ? ORDER BY decision_date DESC LIMIT 5",
            (cutoff,),
        ).fetchall()
    ] if n else []
    return CheckResult(
        name="dates.future_window",
        severity=Severity.QUARANTINE,  # count-bounded (>50 = parser regression); warn-not-block
        passed=(n <= 50),
        metric_value=n,
        threshold=50,
        message=f"{n} rows dated > today+30d (threshold 50)" if n > 50 else
                f"{n} future-dated rows (within tolerance)",
        sample_rows=sample,
        fix_advice="if >50, expect a scraper date-parsing regression",
    )


def check_pre_1700_dates(conn: sqlite3.Connection, **_) -> CheckResult:
    """No legitimate Swiss court decision predates 1700. The earliest
    indexed BGE is 1875. Anything older is a parser bug."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date IS NOT NULL "
        "AND decision_date != '' AND decision_date < '1700-01-01'"
    ).fetchone()[0]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT decision_id, court, decision_date FROM decisions "
            "WHERE decision_date IS NOT NULL AND decision_date != '' "
            "AND decision_date < '1700-01-01' LIMIT 5"
        ).fetchall()
    ] if n else []
    return CheckResult(
        name="dates.pre_1700",
        severity=Severity.QUARANTINE,  # auto-NULLed by _normalize_dates; warn-not-block (froze publish 2026-06-03..06)
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} rows dated before 1700-01-01" if n else
                "no pre-1700 dates",
        sample_rows=sample,
    )


def check_invalid_date_format(conn: sqlite3.Connection, **_) -> CheckResult:
    """Decision dates must be ISO 8601 (YYYY-MM-DD). Anything shorter is
    truncated; anything longer has trailing junk."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE decision_date IS NOT NULL "
        "AND decision_date != '' AND decision_date != 'None' "
        "AND length(decision_date) != 10"
    ).fetchone()[0]
    return CheckResult(
        name="dates.invalid_format",
        severity=Severity.CRITICAL,
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} rows with non-ISO-8601 decision_date" if n else
                "all dates are ISO 8601 format",
    )


def check_null_dates_floor(conn: sqlite3.Connection, **_):
    """For every court with a known NULL-date floor (mkg, ti_gerichte,
    hudoc_ch, …), assert the count hasn't grown materially. Floors are
    documented in docs/MIGRATIONS.md.

    Per-court WARNING: drift below the floor is fine (more recovery!),
    drift above by >10% means a regression."""
    for court, floor in KNOWN_NULL_DATE_FLOORS.items():
        n = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=? AND "
            "(decision_date IS NULL OR decision_date='')", (court,),
        ).fetchone()[0]
        upper = max(int(floor * 1.10), floor + 10)
        yield CheckResult(
            name=f"dates.null_date_floor.{court}",
            severity=Severity.WARNING,
            passed=(n <= upper),
            metric_value=n,
            threshold=upper,
            message=f"{court}: {n} NULL decision_dates (floor {floor})",
            court=court,
            fix_advice="if growing, check the court-specific recovery script in "
                       "build_fts5._recover_decision_dates / scripts/",
        )


def check_total_null_dates(conn: sqlite3.Connection, **_) -> CheckResult:
    """Aggregate: total rows with NULL decision_date should not exceed
    the sum of known floors + 200 absolute slack."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE "
        "(decision_date IS NULL OR decision_date='')"
    ).fetchone()[0]
    expected_max = sum(KNOWN_NULL_DATE_FLOORS.values()) + 200
    return CheckResult(
        name="dates.total_null",
        severity=Severity.WARNING,
        passed=(n <= expected_max),
        metric_value=n,
        threshold=expected_max,
        message=f"{n} total NULL decision_dates (expected ≤ {expected_max})",
        fix_advice="if growing, identify the new court contributing NULL dates "
                   "via per-court breakdown",
    )

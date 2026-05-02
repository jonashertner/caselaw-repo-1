"""Source-data floor checks.

Per docs/MIGRATIONS.md (2026-04-30 lines 230-276), the corpus has
known acceptable residuals that reflect SOURCE-data limits. These
checks ensure those floors stay STABLE — neither growing (regression)
nor shrinking unexpectedly (which could be a recovery success but
warrants review).

Floors:
- 1,542 NULL decision_dates total (mkg 542, ti 549, hudoc 246, ...)
- 64 short full_text rows (so_gerichte 59 + scattered)
"""
from __future__ import annotations

import sqlite3

MODULE_NEVER_CRITICAL = True  # all checks here return WARNING; runner skips in --critical-only

from quality.types import CheckResult, Severity


# Total floor — sum of per-court floors documented in MIGRATIONS.md
TOTAL_NULL_DATES_FLOOR = 1_542
TOTAL_SHORT_TEXT_FLOOR = 64


def check_total_null_dates_within_floor(conn: sqlite3.Connection, **_) -> CheckResult:
    """Total NULL decision_dates should be within ±5% of the documented
    floor. Crossing it upward is a regression; crossing downward is
    typically recovery progress (still flagged as a heads-up)."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE "
        "decision_date IS NULL OR decision_date = ''"
    ).fetchone()[0]
    upper = int(TOTAL_NULL_DATES_FLOOR * 1.10)
    return CheckResult(
        name="floors.total_null_dates",
        severity=Severity.WARNING,
        passed=(n <= upper),
        metric_value=n,
        threshold=upper,
        message=f"{n:,} NULL dates (documented floor {TOTAL_NULL_DATES_FLOOR:,})",
        fix_advice="if growing, identify the new contributing court via "
                   "`SELECT court, COUNT(*) GROUP BY court ORDER BY 2 DESC` "
                   "WHERE decision_date IS NULL",
    )


def check_total_short_text_within_floor(conn: sqlite3.Connection, **_) -> CheckResult:
    """Total short full_text rows (1-499 chars) — known floor 64 from
    so_gerichte truncated PDFs."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE "
        "full_text IS NOT NULL AND length(full_text) BETWEEN 1 AND 499"
    ).fetchone()[0]
    # Allow large slack since the BL/GR archive populations also count here
    upper = TOTAL_SHORT_TEXT_FLOOR + 16_000  # BL 6k + GR 9.5k buffer
    return CheckResult(
        name="floors.total_short_text",
        severity=Severity.WARNING,
        passed=(n <= upper),
        metric_value=n,
        threshold=upper,
        message=f"{n:,} short-text rows (floor {TOTAL_SHORT_TEXT_FLOOR}, "
                f"+ BL/GR archive buffer)",
    )

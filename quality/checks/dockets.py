"""Docket-number hygiene checks (König P5).

The build_fts5._normalize_dockets() pass strips leading/trailing
whitespace on every nightly. These checks verify the auto-correction
ran AND catches any new whitespace classes (newlines, NBSP, …) the
fix doesn't yet cover.
"""
from __future__ import annotations

import sqlite3

from quality.types import CheckResult, Severity


def check_whitespace_in_docket(conn: sqlite3.Connection, **_) -> CheckResult:
    """No docket_number may have leading/trailing whitespace.

    König 2026-04-30 found 20,864 rows; build_fts5._normalize_dockets()
    auto-corrects via TRIM(). After the fix, the count must stay 0."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE docket_number != trim(docket_number)"
    ).fetchone()[0]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT decision_id, court, docket_number FROM decisions "
            "WHERE docket_number != trim(docket_number) LIMIT 5"
        ).fetchall()
    ] if n else []
    return CheckResult(
        name="dockets.whitespace",
        severity=Severity.CRITICAL,
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} dockets with leading/trailing whitespace" if n else
                "all dockets are trimmed",
        sample_rows=sample,
        fix_advice="build_fts5._normalize_dockets() auto-trims; verify it ran",
    )


def check_internal_newlines_in_docket(conn: sqlite3.Connection, **_) -> CheckResult:
    """A real docket never contains a newline. Embedded \\n usually
    means the scraper grabbed two adjacent table cells."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE docket_number LIKE '%' || char(10) || '%' "
        "OR docket_number LIKE '%' || char(13) || '%'"
    ).fetchone()[0]
    return CheckResult(
        name="dockets.internal_newlines",
        severity=Severity.CRITICAL,
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} dockets contain newline chars" if n else
                "no embedded newlines",
        fix_advice="check scraper that wrote these rows; replace newline with space",
    )


def check_empty_docket_pct(conn: sqlite3.Connection, **_) -> CheckResult:
    """Empty docket_number is acceptable for some sources (mkg archive,
    aggregator-only feeds), but >5% would indicate a scraper regression."""
    n_total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    n_empty = conn.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE docket_number IS NULL OR docket_number = ''"
    ).fetchone()[0]
    pct = round(100 * n_empty / n_total, 3) if n_total else 0
    return CheckResult(
        name="dockets.empty_pct",
        severity=Severity.WARNING,
        passed=(pct <= 5.0),
        metric_value=pct,
        threshold=5.0,
        message=f"{n_empty:,} rows ({pct:.2f}%) without docket_number",
        extra={"empty": n_empty, "total": n_total},
    )


def check_excessively_long_docket(conn: sqlite3.Connection, **_) -> CheckResult:
    """A real Swiss docket fits in ~50 chars. >150 chars usually means
    the scraper grabbed a paragraph by mistake."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE length(docket_number) > 150"
    ).fetchone()[0]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT decision_id, court, substr(docket_number, 1, 80) || '...' "
            "AS docket_preview, length(docket_number) AS len "
            "FROM decisions WHERE length(docket_number) > 150 "
            "ORDER BY len DESC LIMIT 5"
        ).fetchall()
    ] if n else []
    return CheckResult(
        name="dockets.excessively_long",
        severity=Severity.WARNING,
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} rows with docket_number > 150 chars",
        sample_rows=sample,
    )

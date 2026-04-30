"""Language-attribution checks.

Every Swiss decision is in DE, FR, IT, or RM (Rumantsch). Anything
else is a scraper bug — the field was populated from the wrong
column or character-set decoded badly.
"""
from __future__ import annotations

import sqlite3

from quality.types import CheckResult, Severity


VALID_LANGUAGES = {"de", "fr", "it", "rm"}
NULL_LANGUAGE_FLOOR_PCT = 0.5  # WARNING if >0.5% of rows have no language


def check_unexpected_language_values(conn: sqlite3.Connection, **_) -> CheckResult:
    """Language must be in {de, fr, it, rm} or NULL."""
    rows = conn.execute(
        "SELECT language, COUNT(*) FROM decisions "
        "WHERE language IS NOT NULL AND language != '' "
        "GROUP BY language"
    ).fetchall()
    bad = [(lang, n) for lang, n in rows if lang not in VALID_LANGUAGES]
    return CheckResult(
        name="languages.unexpected_values",
        severity=Severity.CRITICAL,
        passed=(not bad),
        metric_value=sum(n for _, n in bad),
        threshold=0,
        message=f"{len(bad)} unexpected language codes" if bad else
                f"all language codes ∈ {{de, fr, it, rm}}",
        sample_rows=[{"language": repr(l), "count": n} for l, n in bad[:5]],
        extra={"distribution": dict(rows)},
        fix_advice="language must be a 2-letter ISO code. Investigate the "
                   "scraper that wrote this row's language column.",
    )


def check_null_language_pct(conn: sqlite3.Connection, **_) -> CheckResult:
    """NULL language is acceptable when not detectable from source, but
    >0.5% of corpus means a scraper regression."""
    n_total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    n_null = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE language IS NULL OR language=''"
    ).fetchone()[0]
    pct = round(100 * n_null / n_total, 3) if n_total else 0
    return CheckResult(
        name="languages.null_pct",
        severity=Severity.WARNING,
        passed=(pct <= NULL_LANGUAGE_FLOOR_PCT),
        metric_value=pct,
        threshold=NULL_LANGUAGE_FLOOR_PCT,
        message=f"{n_null:,} rows ({pct:.2f}%) without language",
        extra={"null": n_null, "total": n_total},
    )


def check_language_distribution_plausible(conn: sqlite3.Connection, **_) -> CheckResult:
    """Swiss case-law has roughly equal DE+FR contributions (~46% / ~45%
    each at 2026-04-30, with IT ~8%, RM <0.01%). A nightly where any of
    DE / FR drops below 30% means a multi-court import failure (e.g.
    every German cantonal scraper crashed)."""
    rows = dict(conn.execute(
        "SELECT language, COUNT(*) FROM decisions "
        "WHERE language IN ('de','fr','it','rm') GROUP BY language"
    ).fetchall())
    n_total = sum(rows.values())
    if not n_total:
        return CheckResult(
            name="languages.distribution_plausible",
            severity=Severity.CRITICAL,
            passed=False,
            metric_value=0,
            threshold=None,
            message="zero rows with a language assigned — corpus broken",
        )
    de_pct = 100 * rows.get("de", 0) / n_total
    fr_pct = 100 * rows.get("fr", 0) / n_total
    it_pct = 100 * rows.get("it", 0) / n_total
    # Both DE and FR must each contribute ≥30% (catastrophic-import-loss
    # threshold). IT must be at least 3% (TI gerichte alone has ~58k).
    ok = de_pct >= 30 and fr_pct >= 30 and it_pct >= 3
    return CheckResult(
        name="languages.distribution_plausible",
        severity=Severity.CRITICAL,
        passed=ok,
        metric_value=de_pct,
        threshold=30.0,
        message=f"DE {de_pct:.1f}% / FR {fr_pct:.1f}% / "
                f"IT {it_pct:.1f}% / RM "
                f"{100*rows.get('rm',0)/n_total:.2f}%",
        extra=rows,
        fix_advice="DE and FR each need ≥30%, IT ≥3%; under-floor means a "
                   "language-block of scrapers crashed in publish.py Step 1",
    )

"""Regeste-quality checks.

The Regeste is the official head-note: a 1-3 paragraph abstract
written by the court. Quality issues:
- Empty regeste on a federal court (BGer/BGE always have one)
- Regeste field accidentally populated with full_text (looks like
  an Erwägung, not a head-note — König P7 fix-class)
- Regeste shorter than 20 chars (header-only artefact)
- Regeste longer than 8000 chars (almost certainly full_text leakage)
"""
from __future__ import annotations

import sqlite3

from quality.types import CheckResult, Severity

MODULE_NEVER_CRITICAL = True  # WARNING-only; runner skips in --critical-only


# Per-court regeste coverage floors (measured 2026-04-30).
# Many old BGer cases don't have a regeste at all — the court only
# started writing them consistently for 4-digit-volume BGEs. Setting
# the floor below measured coverage so a regression triggers, but
# normal operation passes.
REGESTE_COVERAGE_FLOORS = {
    "bge":  50.0,    # measured 58.8%
    "bger": 40.0,    # measured 42.8%
}


def check_regeste_obligatory_present(conn: sqlite3.Connection, **_):
    """Per-court regeste coverage floor. A drop below the historical
    floor means build_fts5._fill_missing_regeste regressed."""
    for court, floor in REGESTE_COVERAGE_FLOORS.items():
        n_total = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=?", (court,),
        ).fetchone()[0]
        if n_total == 0:
            continue
        n_with_regeste = conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=? AND "
            "regeste IS NOT NULL AND length(regeste) >= 20", (court,),
        ).fetchone()[0]
        coverage = round(100 * n_with_regeste / n_total, 2)
        yield CheckResult(
            name=f"regeste.coverage.{court}",
            severity=Severity.WARNING,
            passed=(coverage >= floor),
            metric_value=coverage,
            threshold=floor,
            message=f"{court}: {n_with_regeste:,}/{n_total:,} "
                    f"({coverage:.1f}%) have a regeste (floor {floor:.0f}%)",
            court=court,
            extra={"with_regeste": n_with_regeste, "total": n_total},
        )


def check_regeste_excessive_length(conn: sqlite3.Connection, **_) -> CheckResult:
    """A real regeste is ≤ 5000 chars (the Bundesgericht's longest is
    about 4500). >8000 means full_text leaked into the regeste field."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE length(regeste) > 8000"
    ).fetchone()[0]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT decision_id, court, length(regeste) AS regeste_len "
            "FROM decisions WHERE length(regeste) > 8000 "
            "ORDER BY regeste_len DESC LIMIT 5"
        ).fetchall()
    ] if n else []
    return CheckResult(
        name="regeste.excessive_length",
        severity=Severity.WARNING,
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} regestes > 8000 chars (likely full_text leakage)" if n
                else "no oversized regestes",
        sample_rows=sample,
        fix_advice="regeste is the head-note, not the body; check the scraper "
                   "or build_fts5._extract_regeste_from_text() boundary",
    )


def check_regeste_too_short(conn: sqlite3.Connection, **_) -> CheckResult:
    """Regestes between 1 and 19 chars are header artefacts ("Regeste",
    "Sentence:", etc.) — historic baseline measured 52k. Drift detection
    catches new growth; static threshold is a soft cap."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE regeste IS NOT NULL AND length(regeste) BETWEEN 1 AND 19"
    ).fetchone()[0]
    return CheckResult(
        name="regeste.too_short",
        severity=Severity.WARNING,
        passed=(n <= 100_000),
        metric_value=n,
        threshold=100_000,
        message=f"{n:,} regestes in 1..19 chars (baseline ~52k)",
        fix_advice="if >100k, scraper is producing new header-only stubs; "
                   "check build_fts5._extract_regeste_from_text boundary",
    )

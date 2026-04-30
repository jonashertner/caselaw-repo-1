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


REGESTE_OBLIGATORY_COURTS = ["bge", "bge_historical", "bger"]


def check_regeste_obligatory_present(conn: sqlite3.Connection, **_):
    """For BGE / bger / bge_historical, every row should have a regeste
    (German formulation by the court). Coverage < 95% means an
    extraction regression in build_fts5._fill_missing_regeste()."""
    for court in REGESTE_OBLIGATORY_COURTS:
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
            passed=(coverage >= 95.0),
            metric_value=coverage,
            threshold=95.0,
            message=f"{court}: {n_with_regeste:,}/{n_total:,} "
                    f"({coverage:.1f}%) have a regeste",
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
    "Sentence:", etc.) and should be NULL or empty."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE regeste IS NOT NULL AND length(regeste) BETWEEN 1 AND 19"
    ).fetchone()[0]
    return CheckResult(
        name="regeste.too_short",
        severity=Severity.WARNING,
        passed=(n <= 50),
        metric_value=n,
        threshold=50,
        message=f"{n} regestes in 1..19 chars (header-only artefacts)",
        fix_advice="if growing, identify the scraper or extraction boundary "
                   "in build_fts5._extract_regeste_from_text",
    )

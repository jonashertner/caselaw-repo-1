"""Scholarship citation-bridge sanity checks.

The pub_citations_decisions + pub_citations_statutes tables in
legal_scholarship.db connect open-access Swiss legal scholarship to
the case + statute corpus. Populated by Step 2b of
build_legal_scholarship.py (scholarship_citation_extractor.extract_all).

Dry-run baseline (2026-05-28, 9,168 full-text pubs, regex + lookup):
  pub_citations_decisions: ~34,443 rows
  pub_citations_statutes:  ~90,177 rows
  pubs with ≥1 citation:    3,987 / 9,168 ≈ 43%

These checks catch silent regressions in the extractor (bad
abbreviation lookup, broken docket regex, schema drift).
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from quality.types import CheckResult, Severity


def _scholarship_db_path() -> Path:
    return Path(
        os.environ.get(
            "SWISS_CASELAW_SCHOLARSHIP_DB", "output/legal_scholarship.db"
        )
    )


def check_scholarship_db_present(conn: sqlite3.Connection, **_) -> CheckResult:
    """legal_scholarship.db must exist after Step 2h of publish.py."""
    p = _scholarship_db_path()
    return CheckResult(
        name="scholarship.db_present",
        severity=Severity.WARNING,
        passed=p.exists(),
        metric_value=1 if p.exists() else 0,
        threshold=1,
        message=f"legal_scholarship.db at {p}"
                + (" (present)" if p.exists() else " (MISSING)"),
        fix_advice="rebuild via Step 2h: "
                   "python -m search_stack.build_legal_scholarship",
    )


def check_scholarship_decision_citations_floor(
    conn: sqlite3.Connection, **_,
) -> CheckResult:
    """Citation extraction should yield ~34k decision-citations against the
    9k full-text scholarship corpus. <25k means the case-citation regex or
    decision-lookup map broke."""
    p = _scholarship_db_path()
    if not p.exists():
        return CheckResult(
            name="scholarship.decision_citations_floor",
            severity=Severity.WARNING,
            passed=False, metric_value=0, threshold=25_000,
            message="legal_scholarship.db missing",
        )
    sc = sqlite3.connect(f"file:{p}?mode=ro&immutable=1", uri=True)
    try:
        n = sc.execute(
            "SELECT COUNT(*) FROM pub_citations_decisions"
        ).fetchone()[0]
    except sqlite3.OperationalError:
        n = 0
    finally:
        sc.close()
    return CheckResult(
        name="scholarship.decision_citations_floor",
        severity=Severity.WARNING,
        passed=(n >= 25_000),
        metric_value=n,
        threshold=25_000,
        message=f"{n:,} pub→decision citations (expect ~34k)",
        fix_advice="check search_stack/scholarship_citation_extractor.py "
                   "and the case-citation regex in reference_extraction.py",
    )


def check_scholarship_statute_citations_floor(
    conn: sqlite3.Connection, **_,
) -> CheckResult:
    """Statute-citation extraction yields ~90k references. <65k means
    the statute regex or law-abbr lookup regressed."""
    p = _scholarship_db_path()
    if not p.exists():
        return CheckResult(
            name="scholarship.statute_citations_floor",
            severity=Severity.WARNING,
            passed=False, metric_value=0, threshold=65_000,
            message="legal_scholarship.db missing",
        )
    sc = sqlite3.connect(f"file:{p}?mode=ro&immutable=1", uri=True)
    try:
        n = sc.execute(
            "SELECT COUNT(*) FROM pub_citations_statutes"
        ).fetchone()[0]
    except sqlite3.OperationalError:
        n = 0
    finally:
        sc.close()
    return CheckResult(
        name="scholarship.statute_citations_floor",
        severity=Severity.WARNING,
        passed=(n >= 65_000),
        metric_value=n,
        threshold=65_000,
        message=f"{n:,} pub→statute citations (expect ~90k)",
        fix_advice="check statute regex + law-abbreviation map. "
                   "statutes.db schema may have changed.",
    )


def check_scholarship_bridge_coverage(
    conn: sqlite3.Connection, **_,
) -> CheckResult:
    """Of the ~9k full-text scholarship records, ≥35% should produce at
    least one resolved citation (decision or statute). Lower means the
    extractor is silently failing on a subset of sources."""
    p = _scholarship_db_path()
    if not p.exists():
        return CheckResult(
            name="scholarship.bridge_coverage",
            severity=Severity.WARNING,
            passed=False, metric_value=0, threshold=0.35,
            message="legal_scholarship.db missing",
        )
    sc = sqlite3.connect(f"file:{p}?mode=ro&immutable=1", uri=True)
    try:
        full_text = sc.execute(
            "SELECT COUNT(*) FROM publications WHERE has_full_text=1"
        ).fetchone()[0]
        if full_text == 0:
            ratio = 0.0
        else:
            n_with = sc.execute(
                "SELECT COUNT(DISTINCT pub_id) FROM ("
                "  SELECT pub_id FROM pub_citations_decisions"
                "  UNION"
                "  SELECT pub_id FROM pub_citations_statutes)"
            ).fetchone()[0]
            ratio = n_with / full_text
    except sqlite3.OperationalError:
        ratio = 0.0
    finally:
        sc.close()
    return CheckResult(
        name="scholarship.bridge_coverage",
        severity=Severity.INFO,
        passed=(ratio >= 0.35),
        metric_value=round(ratio, 3),
        threshold=0.35,
        message=f"{100*ratio:.1f}% of full-text pubs have ≥1 citation "
                f"(baseline 43%)",
        fix_advice="if dropping, inspect per-source breakdown in extractor logs",
    )

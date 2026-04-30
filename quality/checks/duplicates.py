"""Duplicate-detection checks.

Two failure classes:
1. Multiple rows for the same (court, docket) — the per-canton dedup
   missed a logical duplicate. König P1 EGMR class belongs here.
2. Same decision_id family appearing in multiple courts (e.g. an
   ATF case wrongly attributed to both `bge` and `bge_egmr`).

The build_fts5._dedup_decisions() pass enforces docket+court
uniqueness. These checks catch regressions in that pass.
"""
from __future__ import annotations

import sqlite3

from quality.types import CheckResult, Severity


def check_court_docket_collisions(conn: sqlite3.Connection, **_) -> CheckResult:
    """The (court, docket_number) pair is supposed to be ~unique. Some
    courts genuinely re-use dockets across years (e.g. simple sequence
    numbers) — those legitimate collisions are captured below as
    ≤ 5,000 acceptable. Anything more means a regression in
    build_fts5._dedup_decisions()."""
    n_groups = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM decisions
            WHERE docket_number IS NOT NULL AND docket_number != ''
            GROUP BY court, docket_number
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    sample = [
        dict(r) for r in conn.execute("""
            SELECT court, docket_number, COUNT(*) AS n FROM decisions
            WHERE docket_number IS NOT NULL AND docket_number != ''
            GROUP BY court, docket_number HAVING n > 1
            ORDER BY n DESC LIMIT 5
        """).fetchall()
    ] if n_groups else []
    return CheckResult(
        name="duplicates.court_docket_collisions",
        severity=Severity.WARNING,
        passed=(n_groups <= 5_000),
        metric_value=n_groups,
        threshold=5_000,
        message=f"{n_groups:,} (court, docket) pairs with >1 row",
        sample_rows=sample,
        fix_advice="if growing, check build_fts5._dedup_decisions() canonical_key",
    )


def check_egmr_no_dual_attribution(conn: sqlite3.Connection, **_) -> CheckResult:
    """König #1: BGE and EGMR cases were counted twice under court='bge'
    AND court='bge_egmr'. The fix in scrapers/entscheidsuche_ingest.py
    remaps. This check ensures it never regresses."""
    n_bge_cedh = conn.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE court='bge' AND source_url LIKE '%cedh%'"
    ).fetchone()[0]
    return CheckResult(
        name="duplicates.egmr_dual_attribution",
        severity=Severity.CRITICAL,
        passed=(n_bge_cedh == 0),
        metric_value=n_bge_cedh,
        threshold=0,
        message=f"{n_bge_cedh} bge rows with cedh.coe.int URL "
                f"(should all be in bge_egmr)" if n_bge_cedh else
                "EGMR attribution clean",
        fix_advice="codified by build_fts5._dedup_egmr_in_bge() + "
                   "scrapers/entscheidsuche_ingest.py override",
    )


def check_bge_egmr_count_in_range(conn: sqlite3.Connection, **_) -> CheckResult:
    """bge_egmr should have ~476 rows post-2026-04-29 fix; tolerance
    range 470-500 leaves room for 1-2 new judgements per quarter."""
    n = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE court='bge_egmr'"
    ).fetchone()[0]
    return CheckResult(
        name="duplicates.bge_egmr_count_range",
        severity=Severity.WARNING,
        passed=(470 <= n <= 700),
        metric_value=n,
        threshold=None,
        message=f"bge_egmr has {n} rows (expected 470-700)",
        fix_advice="if dropped: dedup over-fired; if grown beyond 700, "
                   "EGMR scraper got new feed",
    )


def check_decision_id_collisions_across_courts(conn: sqlite3.Connection, **_) -> CheckResult:
    """A logical decision shouldn't appear under two different `court`
    values. The dedup pass already enforces this for clean canonical_keys
    but stripped/normalized variants could slip through."""
    n = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT decision_id FROM decisions
            GROUP BY decision_id
            HAVING COUNT(DISTINCT court) > 1
        )
    """).fetchone()[0]
    return CheckResult(
        name="duplicates.decision_id_cross_court",
        severity=Severity.CRITICAL,
        passed=(n == 0),
        metric_value=n,
        threshold=0,
        message=f"{n} decision_ids attributed to >1 court" if n else
                "all decision_ids have a single court attribution",
    )

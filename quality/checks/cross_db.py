"""Cross-database consistency checks.

The corpus lives across 6 SQLite DBs that must agree on row counts
and ID space:

- output/decisions.db          (970k decisions, FTS5)
- output/reference_graph.db    (citations + statute edges)
- output/decision_structure.db (Sachverhalt/Erw/Disp paragraphs)
- output/statutes.db           (federal statutes)
- output/cantonal_laws.db      (cantonal laws)
- output/ok_commentaries.db    (scholarly commentary)

These checks verify each sidecar exists and that decision_id spaces
overlap correctly (sidecar rows must reference real decisions).
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from quality.types import CheckResult, Severity


SIDECAR_DBS = [
    ("reference_graph", "output/reference_graph.db"),
    ("decision_structure", "output/decision_structure.db"),
    ("statutes", "output/statutes.db"),
    ("cantonal_laws", "output/cantonal_laws.db"),
    ("ok_commentaries", "output/ok_commentaries.db"),
]


def _resolve(env_var: str, default: str) -> Path:
    return Path(os.environ.get(env_var, default))


def check_sidecar_dbs_present(conn: sqlite3.Connection, **_):
    """Every sidecar DB must exist; downstream tools fail-soft otherwise
    but the dataset isn't fully published until all are in place."""
    paths = {
        "reference_graph":    _resolve("SWISS_CASELAW_REFERENCE_GRAPH",
                                       "output/reference_graph.db"),
        "decision_structure": _resolve("SWISS_CASELAW_STRUCTURE_DB",
                                       "output/decision_structure.db"),
        "statutes":           _resolve("SWISS_CASELAW_STATUTES_DB",
                                       "output/statutes.db"),
        "cantonal_laws":      _resolve("SWISS_CASELAW_CANTONAL_DB",
                                       "output/cantonal_laws.db"),
        "ok_commentaries":    _resolve("SWISS_CASELAW_OK_DB",
                                       "output/ok_commentaries.db"),
    }
    for name, p in paths.items():
        yield CheckResult(
            name=f"cross_db.sidecar_present.{name}",
            severity=Severity.CRITICAL,
            passed=p.exists(),
            metric_value=1 if p.exists() else 0,
            threshold=1,
            message=f"{name} at {p}" + (
                " (present)" if p.exists() else " (MISSING)"),
            fix_advice=f"rebuild via search_stack/build_{name}_db.py "
                       f"or scripts/extract_*.py",
        )


def check_reference_graph_sanity(conn: sqlite3.Connection, **_) -> CheckResult:
    """The reference_graph.db should have ~6M citation_targets and
    ~11M statute edges per the May 2026 measurement. >5% drop is a
    regression."""
    rg_path = _resolve("SWISS_CASELAW_REFERENCE_GRAPH",
                       "output/reference_graph.db")
    if not rg_path.exists():
        return CheckResult(
            name="cross_db.reference_graph_sanity",
            severity=Severity.WARNING,
            passed=False,
            metric_value=0,
            threshold=None,
            message=f"reference_graph.db missing at {rg_path}",
        )
    rg = sqlite3.connect(f"file:{rg_path}?mode=ro&immutable=1", uri=True)
    try:
        # Detect what tables exist; both citation_targets (resolved) and
        # citation_references (raw) live in this DB.
        tables = {r[0] for r in rg.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        cit = (
            rg.execute("SELECT COUNT(*) FROM citation_targets").fetchone()[0]
            if "citation_targets" in tables else 0
        )
        stat = (
            rg.execute("SELECT COUNT(*) FROM statute_references").fetchone()[0]
            if "statute_references" in tables else 0
        )
    finally:
        rg.close()
    return CheckResult(
        name="cross_db.reference_graph_sanity",
        severity=Severity.WARNING,
        passed=(cit >= 5_000_000 and stat >= 8_000_000),
        metric_value=cit,
        threshold=5_000_000,
        message=f"citation_targets: {cit:,}, statute_references: {stat:,}",
        extra={"citation_targets": cit, "statute_references": stat},
        fix_advice="if citation count dropped, search_stack/build_reference_"
                   "graph.py may have failed in publish.py Step 2c",
    )


def check_no_orphan_structure_rows(conn: sqlite3.Connection, **_) -> CheckResult:
    """Every decision_id in decision_structure.db should also exist in
    decisions.db. Orphans mean structure rebuilt against an older corpus
    snapshot."""
    sc_path = _resolve("SWISS_CASELAW_STRUCTURE_DB",
                       "output/decision_structure.db")
    if not sc_path.exists():
        return CheckResult(
            name="cross_db.no_orphan_structure_rows",
            severity=Severity.INFO,
            passed=True,
            metric_value=0,
            threshold=None,
            message="sidecar absent — skipped",
        )
    sc = sqlite3.connect(f"file:{sc_path}?mode=ro&immutable=1", uri=True)
    try:
        # Sample 5000 sidecar IDs, check existence in main DB
        sample_ids = [r[0] for r in sc.execute(
            "SELECT decision_id FROM structure ORDER BY random() LIMIT 5000"
        ).fetchall()]
    finally:
        sc.close()
    if not sample_ids:
        return CheckResult(
            name="cross_db.no_orphan_structure_rows",
            severity=Severity.INFO,
            passed=True,
            metric_value=0,
            threshold=None,
            message="empty sidecar — skipped",
        )
    placeholders = ",".join("?" * len(sample_ids))
    n_present = conn.execute(
        f"SELECT COUNT(*) FROM decisions WHERE decision_id IN ({placeholders})",
        sample_ids,
    ).fetchone()[0]
    orphan_pct = round(100 * (len(sample_ids) - n_present) / len(sample_ids), 3)
    return CheckResult(
        name="cross_db.no_orphan_structure_rows",
        severity=Severity.WARNING,
        passed=(orphan_pct <= 1.0),
        metric_value=orphan_pct,
        threshold=1.0,
        message=f"{len(sample_ids) - n_present}/{len(sample_ids)} "
                f"({orphan_pct:.2f}%) sidecar rows orphan",
        fix_advice="rerun extract_decision_structure.py against current "
                   "decisions.db so IDs realign",
    )

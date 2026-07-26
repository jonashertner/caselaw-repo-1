"""Statute-graph integrity checks.

The reference_graph.db captures 12.4M decision→statute edges. Per-law
distribution sanity:
- Top federal acts should each keep hundreds of thousands of edges
- Each act is counted across its DE/FR/IT abbreviations, since
  `statutes.law_code` is language-specific
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from quality.checks._common import statute_edge_table
from quality.types import CheckResult, Severity

MODULE_NEVER_CRITICAL = True  # WARNING-only; runner skips in --critical-only


def _rg_path() -> Path:
    return Path(os.environ.get(
        "SWISS_CASELAW_REFERENCE_GRAPH", "output/reference_graph.db",
    ))


def _open_rg() -> sqlite3.Connection | None:
    p = _rg_path()
    if not p.exists():
        return None
    rg = sqlite3.connect(f"file:{p}?mode=ro&immutable=1", uri=True)
    rg.row_factory = sqlite3.Row
    rg.execute("PRAGMA busy_timeout=30000")
    return rg


# `statutes.law_code` is stored upper-case and language-specific: the same
# act appears under its German, French and Italian abbreviation, so counting
# only the German form undercounts badly (OR alone is 125,592 edges; OR+CO
# is 359,195). Floors are the 2026-07-26 measurement less ~15% headroom.
TOP_FEDERAL_LAWS = {
    "OR":   (("OR", "CO"),       300_000),   # measured   359,195
    "ZGB":  (("ZGB", "CC"),      400_000),   # measured   476,337
    "StGB": (("STGB", "CP"),     430_000),   # measured   515,633
    "BGG":  (("BGG", "LTF"),   1_500_000),   # measured 1,772,585
}


def check_top_federal_laws_present(conn: sqlite3.Connection, **_):
    """Each top federal act should keep a large body of statute edges
    pointing at it. A drop means the regex parser missed a citation
    form — often in just one language."""
    rg = _open_rg()
    if rg is None:
        return
    try:
        edge_table = statute_edge_table(rg)
        # law_code lives on `statutes`; the edge table carries only
        # statute_id (search_stack/build_reference_graph.py SCHEMA_SQL).
        cols = {r[1] for r in rg.execute(
            "PRAGMA table_info(statutes)"
        ).fetchall()}
        law_col = "law_code" if "law_code" in cols else (
            "abbreviation" if "abbreviation" in cols else None)
        if edge_table is None or law_col is None:
            # Report the mismatch instead of returning empty-handed —
            # silently yielding nothing is how this module went missing
            # from every QC report for months.
            yield CheckResult(
                name="statute_graph.schema_recognised",
                severity=Severity.WARNING,
                passed=False,
                metric_value=0,
                threshold=1,
                message=(f"reference_graph.db: edge table "
                         f"{edge_table or 'MISSING'}, statutes law column "
                         f"{law_col or 'MISSING'} — per-law checks skipped"),
                fix_advice="schema changed in search_stack/build_reference_"
                           "graph.py; update STATUTE_EDGE_TABLES in "
                           "quality/checks/_common.py",
            )
            return
        for label, (codes, floor) in TOP_FEDERAL_LAWS.items():
            placeholders = ",".join("?" * len(codes))
            n = rg.execute(
                f"SELECT COUNT(*) FROM {edge_table} WHERE statute_id IN "
                f"(SELECT statute_id FROM statutes "
                f"WHERE UPPER({law_col}) IN ({placeholders}))",
                tuple(c.upper() for c in codes),
            ).fetchone()[0]
            yield CheckResult(
                name=f"statute_graph.top_law.{label}",
                severity=Severity.WARNING,
                passed=(n >= floor),
                metric_value=n,
                threshold=floor,
                message=f"{label}: {n:,} edges across {'/'.join(codes)} "
                        f"(floor {floor:,})",
            )
    finally:
        rg.close()

"""Statute-graph integrity checks.

The reference_graph.db captures ~11M statute edges. Per-law
distribution sanity:
- Top federal laws (OR, ZGB, StGB, BGG) should each have >50k edges
- Resolution to known SR numbers should be near-100%
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

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


# Top-cited federal laws — counts taken at the Apr 2026 baseline.
TOP_FEDERAL_LAW_FLOORS = {
    "OR":   400_000,    # Code of Obligations
    "ZGB":  300_000,    # Civil Code
    "StGB": 200_000,    # Penal Code
    "BGG":  100_000,    # Federal Tribunal Act
}


def check_top_federal_laws_present(conn: sqlite3.Connection, **_):
    """Each of the top federal laws should have many statute references
    pointing at it. A drop means the regex parser missed a citation
    form."""
    rg = _open_rg()
    if rg is None:
        return
    try:
        # Schema may vary; introspect column names
        cols = {r[1] for r in rg.execute(
            "PRAGMA table_info(statute_references)"
        ).fetchall()}
        law_col = "law_code" if "law_code" in cols else (
            "abbreviation" if "abbreviation" in cols else None)
        if law_col is None:
            return
        for code, floor in TOP_FEDERAL_LAW_FLOORS.items():
            n = rg.execute(
                f"SELECT COUNT(*) FROM statute_references WHERE {law_col}=?",
                (code,),
            ).fetchone()[0]
            yield CheckResult(
                name=f"statute_graph.top_law.{code}",
                severity=Severity.WARNING,
                passed=(n >= floor),
                metric_value=n,
                threshold=floor,
                message=f"{code}: {n:,} references (floor {floor:,})",
            )
    finally:
        rg.close()

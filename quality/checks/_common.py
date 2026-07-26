"""Shared helpers for check modules."""
from __future__ import annotations

import sqlite3
from typing import Iterator


def iter_active_courts(
    conn: sqlite3.Connection, min_rows: int = 10,
) -> Iterator[str]:
    """Yield every court id with ≥ min_rows decisions, alphabetical."""
    rows = conn.execute(
        "SELECT court FROM decisions WHERE court IS NOT NULL AND court != '' "
        "GROUP BY court HAVING COUNT(*) >= ? ORDER BY court",
        (min_rows,),
    ).fetchall()
    for r in rows:
        yield r[0]


# search_stack/build_reference_graph.py names the decision→statute edge
# table `decision_statutes`. Check code used to count `statute_references`
# — a name that only ever existed as the extractor function
# extract_statute_references() — so every statute-edge check silently
# reported 0. Keep the legacy name as a fallback in case an older graph
# is pointed at, but look for the real one first.
STATUTE_EDGE_TABLES = ("decision_statutes", "statute_references")


def statute_edge_table(rg: sqlite3.Connection) -> str | None:
    """Name of the decision→statute edge table, or None if absent."""
    tables = {r[0] for r in rg.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    for name in STATUTE_EDGE_TABLES:
        if name in tables:
            return name
    return None


def count_statute_edges(rg: sqlite3.Connection) -> int:
    """Total decision→statute edges; 0 when the table is missing."""
    table = statute_edge_table(rg)
    if table is None:
        return 0
    return rg.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

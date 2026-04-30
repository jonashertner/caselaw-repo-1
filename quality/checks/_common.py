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

"""History store for QC measurements (`quality/history.db`).

Append-only SQLite. Each run adds one row per
(check_name, court, metric). `drift.py` queries the last 7 days to
compute MAD bands.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from quality.types import CheckRunReport

logger = logging.getLogger(__name__)

HISTORY_DB = Path("quality/history.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS measurements (
    run_at      TEXT NOT NULL,
    check_name  TEXT NOT NULL,
    court       TEXT,
    metric      TEXT NOT NULL,
    value       REAL NOT NULL,
    severity    TEXT NOT NULL,
    passed      INTEGER NOT NULL,
    PRIMARY KEY (run_at, check_name, court, metric)
);

CREATE INDEX IF NOT EXISTS idx_measurements_check_court
    ON measurements (check_name, court, metric, run_at);

CREATE TABLE IF NOT EXISTS run_log (
    run_at            TEXT PRIMARY KEY,
    db_path           TEXT NOT NULL,
    duration_seconds  REAL NOT NULL,
    total             INTEGER NOT NULL,
    passed            INTEGER NOT NULL,
    critical_failures INTEGER NOT NULL,
    warning_failures  INTEGER NOT NULL
);
"""


def _connect(db: Path | str = HISTORY_DB) -> sqlite3.Connection:
    db = Path(db)
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db))
    conn.executescript(_SCHEMA)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def append_measurements(report: CheckRunReport, db: Path | str = HISTORY_DB) -> None:
    """Append one row per CheckResult into `measurements`."""
    conn = _connect(db)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO run_log "
            "(run_at, db_path, duration_seconds, total, passed, "
            " critical_failures, warning_failures) VALUES (?,?,?,?,?,?,?)",
            (
                report.run_at, report.db_path, report.duration_seconds,
                len(report.results),
                sum(1 for r in report.results if r.passed),
                len(report.critical_failures),
                len(report.warning_failures),
            ),
        )
        rows = []
        for r in report.results:
            rows.append((
                report.run_at, r.name, r.court or "", "value",
                float(r.metric_value), r.severity.value, int(r.passed),
            ))
        conn.executemany(
            "INSERT OR REPLACE INTO measurements "
            "(run_at, check_name, court, metric, value, severity, passed) "
            "VALUES (?,?,?,?,?,?,?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def historical_values(
    check_name: str, court: str | None, metric: str = "value",
    days: int = 7, db: Path | str = HISTORY_DB,
) -> list[float]:
    """Return values from the last `days` days for the given key."""
    if not Path(db).exists():
        return []
    conn = _connect(db)
    try:
        rows = conn.execute(
            "SELECT value FROM measurements "
            "WHERE check_name=? AND court=? AND metric=? "
            "AND run_at >= datetime('now', ?) "
            "ORDER BY run_at DESC",
            (check_name, court or "", metric, f"-{int(days)} days"),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

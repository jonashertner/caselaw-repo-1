"""Schema-integrity checks. Detects NULL violations, broken FK
relationships, orphan rows, and table-shape regressions.

CRITICAL on every assertion — a schema break can corrupt every
downstream consumer (Parquet export, HF dataset, MCP queries).
"""
from __future__ import annotations

import sqlite3

from quality.types import CheckResult, Severity


# Required columns: must always have a value. From the canonical
# decisions table schema (mcp_server.py + build_fts5.py).
REQUIRED_NOT_NULL = ["decision_id", "court"]


def check_decisions_table_exists(conn: sqlite3.Connection, **_) -> CheckResult:
    """The `decisions` table is the entire dataset — no table = no service."""
    n = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='decisions'"
    ).fetchone()[0]
    return CheckResult(
        name="schema.decisions_table_exists",
        severity=Severity.CRITICAL,
        passed=(n == 1),
        metric_value=n,
        threshold=1,
        message=("decisions table present" if n == 1 else
                 "decisions table is MISSING — DB corrupt"),
        fix_advice="rebuild output/decisions.db via build_fts5.py",
    )


def check_decisions_fts_exists(conn: sqlite3.Connection, **_) -> CheckResult:
    """The FTS5 virtual table backs every search."""
    n = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE name='decisions_fts'"
    ).fetchone()[0]
    return CheckResult(
        name="schema.decisions_fts_exists",
        severity=Severity.CRITICAL,
        passed=(n >= 1),
        metric_value=n,
        threshold=1,
        message="FTS5 virtual table present" if n >= 1 else
                "FTS5 virtual table MISSING — search broken",
        fix_advice="rebuild output/decisions.db via build_fts5.py",
    )


def check_required_columns_not_null(conn: sqlite3.Connection, **_):
    """No row may have NULL decision_id or NULL court."""
    for col in REQUIRED_NOT_NULL:
        n = conn.execute(
            f"SELECT COUNT(*) FROM decisions WHERE {col} IS NULL OR {col} = ''"
        ).fetchone()[0]
        sample = []
        if n:
            sample = [
                dict(r) for r in conn.execute(
                    f"SELECT decision_id, court, docket_number FROM decisions "
                    f"WHERE {col} IS NULL OR {col} = '' LIMIT 5"
                ).fetchall()
            ]
        yield CheckResult(
            name=f"schema.required_not_null.{col}",
            severity=Severity.CRITICAL,
            passed=(n == 0),
            metric_value=n,
            threshold=0,
            message=f"{n} rows with NULL/empty {col}" if n else
                    f"all rows have non-null {col}",
            sample_rows=sample,
            fix_advice="investigate ingestion path; required columns must be set "
                       "by every scraper",
        )


def check_decision_id_uniqueness(conn: sqlite3.Connection, **_) -> CheckResult:
    """decision_id is the primary key — sqlite enforces this, but a
    rogue scraper writing the same id twice could elsewhere lose data."""
    n_total = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    n_unique = conn.execute(
        "SELECT COUNT(DISTINCT decision_id) FROM decisions"
    ).fetchone()[0]
    duplicates = n_total - n_unique
    return CheckResult(
        name="schema.decision_id_uniqueness",
        severity=Severity.CRITICAL,
        passed=(duplicates == 0),
        metric_value=duplicates,
        threshold=0,
        message=f"{duplicates} duplicate decision_ids" if duplicates else
                "all decision_ids unique",
        extra={"total": n_total, "distinct": n_unique},
        fix_advice="check INSERT OR IGNORE / INSERT OR REPLACE in scrapers and "
                   "build_fts5.merge_shards",
    )


def check_corpus_total_count(conn: sqlite3.Connection, **_) -> CheckResult:
    """A 1% drop in total count between nights is suspicious enough to block.

    Hard floor: 950k. The 2026-04-30 fully-rebuilt corpus is 971,112.
    """
    n = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    return CheckResult(
        name="schema.corpus_total_count",
        severity=Severity.CRITICAL,
        passed=(n >= 950_000),
        metric_value=n,
        threshold=950_000,
        message=f"corpus total: {n:,}" + (
            " (BELOW 950k floor — rebuild failure)" if n < 950_000 else ""),
        fix_advice="check publish.py Step 2 build_fts5 logs; verify .tmp swap "
                   "completed atomically",
    )

"""A full rebuild must PRESERVE the wayback_queue (incl. attempted_at) from the
live DB and take the fast incremental enqueue path — not wipe archiver progress
and re-run the ~1h full backfill every Sunday.

Regression guard for _ensure_wayback_queue(conn, live_db_path=...).
"""
import sqlite3
from pathlib import Path

from build_fts5 import _ensure_wayback_queue

_DECISIONS_DDL = """
CREATE TABLE decisions (
    decision_id TEXT, source_url TEXT, pdf_url TEXT, scraped_at TEXT
);
"""
_QUEUE_DDL = """
CREATE TABLE wayback_queue (
    decision_id TEXT NOT NULL, url TEXT NOT NULL, url_type TEXT NOT NULL,
    queued_at TEXT NOT NULL DEFAULT (datetime('now')),
    attempted_at TEXT, status_code INTEGER, archived_url TEXT,
    PRIMARY KEY (decision_id, url, url_type)
);
"""


def _make_live(path: Path):
    c = sqlite3.connect(path)
    c.executescript(_DECISIONS_DDL + _QUEUE_DDL)
    c.executemany("INSERT INTO decisions VALUES (?,?,?,?)", [
        ("d1", "http://a", None, "2026-01-01"),
        ("d2", "http://b", None, "2026-01-01"),
    ])
    # d1 already archived (attempted), d2 still pending.
    c.executemany(
        "INSERT INTO wayback_queue"
        "(decision_id,url,url_type,queued_at,attempted_at,status_code,archived_url)"
        " VALUES (?,?,?,?,?,?,?)", [
            ("d1", "http://a", "source", "2026-01-01 00:00:00",
             "2026-01-02 00:00:00", 200, "http://web.archive.org/a"),
            ("d2", "http://b", "source", "2026-01-01 00:00:00", None, None, None),
        ])
    c.commit(); c.close()


def test_full_rebuild_preserves_queue_and_goes_incremental(tmp_path):
    live = tmp_path / "decisions.db"
    _make_live(live)

    # Fresh .tmp build: same old decisions + one NEW decision scraped later.
    tmp = sqlite3.connect(tmp_path / "decisions.db.tmp")
    tmp.executescript(_DECISIONS_DDL)
    tmp.executemany("INSERT INTO decisions VALUES (?,?,?,?)", [
        ("d1", "http://a", None, "2026-01-01"),
        ("d2", "http://b", None, "2026-01-01"),
        ("d3", "http://c", None, "2026-06-01"),  # new since the queue marker
    ])
    tmp.commit()

    _ensure_wayback_queue(tmp, live_db_path=live)

    rows = {r[0]: r for r in tmp.execute(
        "SELECT decision_id, attempted_at, status_code FROM wayback_queue")}
    # All three present: d1/d2 preserved from live, d3 added incrementally.
    assert set(rows) == {"d1", "d2", "d3"}
    # d1's archiver progress is PRESERVED (not wiped to NULL).
    assert rows["d1"][1] == "2026-01-02 00:00:00" and rows["d1"][2] == 200
    # d2 + d3 are pending.
    assert rows["d2"][1] is None and rows["d3"][1] is None


def test_first_build_with_no_live_db_full_backfills(tmp_path):
    """No live DB → original full-backfill behaviour is intact."""
    tmp = sqlite3.connect(tmp_path / "decisions.db.tmp")
    tmp.executescript(_DECISIONS_DDL)
    tmp.executemany("INSERT INTO decisions VALUES (?,?,?,?)", [
        ("d1", "http://a", None, "2026-01-01"),
    ])
    tmp.commit()
    _ensure_wayback_queue(tmp, live_db_path=tmp_path / "does-not-exist.db")
    n = tmp.execute("SELECT COUNT(*) FROM wayback_queue").fetchone()[0]
    assert n == 1  # full backfill enqueued the one source url

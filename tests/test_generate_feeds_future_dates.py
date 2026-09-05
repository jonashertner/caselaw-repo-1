"""RSS feeds must not leak future-dated rows.

A known data-quality issue lets decisions land with a decision_date up to
365 days in the future (build_fts5._normalize_dates tolerates this on
purpose, to preserve legitimate pending-publication dates). Feeds sort by
decision_date DESC, so those rows floated to the top of every feed —
readers saw decisions "published" after the feed's own build date.

Fix: feed_sql() adds "AND decision_date <= date('now')" to the WHERE
clause of BOTH query shapes — the idx_decisions_date-pinned walk that
query_decisions() tries first and the unpinned fallback it drops to when
the window is thin or the index is missing (2026-09-05 rewrite). Since
date('now') is UTC and returns 'YYYY-MM-DD', this is a lexical string
comparison — it is only correct because decision_date is stored in the same
ISO YYYY-MM-DD shape (no time component). This test pins that format
assumption explicitly rather than assuming it.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import generate_feeds as gf  # noqa: E402

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@pytest.fixture
def fixture_db(tmp_path):
    db_path = tmp_path / "decisions.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE decisions (
            decision_id TEXT PRIMARY KEY,
            court TEXT, docket_number TEXT, decision_date TEXT,
            language TEXT, regeste TEXT, full_text TEXT, legal_area TEXT
        );
        -- same name as db_schema.SCHEMA_SQL so the pinned walk is available
        CREATE INDEX idx_decisions_date ON decisions(decision_date);
    """)

    today = datetime.now(timezone.utc).date()
    past_date = (today - timedelta(days=1)).isoformat()
    today_date = today.isoformat()
    future_date = (today + timedelta(days=30)).isoformat()  # within the 365d tolerance

    rows = [
        ("past_1", "bger", "1B_1/2026", past_date, "de", "R1", "T1", "civil"),
        ("today_1", "bger", "1B_2/2026", today_date, "de", "R2", "T2", "civil"),
        ("future_1", "bger", "1B_3/2026", future_date, "de", "R3", "T3", "civil"),
    ]
    conn.executemany(
        "INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?)", rows
    )
    conn.commit()
    conn.close()
    return db_path


def test_decision_date_column_is_iso_yyyy_mm_dd(fixture_db):
    """Pin the format assumption the lexical date('now') comparison relies
    on — don't just assume it, assert it against real fixture rows."""
    conn = sqlite3.connect(str(fixture_db))
    dates = [r[0] for r in conn.execute("SELECT decision_date FROM decisions")]
    conn.close()
    assert dates, "fixture produced no rows"
    for d in dates:
        assert _ISO_DATE_RE.match(d), f"decision_date not ISO YYYY-MM-DD: {d!r}"


def test_future_dated_row_excluded_from_feed(fixture_db):
    """Fallback shape: three rows < ITEMS_PER_FEED, so the pinned walk comes
    up thin and query_decisions drops to the unpinned query."""
    conn = sqlite3.connect(f"file:{fixture_db}?immutable=1", uri=True)
    stats: dict = {}
    items = gf.query_decisions(conn, "1=1", (), stats=stats)
    conn.close()
    assert stats["path"] == "fallback"
    ids = {row["decision_id"] for row in items}
    assert "future_1" not in ids
    assert "past_1" in ids
    assert "today_1" in ids


def test_pinned_date_walk_also_excludes_future_dated_row(fixture_db):
    """Pinned shape: the newest-first walk of idx_decisions_date would meet
    future_1 FIRST (it is the largest decision_date in the index), so the
    ceiling has to live in feed_sql() itself, not only in the fallback.
    limit=2 makes the window thick enough that the pinned result is what
    query_decisions returns; the raw pinned SQL is checked as well so the
    assertion does not depend on the fallback logic."""
    conn = sqlite3.connect(f"file:{fixture_db}?immutable=1", uri=True)
    assert gf.has_index(conn) is True

    stats: dict = {}
    items = gf.query_decisions(conn, "1=1", (), limit=2, stats=stats)
    assert stats["path"] == "date-walk"
    assert [row["decision_id"] for row in items] == ["today_1", "past_1"]

    floor = gf.recent_floor()
    raw = gf._rows(conn.execute(
        gf.feed_sql("1=1", pinned=True, floored=True), (floor, 50)))
    conn.close()
    ids = [row["decision_id"] for row in raw]
    assert "future_1" not in ids
    assert ids == ["today_1", "past_1"]
    today = datetime.now(timezone.utc).date().isoformat()
    assert all(row["decision_date"] <= today for row in raw)


def test_no_item_dated_after_today(fixture_db):
    conn = sqlite3.connect(f"file:{fixture_db}?immutable=1", uri=True)
    items = gf.query_decisions(conn, "1=1", ())
    conn.close()
    today = datetime.now(timezone.utc).date().isoformat()
    for row in items:
        assert row["decision_date"] <= today

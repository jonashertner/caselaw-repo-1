"""generate_feeds.py — the RSS query must be index-bounded and output-stable.

2026-09-03: Step 5b timed out at its 300 s cap. The six filtered feeds
(court = ? / language = ?) used the predicate's single-column index and then
sorted EVERY row of that court/language to find the newest 50 — one random
table-page read per row on the 70 GB decisions table, ~1.4 M reads in total,
200-240 s on a quiet day. The fix pins idx_decisions_date and walks it
newest-first inside a bounded window, falling back to the original query when
the window is thin or the index is missing. These tests pin:

  * the pinned query's plan walks idx_decisions_date and never needs a full
    ORDER BY sort (only the per-date tie-break may use a temp b-tree);
  * for every shipped feed the rows are IDENTICAL to the original query;
  * the fallback fires (and matches) for a predicate with <LIMIT recent rows,
    and when the date index does not exist;
  * NULL / '' and future decision_dates stay excluded on both shapes;
  * main() writes all seven feeds atomically (no *.tmp left behind).

Offline: a tiny decisions.db built from db_schema.SCHEMA_SQL in tmp_path.
"""
from __future__ import annotations

import sqlite3
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from itertools import pairwise
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import db_schema
import generate_feeds as gf

TODAY = date(2026, 9, 4)
FLOOR = gf.recent_floor(TODAY)

FEEDS = [
    ("1=1", ()),
    ("court = ?", ("bger",)),
    ("court = ?", ("bvger",)),
    ("court = ?", ("bge",)),
    ("language = ?", ("de",)),
    ("language = ?", ("fr",)),
    ("language = ?", ("it",)),
]


def _iso(days_ago: int) -> str:
    return (TODAY - timedelta(days=days_ago)).isoformat()


def _reference(conn, where, params, limit=gf.ITEMS_PER_FEED):
    """The pre-2026-09-04 query, verbatim: predicate index + full sort."""
    return gf._rows(conn.execute(gf.feed_sql(where), params + (limit,)))


@pytest.fixture()
def db(tmp_path: Path) -> Path:
    path = tmp_path / "decisions.db"
    conn = sqlite3.connect(path)
    conn.executescript(db_schema.SCHEMA_SQL)
    rows = []

    def add(court, lang, when, *, regeste="", full_text="", legal_area=""):
        rows.append((
            f"{court}_{len(rows)}", court, "CH", f"{len(rows) % 9}X {len(rows)}/2026",
            when, lang, regeste, full_text, legal_area,
        ))

    # bger: 80 rows in the last 300 days (de/fr/it mix, ties on the same
    # date so the decision_id tie-break matters) + 30 rows years ago.
    for i in range(80):
        add("bger", ("de", "fr", "it")[i % 3], _iso(1 + (i * 3) % 300),
            regeste="Regeste" if i % 2 else "", full_text="x" * 400)
    for i in range(30):
        add("bger", "de", _iso(800 + i * 7))
    # bvger: 60 recent rows, French.
    for i in range(60):
        add("bvger", "fr", _iso(2 + i * 4), legal_area="Asylrecht")
    # bge: only 10 rows inside the window, 70 older -> fallback path.
    for i in range(10):
        add("bge", "de", _iso(40 + i * 20))
    for i in range(70):
        add("bge", "de", _iso(400 + i * 11))
    # Cantonal courts to make de/fr/it feeds cross courts.
    for i in range(60):
        add("ge_gerichte", "fr", _iso(1 + i * 5))
    for i in range(55):
        add("ti_gerichte", "it", _iso(3 + i * 5))
    for i in range(20):
        add("zh_gerichte", "de", _iso(7 + i * 9))
    # Excluded rows: missing / empty date, and one future-dated row. The
    # future row is dated off the REAL clock (not TODAY) so it stays in the
    # future however long this fixture lives; both query shapes cap
    # decision_date at date('now'), so parity holds and it never appears.
    add("bger", "de", None)
    add("bger", "de", "")
    add("bvger", "de", (datetime.now(timezone.utc).date() + timedelta(days=30)).isoformat())

    conn.executemany(
        "INSERT INTO decisions(decision_id, court, canton, docket_number, "
        "decision_date, language, regeste, full_text, legal_area) "
        "VALUES (?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()
    return path


def _connect(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?immutable=1", uri=True)


# ── plan shape ────────────────────────────────────────────────────────────────

def test_pinned_plan_walks_the_date_index_without_a_full_sort(db):
    conn = _connect(db)
    for where, params in FEEDS:
        plan = [r[3] for r in conn.execute(
            "EXPLAIN QUERY PLAN " + gf.feed_sql(where, pinned=True, floored=True),
            params + (FLOOR, 50))]
        assert any(gf.DATE_INDEX in line for line in plan), (where, plan)
        # A full sort would read "USE TEMP B-TREE FOR ORDER BY"; only the
        # per-date tie-break ("RIGHT PART" / "LAST TERM") is acceptable.
        assert not any(line.endswith("FOR ORDER BY") for line in plan), (where, plan)


def test_unpinned_plan_is_the_old_full_sort(db):
    """Documents why the pin exists: without it the planner sorts the whole
    court/language slice (this is the 2026-09-03 shape)."""
    conn = _connect(db)
    plan = [r[3] for r in conn.execute(
        "EXPLAIN QUERY PLAN " + gf.feed_sql("court = ?"), ("bger", 50))]
    assert any("idx_decisions_court" in line for line in plan), plan
    assert any(line.endswith("FOR ORDER BY") for line in plan), plan


# ── output parity ─────────────────────────────────────────────────────────────

def test_every_feed_matches_the_original_query(db):
    conn = _connect(db)
    for where, params in FEEDS:
        stats: dict = {}
        got = gf.query_decisions(conn, where, params, since=FLOOR, stats=stats)
        assert got == _reference(conn, where, params), (where, params, stats)
        assert len(got) == gf.ITEMS_PER_FEED


def test_thin_window_falls_back_and_still_matches(db):
    conn = _connect(db)
    stats: dict = {}
    got = gf.query_decisions(conn, "court = ?", ("bge",), since=FLOOR, stats=stats)
    assert stats["path"] == "fallback" and stats["window_rows"] == 10
    assert got == _reference(conn, "court = ?", ("bge",))
    assert len(got) == 50
    # ...whereas an active court takes the cheap path.
    stats = {}
    gf.query_decisions(conn, "court = ?", ("bger",), since=FLOOR, stats=stats)
    assert stats["path"] == "date-walk"


def test_missing_date_index_uses_the_unpinned_query(db):
    rw = sqlite3.connect(db)
    rw.execute(f"DROP INDEX {gf.DATE_INDEX}")
    rw.commit()
    rw.close()
    conn = _connect(db)
    assert gf.has_index(conn) is False
    stats: dict = {}
    got = gf.query_decisions(conn, "language = ?", ("fr",), since=FLOOR, stats=stats)
    assert stats == {"path": "fallback", "window_rows": None}
    assert got == _reference(conn, "language = ?", ("fr",))


def test_pinned_query_on_a_db_without_the_index_would_error(db):
    """The has_index() guard is load-bearing: INDEXED BY on a missing index
    is a hard SQLite error, not a silent fallback."""
    rw = sqlite3.connect(db)
    rw.execute(f"DROP INDEX {gf.DATE_INDEX}")
    rw.commit()
    rw.close()
    conn = _connect(db)
    with pytest.raises(sqlite3.OperationalError):
        conn.execute(gf.feed_sql("1=1", pinned=True, floored=True), (FLOOR, 50))


def test_null_and_empty_dates_are_excluded_and_order_is_newest_first(db):
    conn = _connect(db)
    stats: dict = {}
    rows = gf.query_decisions(conn, "1=1", (), since=FLOOR, stats=stats)
    assert stats["path"] == "date-walk"
    dates = [r["decision_date"] for r in rows]
    assert all(dates)
    assert dates == sorted(dates, reverse=True)
    # the future-dated bvger row sits at the top of idx_decisions_date; the
    # pinned walk must never surface it
    assert dates[0] <= datetime.now(timezone.utc).date().isoformat()
    # ties broken by decision_id ascending, exactly like the old query
    for a, b in pairwise(rows):
        if a["decision_date"] == b["decision_date"]:
            assert a["decision_id"] < b["decision_id"]


def test_recent_floor_is_one_year_back():
    assert gf.recent_floor(date(2026, 9, 4)) == "2025-09-04"
    assert gf.RECENT_WINDOW_DAYS == 365


# ── main(): files ─────────────────────────────────────────────────────────────

def test_main_writes_seven_feeds_atomically(db, tmp_path, monkeypatch, capsys):
    out = tmp_path / "docs"
    monkeypatch.setattr(sys, "argv", ["generate_feeds.py", "--db", str(db), "--out", str(out)])
    gf.main()

    expected = ["feed.xml"] + [f"feeds/{n}.xml" for n in ("bger", "bvger", "bge", "de", "fr", "it")]
    for rel in expected:
        p = out / rel
        assert p.exists(), rel
        root = ET.parse(p).getroot()
        assert root.tag == "rss"
        items = root.findall("./channel/item")
        assert len(items) == 50, rel
        assert all(i.findtext("link", "").startswith(gf.MCP_URL + "/entscheid/") for i in items)
    assert not list(out.rglob("*.tmp"))
    printed = capsys.readouterr().out
    assert "Generated 7 feeds:" in printed
    assert "feeds/bge.xml (50 items" in printed and "fallback: 10 rows" in printed
    assert "feeds/bger.xml (50 items" in printed and "fallback" not in printed.split("feeds/bger.xml")[1].split("\n")[0]


def test_write_atomic_replaces_in_place_and_leaves_no_temp(tmp_path):
    target = tmp_path / "feed.xml"
    target.write_text("old")
    gf.write_atomic(target, "new")
    assert target.read_text() == "new"
    assert list(tmp_path.iterdir()) == [target]

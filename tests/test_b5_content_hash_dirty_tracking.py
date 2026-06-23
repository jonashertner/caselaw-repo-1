"""B5 — content-hash dirty-tracking + FTS trigger guard.

The nightly content-hash phase used to read+hash the whole ~970k-row table to repair the
~17k hashes the text-mutating passes left stale. B5: those passes now set content_hash=NULL
when they change regeste/full_text, and _compute_content_hashes only re-hashes the NULL'd
rows (WHERE content_hash IS NULL) — and the decisions_au trigger skips FTS reindex on
content_hash-only updates. These tests pin that contract.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import db_schema  # noqa: E402
from build_fts5 import (  # noqa: E402
    _compute_content_hashes,
    _compute_row_content_hash_inline,
    _migrate_short_text_to_regeste,
)


def _mini_conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute(
        "CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, court TEXT, "
        "regeste TEXT, full_text TEXT, content_hash TEXT)"
    )
    return c


def test_migrate_nulls_content_hash():
    c = _mini_conn()
    c.execute("INSERT INTO decisions VALUES ('d1','sh_gerichte','','Art. 116a ZPO. Kostenerlass im Verfahren.','OLDHASH')")
    c.commit()
    assert _migrate_short_text_to_regeste(c) == 1
    r = c.execute("SELECT regeste, full_text, content_hash FROM decisions WHERE decision_id='d1'").fetchone()
    assert r["regeste"] == "Art. 116a ZPO. Kostenerlass im Verfahren."
    assert r["full_text"] is None
    assert r["content_hash"] is None  # dirty-tracked: hash invalidated


def test_compute_content_hashes_only_rehashes_null():
    c = _mini_conn()
    c.execute("INSERT INTO decisions VALUES ('a','c','reg-a','text-a',NULL)")            # NULL -> rehash
    c.execute("INSERT INTO decisions VALUES ('b','c','reg-b','text-b','PRESENT_HASH')")  # present -> gate skips
    c.commit()
    assert _compute_content_hashes(c) == 1  # only the NULL row processed
    assert c.execute("SELECT content_hash FROM decisions WHERE decision_id='a'").fetchone()[0] \
        == _compute_row_content_hash_inline("reg-a", "text-a")
    assert c.execute("SELECT content_hash FROM decisions WHERE decision_id='b'").fetchone()[0] \
        == "PRESENT_HASH"  # gate left it untouched (passes NULL it when they change it)


def test_dirty_tracking_end_to_end():
    c = _mini_conn()
    c.execute("INSERT INTO decisions VALUES ('d','sh_gerichte','','Art. 5 ZGB Treu und Glauben Auslegung.','PRE_HASH')")
    c.commit()
    _migrate_short_text_to_regeste(c)   # mutates + NULLs the hash
    _compute_content_hashes(c)          # re-hashes the NULL'd row
    r = c.execute("SELECT regeste, full_text, content_hash FROM decisions WHERE decision_id='d'").fetchone()
    assert r["content_hash"] == _compute_row_content_hash_inline(r["regeste"], r["full_text"])


def test_au_trigger_guard_keeps_fts_consistent():
    c = sqlite3.connect(":memory:")
    c.executescript(db_schema.SCHEMA_SQL)
    c.execute(
        "INSERT INTO decisions (decision_id,court,canton,docket_number,language,title,regeste,full_text) "
        "VALUES ('d1','bger','CH','1A_1/2025','de','Titel','Verjaehrung im Vertragsrecht','Voller Text')"
    )
    c.commit()
    hits = lambda t: c.execute("SELECT COUNT(*) FROM decisions_fts WHERE decisions_fts MATCH ?", (t,)).fetchone()[0]
    assert hits("Verjaehrung") == 1                       # ai trigger indexed it
    # content_hash-only update: WHEN guard skips reindex, FTS must stay consistent
    c.execute("UPDATE decisions SET content_hash='abc' WHERE decision_id='d1'"); c.commit()
    assert hits("Verjaehrung") == 1                       # not corrupted
    # regeste change: WHEN guard fires reindex, FTS reflects it
    c.execute("UPDATE decisions SET regeste='Haftung aus Delikt' WHERE decision_id='d1'"); c.commit()
    assert hits("Haftung") == 1
    assert hits("Verjaehrung") == 0

"""build_fts5 dedup: keep the RICHEST full_text across shard versions.

Regression for the 2026-06-15 Ticino truncation finding: ~40K decisions
(98.5% ti_gerichte) were served truncated because the direct shard's
text-poor copy (~1.5K) won direct-first INSERT OR IGNORE over the full
es-shard copy (20K+). insert_decision now upgrades the stored full_text
in place on a same-canonical-key collision when the incoming copy is
substantially richer, while preserving the existing row's metadata
(chamber labels — the reason for direct-first). The decisions_au trigger
must propagate the upgrade to decisions_fts so search recall benefits.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import build_fts5  # noqa: E402
from db_schema import SCHEMA_SQL  # noqa: E402


def _conn(tmp_path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(tmp_path / "d.db"))
    c.executescript(SCHEMA_SQL)
    return c


def _row(full_text: str, chamber: str, date: str = "2001-06-01",
         regeste=None) -> dict:
    return {
        "decision_id": "ti_gerichte_12.2001.52", "court": "ti_gerichte",
        "docket_number": "12.2001.52", "decision_date": date, "canton": "TI",
        "chamber": chamber, "language": "it", "title": "Tizio c. Caio",
        "full_text": full_text, "regeste": regeste,
        "source_url": "https://www.sentenze.ti.ch/x",
    }


def test_richer_es_text_upgrades_and_preserves_metadata(tmp_path):
    c = _conn(tmp_path)
    short = "Sentenza breve. " + "A" * 1200
    rich = "CONSIDERANDOUNICO in diritto: " + "B" * 20000
    assert build_fts5.insert_decision(c, _row(short, "DirectChamber")) is True
    # es copy of the SAME decision, far richer text, different metadata
    assert build_fts5.insert_decision(c, _row(rich, "EsChamber")) is True
    row = c.execute(
        "SELECT full_text, chamber FROM decisions WHERE decision_id = ?",
        ("ti_gerichte_12.2001.52",),
    ).fetchone()
    assert len(row[0]) > 20000, "full_text should be upgraded to the richer copy"
    assert row[1] == "DirectChamber", "direct-shard metadata must be preserved"
    # exactly one row (upgrade in place, not a second insert)
    assert c.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 1
    # FTS recall: a word present only in the rich text is now searchable
    n = c.execute(
        "SELECT COUNT(*) FROM decisions_fts WHERE decisions_fts MATCH ?",
        ("CONSIDERANDOUNICO",),
    ).fetchone()[0]
    assert n == 1, "decisions_au trigger should reindex FTS with restored text"


def test_shorter_incoming_text_not_upgraded(tmp_path):
    c = _conn(tmp_path)
    rich = "X" * 20000
    poor = "Y" * 1200
    assert build_fts5.insert_decision(c, _row(rich, "DirectChamber")) is True
    assert build_fts5.insert_decision(c, _row(poor, "EsChamber")) is False
    row = c.execute(
        "SELECT full_text FROM decisions WHERE decision_id = ?",
        ("ti_gerichte_12.2001.52",),
    ).fetchone()
    assert len(row[0]) == 20000, "must keep the longer existing text"


def test_marginally_longer_not_upgraded(tmp_path):
    # Guard the threshold: only >2x AND +1000 chars upgrades; small growth
    # (e.g. boilerplate) must not churn the row.
    c = _conn(tmp_path)
    base = "Z" * 2000
    slightly = "Z" * 2500  # longer but not >2x
    assert build_fts5.insert_decision(c, _row(base, "DirectChamber")) is True
    assert build_fts5.insert_decision(c, _row(slightly, "EsChamber")) is False


def test_different_date_still_suffix_disambiguated(tmp_path):
    # case (ii) regression: same docket, different date -> _d suffix, two rows
    c = _conn(tmp_path)
    assert build_fts5.insert_decision(
        c, _row("A" * 2000, "C", date="2001-06-01")) is True
    assert build_fts5.insert_decision(
        c, _row("B" * 2000, "C", date="2002-07-02")) is True
    ids = [r[0] for r in c.execute("SELECT decision_id FROM decisions")]
    assert len(ids) == 2
    assert any(i.endswith("_d20020702") for i in ids)


def test_regeste_filled_when_missing(tmp_path):
    # If the stored row lacks a regeste and the richer copy has one, take it.
    c = _conn(tmp_path)
    assert build_fts5.insert_decision(
        c, _row("A" * 1200, "DirectChamber", regeste=None)) is True
    assert build_fts5.insert_decision(
        c, _row("B" * 20000, "EsChamber", regeste="Massima ufficiale")) is True
    rg = c.execute(
        "SELECT regeste FROM decisions WHERE decision_id = ?",
        ("ti_gerichte_12.2001.52",),
    ).fetchone()[0]
    assert rg == "Massima ufficiale"

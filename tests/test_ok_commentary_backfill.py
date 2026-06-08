"""Tests for the OnlineKommentar sr_number/abbr backfill (GH #23).

Commentaries for laws outside the scraper's core set were ingested with empty
sr_number AND abbr (only the title carried the abbreviation, e.g. "Art. 13 StHG"),
making them unreachable by abbreviation. The backfill derives sr_number + the
canonical abbreviation from the title via statutes.db.
"""
import json
import sqlite3

import search_stack.build_ok_commentaries_db as okmod
from search_stack.build_ok_commentaries_db import (
    resolve_law_from_title,
    load_abbr_index,
)


def test_resolve_law_from_title_extracts_trailing_abbreviation():
    idx = {"STHG": ("642.14", "StHG"), "OR": ("220", "OR")}
    assert resolve_law_from_title("Art. 13 StHG", idx) == ("642.14", "StHG")
    assert resolve_law_from_title("Vorb. zu Art. 13-14a StHG", idx) == ("642.14", "StHG")
    assert resolve_law_from_title("Art. 41 OR", idx) == ("220", "OR")


def test_resolve_law_from_title_handles_trailing_punctuation():
    idx = {"DBG": ("642.11", "DBG")}
    assert resolve_law_from_title("Art. 16 DBG.", idx) == ("642.11", "DBG")


def test_resolve_law_from_title_returns_none_for_unknown():
    idx = {"STHG": ("642.14", "StHG")}
    assert resolve_law_from_title("Präambel", idx) is None
    assert resolve_law_from_title("", idx) is None
    assert resolve_law_from_title(None, idx) is None


def test_load_abbr_index_maps_all_languages_to_canonical_de_abbr(tmp_path):
    db = tmp_path / "statutes.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE laws (sr_number TEXT, abbr_de TEXT, abbr_fr TEXT, abbr_it TEXT)"
    )
    conn.execute("INSERT INTO laws VALUES ('642.14','StHG','LHID','LAID')")
    conn.execute("INSERT INTO laws VALUES ('220','OR','CO','CO')")
    conn.execute("INSERT INTO laws VALUES ('999.9','','','')")  # no abbr -> skipped
    conn.commit()
    idx = load_abbr_index(conn)
    conn.close()
    # any-language abbreviation resolves to (sr, canonical de abbr)
    assert idx["STHG"] == ("642.14", "StHG")
    assert idx["LHID"] == ("642.14", "StHG")
    assert idx["LAID"] == ("642.14", "StHG")
    assert idx["OR"] == ("220", "OR")
    assert "" not in idx


def test_build_backfills_empty_sr_and_leaves_populated_rows_untouched(tmp_path, monkeypatch):
    sdb = tmp_path / "statutes.db"
    sc = sqlite3.connect(sdb)
    sc.execute("CREATE TABLE laws (sr_number TEXT, abbr_de TEXT, abbr_fr TEXT, abbr_it TEXT)")
    sc.execute("INSERT INTO laws VALUES ('642.14','StHG','LHID','LAID')")
    sc.commit()
    sc.close()
    inp = tmp_path / "commentaries.json"
    inp.write_text(json.dumps([
        {"ok_uuid": "x1", "legislative_act_uuid": "act1", "sr_number": "", "abbr": "",
         "article_num": "13", "title": "Art. 13 StHG", "language": "de", "content_text": "t"},
        {"ok_uuid": "x2", "legislative_act_uuid": "act2", "sr_number": "220", "abbr": "OR",
         "article_num": "41", "title": "Art. 41 OR", "language": "de", "content_text": "t"},
    ]), encoding="utf-8")
    out = tmp_path / "ok.db"
    monkeypatch.setattr(okmod, "INPUT_FILE", inp)
    monkeypatch.setattr(okmod, "OUTPUT_DB", out)
    monkeypatch.setattr(okmod, "STATUTES_DB", sdb)

    okmod.build_db()

    conn = sqlite3.connect(out)
    sthg = conn.execute("SELECT sr_number, abbr FROM commentaries WHERE article_num='13'").fetchone()
    orr = conn.execute("SELECT sr_number, abbr FROM commentaries WHERE article_num='41'").fetchone()
    conn.close()
    assert sthg == ("642.14", "StHG"), "empty-sr StHG row should be backfilled from its title"
    assert orr == ("220", "OR"), "already-populated row must be left untouched"

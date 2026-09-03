"""Defect J (2026-09-03 planning): parse_xml used `.//article`, which also
matches the articles inside transitional / final-provisions blocks
(eId "disp_uN/art_N"), and parse_article discarded the prefix. OR Art. 1 (de)
had 14 indistinguishable rows (main body + 13 amendment blocks), 492 keys
collided on the dev slice, and get_law(220, 2) returned 13 "Art. 2" blocks.

parse_root now records `section` (the block eId prefix, '' for the main
body), `section_heading` (that block's heading) and `eid`, counts and WARNs
every drop and every duplicate (section, article_num) key, and build_db
stores the three columns. `amendment_refs` is gone from the schema.
"""
from __future__ import annotations

import importlib
import json
import logging
import sqlite3
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

import pytest

import search_stack.build_statutes_db as b

AKN = b.AKN_NS

TWO_SECTIONS = f'''<?xml version="1.0" encoding="UTF-8"?>
<akomaNtoso xmlns="{AKN}">
  <act>
    <body>
      <part eId="part_1">
        <heading>Allgemeine Bestimmungen</heading>
        <article eId="art_1">
          <num>Art. 1</num>
          <heading>Abschluss des Vertrages</heading>
          <paragraph eId="art_1/para_1"><num>1</num><content><p>Zum Abschlusse eines Vertrages ist die übereinstimmende gegenseitige Willensäusserung der Parteien erforderlich.</p></content></paragraph>
        </article>
        <article eId="art_24">
          <num><b>Art. 24</b></num>
          <paragraph eId="art_24/para_1"><num>1</num><content><p>Der Irrtum ist namentlich in folgenden Fällen ein wesentlicher.</p></content></paragraph>
        </article>
      </part>
    </body>
    <conclusions>
      <proviso eId="disp_u2">
        <heading>Schlussbestimmungen der Änderung<br/>vom 23. März 1962</heading>
        <article eId="disp_u2/art_1">
          <num>Art. 1</num>
          <content><paragraph eId="disp_u2/art_1/para"><content><p>Der Bundesrat bestimmt den Zeitpunkt des Inkrafttretens.</p></content></paragraph></content>
        </article>
      </proviso>
      <transitional eId="disp_u12">
        <heading>Schlussbestimmungen zum VIII. Titel</heading>
        <article eId="disp_u12/art_2_4">
          <num>Art. 2–4</num>
          <content><paragraph eId="disp_u12/art_2_4/para"><content><p>Die Änderungen können unter AS 1990 802 konsultiert werden.</p></content></paragraph></content>
        </article>
      </transitional>
    </conclusions>
  </act>
</akomaNtoso>'''


def _root(xml: str):
    return ET.fromstring(xml.encode("utf-8"))


def test_sections_and_headings_recorded():
    arts = b.parse_root(_root(TWO_SECTIONS))
    by_key = {(a["section"], a["article_num"]): a for a in arts}
    assert set(by_key) == {("", "1"), ("", "24"), ("disp_u2", "1"), ("disp_u12", "2")}

    main = by_key[("", "1")]
    assert main["section"] == "" and main["section_heading"] is None and main["eid"] == "art_1"
    assert main["heading"] == "Abschluss des Vertrages"

    trans = by_key[("disp_u2", "1")]
    assert trans["eid"] == "disp_u2/art_1"
    # <br/> inside the block heading collapses to one space
    assert trans["section_heading"] == "Schlussbestimmungen der Änderung vom 23. März 1962"
    assert trans["text"] == "Der Bundesrat bestimmt den Zeitpunkt des Inkrafttretens."

    # H: the range article of a transitional block is "2", never "24"
    assert by_key[("disp_u12", "2")]["section_heading"] == "Schlussbestimmungen zum VIII. Titel"
    assert by_key[("", "24")]["text"] == "1 Der Irrtum ist namentlich in folgenden Fällen ein wesentlicher."


def test_stats_counter_threaded_through():
    stats: Counter = Counter()
    arts = b.parse_root(_root(TWO_SECTIONS), stats)
    assert len(arts) == 4
    assert stats["articles"] == 4
    assert stats["dropped_no_num"] == 0 and stats["dropped_no_text"] == 0
    assert stats["duplicate_key"] == 0


def test_duplicate_key_keeps_first_and_warns(caplog):
    xml = f'''<akomaNtoso xmlns="{AKN}"><act><body>
      <article eId="art_5"><num>Art. 5</num><paragraph><content><p>Erste Fassung.</p></content></paragraph></article>
      <article eId="art_5"><num>Art. 5</num><paragraph><content><p>Zweite Fassung.</p></content></paragraph></article>
      <article eId="disp_u1/art_5"><num>Art. 5</num><paragraph><content><p>Andere Sektion.</p></content></paragraph></article>
    </body></act></akomaNtoso>'''
    stats: Counter = Counter()
    with caplog.at_level(logging.WARNING, logger="build_statutes"):
        arts = b.parse_root(_root(xml), stats, source="220/de.xml")
    assert [(a["section"], a["text"]) for a in arts] == [
        ("", "Erste Fassung."), ("disp_u1", "Andere Sektion."),
    ]
    assert stats["duplicate_key"] == 1
    assert stats["articles"] == 2
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "duplicate article" in warnings[0].getMessage()
    assert "220/de.xml" in warnings[0].getMessage()
    assert "art_5" in warnings[0].getMessage()


def test_drops_are_counted_and_warned(caplog):
    # Empty <num> under a range eId (nothing to fill it from) -> no number.
    # A <num> the regex does not understand ("Ziff. I und II") is kept raw,
    # fail-open like #87's unknown ordinals; scripts/statutes_reparse_diff.py
    # sizes those shapes corpus-wide.
    xml = f'''<akomaNtoso xmlns="{AKN}"><act><body>
      <article eId="art_135_149"><num><b> </b></num><paragraph><content><p>Text.</p></content></paragraph></article>
      <article eId="art_7"><num>Art. 7</num></article>
      <article eId="art_8"><num>Art. 8</num><paragraph><content><p>Bleibt.</p></content></paragraph></article>
      <article eId="art_9"><num>Ziff. I und II</num><paragraph><content><p>Roh.</p></content></paragraph></article>
    </body></act></akomaNtoso>'''
    stats: Counter = Counter()
    with caplog.at_level(logging.WARNING, logger="build_statutes"):
        arts = b.parse_root(_root(xml), stats)
    assert [a["article_num"] for a in arts] == ["8", "Ziff. I und II"]
    assert stats["dropped_no_num"] == 1
    assert stats["dropped_no_text"] == 1
    msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("no article number" in m and "art_135_149" in m for m in msgs)
    assert any("no text" in m and "art_7" in m for m in msgs)


def test_parse_xml_accepts_stats(tmp_path):
    p = tmp_path / "de.xml"
    p.write_text(TWO_SECTIONS, encoding="utf-8")
    stats: Counter = Counter()
    arts = b.parse_xml(p, stats)
    assert len(arts) == 4 and stats["articles"] == 4
    # backwards compatible: stats optional
    assert len(b.parse_xml(p)) == 4


def test_parse_xml_counts_parse_errors(tmp_path):
    p = tmp_path / "de.xml"
    p.write_text("<akomaNtoso><act>", encoding="utf-8")
    stats: Counter = Counter()
    assert b.parse_xml(p, stats) == []
    assert stats["parse_error"] == 1


def test_build_db_end_to_end_stores_sections(tmp_path: Path, monkeypatch):
    fedlex_dir = tmp_path / "fedlex"
    xml_dir = fedlex_dir / "xml" / "220"
    xml_dir.mkdir(parents=True)
    (xml_dir / "de.xml").write_text(TWO_SECTIONS, encoding="utf-8")
    (fedlex_dir / "laws.json").write_text(json.dumps([{
        "sr_number": "220", "title_de": "Obligationenrecht", "abbr_de": "OR",
        "consolidation_date": "2026-01-01",
        "work_uri": "https://fedlex.data.admin.ch/eli/cc/27/317_321_377",
    }]))
    db_path = tmp_path / "statutes.db"

    import search_stack.build_statutes_db as bsd
    importlib.reload(bsd)
    monkeypatch.setattr(bsd, "FEDLEX_DIR", fedlex_dir)
    monkeypatch.setattr(bsd, "OUTPUT_DB", db_path)
    bsd.build_db()

    con = sqlite3.connect(db_path)
    rows = con.execute(
        "SELECT article_num, section, section_heading, eid FROM articles "
        "WHERE sr_number='220' AND lang='de' ORDER BY section, article_num"
    ).fetchall()
    assert rows == [
        ("1", "", None, "art_1"),
        ("24", "", None, "art_24"),
        ("2", "disp_u12", "Schlussbestimmungen zum VIII. Titel", "disp_u12/art_2_4"),
        ("1", "disp_u2", "Schlussbestimmungen der Änderung vom 23. März 1962", "disp_u2/art_1"),
    ]
    # J: OR Art. 1 main body is exactly one row once section is in the key
    assert con.execute(
        "SELECT COUNT(*) FROM articles WHERE sr_number='220' AND article_num='1' AND section=''"
    ).fetchone()[0] == 1

    names = {(r[0], r[1]) for r in con.execute("SELECT type, name FROM sqlite_master")}
    assert ("index", "idx_articles_key") in names
    assert ("table", "amendment_refs") not in names
    assert ("index", "idx_amendment_refs_eli") not in names
    # articles_fts unchanged: 5 columns, text is column 3
    fts_cols = [r[1] for r in con.execute("PRAGMA table_info(articles_fts)")]
    assert fts_cols == ["sr_number", "article_num", "heading", "text", "lang"]
    assert con.execute("SELECT COUNT(*) FROM articles_fts WHERE articles_fts MATCH 'Irrtum'").fetchone()[0] == 1
    # journal mode is DELETE for the immutable=1 readers, no sidecars left
    assert con.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert not (tmp_path / "statutes.tmp").exists()
    assert not (tmp_path / "statutes.tmp-wal").exists()


def test_build_db_removes_stale_tmp_sidecars(tmp_path: Path, monkeypatch):
    fedlex_dir = tmp_path / "fedlex"
    (fedlex_dir / "xml").mkdir(parents=True)
    (fedlex_dir / "laws.json").write_text("[]")
    db_path = tmp_path / "statutes.db"
    for leftover in ("statutes.tmp", "statutes.tmp-wal", "statutes.tmp-shm"):
        (tmp_path / leftover).write_bytes(b"junk")

    import search_stack.build_statutes_db as bsd
    importlib.reload(bsd)
    monkeypatch.setattr(bsd, "FEDLEX_DIR", fedlex_dir)
    monkeypatch.setattr(bsd, "OUTPUT_DB", db_path)
    bsd.build_db()
    assert db_path.exists()
    for leftover in ("statutes.tmp", "statutes.tmp-wal", "statutes.tmp-shm"):
        assert not (tmp_path / leftover).exists(), leftover


def test_make_statutes_conn_uses_real_schema():
    from tests._statutes_fixture import make_statutes_conn
    con = make_statutes_conn([
        {"sr_number": "220", "article_num": "1", "text": "Hauptteil."},
        {"sr_number": "220", "article_num": "1", "text": "Übergang.",
         "section": "disp_u2", "section_heading": "Schlussbestimmungen"},
    ])
    cols = [r[1] for r in con.execute("PRAGMA table_info(articles)")]
    assert cols == ["id", "sr_number", "article_num", "heading", "footnote", "text", "xml",
                    "lang", "section", "section_heading", "eid"]
    rows = con.execute("SELECT section, eid, text FROM articles ORDER BY id").fetchall()
    assert rows == [("", "art_1", "Hauptteil."), ("disp_u2", "disp_u2/art_1", "Übergang.")]
    assert con.execute("SELECT abbr_de FROM laws WHERE sr_number='220'").fetchone()[0] == "OR"
    assert con.execute("SELECT COUNT(*) FROM articles_fts WHERE articles_fts MATCH 'Hauptteil'").fetchone()[0] == 1
    with pytest.raises(KeyError):
        make_statutes_conn([{"sr_number": "220", "article_num": "1", "text": "x", "bogus": 1}])


def test_block_rows_sharing_a_number_are_all_kept():
    """Production gate 2026-09-03: Fedlex reuses eIds inside the declaration
    blocks of treaties (SR 0.131.1 has seven `decl_u2/art_1`, one per
    declaring state). Those are content, not duplicates; only the main body
    is deduplicated."""
    xml = f'''<akomaNtoso xmlns="{AKN}"><act><body>
      <article eId="art_1"><num>Art. 1</num><paragraph><content><p>Hauptteil.</p></content></paragraph></article>
      <declarations eId="decl_u2"><heading>Erklärungen</heading>
        <article eId="decl_u2/art_1"><num>1</num><paragraph><content><p>Erklärung Österreich.</p></content></paragraph></article>
        <article eId="decl_u2/art_1"><num>1</num><paragraph><content><p>Erklärung Deutschland.</p></content></paragraph></article>
        <article eId="decl_u2/art_1"><num>1</num><paragraph><content><p>Erklärung Schweiz.</p></content></paragraph></article>
      </declarations>
    </body></act></akomaNtoso>'''
    stats: Counter = Counter()
    arts = b.parse_root(_root(xml), stats)
    assert [(a["section"], a["article_num"], a["text"]) for a in arts] == [
        ("", "1", "Hauptteil."),
        ("decl_u2", "1", "Erklärung Österreich."),
        ("decl_u2", "1", "Erklärung Deutschland."),
        ("decl_u2", "1", "Erklärung Schweiz."),
    ]
    assert stats["duplicate_key"] == 0 and stats["articles"] == 4
    assert all(a["section_heading"] == "Erklärungen" for a in arts[1:])


def test_deleted_rule_keeps_its_heading_as_body():
    """Treaty regulations mark deleted rules as a number plus "[Gelöscht]"
    and nothing else (77 rows corpus-wide). The heading is the only content;
    the old fallback served the number itself as text."""
    xml = f'''<akomaNtoso xmlns="{AKN}"><act><body>
      <article eId="art_30"><num><b>Regel 30</b></num><heading>[<i>Gelöscht</i>]</heading></article>
      <article eId="art_31"><num><b>Regel 31</b></num><heading>Fristen</heading>
        <paragraph><content><p>Die Frist beträgt zwei Monate.</p></content></paragraph></article>
    </body></act></akomaNtoso>'''
    stats: Counter = Counter()
    arts = b.parse_root(_root(xml), stats)
    assert [(a["article_num"], a["heading"], a["text"]) for a in arts] == [
        ("Regel 30", "[Gelöscht]", "[Gelöscht]"),
        ("Regel 31", "Fristen", "Die Frist beträgt zwei Monate."),
    ]
    assert stats["dropped_no_text"] == 0

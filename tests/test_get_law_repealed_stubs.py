"""Repealed articles: empty body, note in `footnote`, labelled — on the
current-law path (which now exposes `footnote`) and for old-shape rows.

Before 2026-09-05 a repealed article (ZGB Art. 10: a number plus
"Aufgehoben durch ...") was served with the note as its body, and the
current-law path did not return `footnote` at all, so a caller asking for
"the text of Art. 10 ZGB" got the amendment note and nothing said so.
LawRider's edition comparison (2026-09-03) counted every such row as a
footnote spliced into the article. The production statutes.db still holds
old-shape rows until the next monthly rebuild, so the read side normalises
them itself (_label_statute_stubs); the parser produces the new shape.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
for p in (REPO, REPO / "tests"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import mcp_server as m  # noqa: E402
from _statutes_fixture import make_statutes_conn  # noqa: E402

NOTE_10 = ("Aufgehoben durch Anhang 1 Ziff. II 3 der Zivilprozessordnung vom 19. Dez. 2008, "
           "mit Wirkung seit 1. Jan. 2011 (AS 2010 1739; BBl 2006 7221).")
NOTE_15 = "Aufgehoben durch Ziff. I des BG vom 7. Okt. 1994, mit Wirkung seit 1. Jan. 1996."
NOTE_94 = ("Fassung gemäss Ziff. I des BG vom 18. Dez. 2020 (Ehe für alle), in Kraft seit "
           "1. Juli 2022 (AS 2021 747; BBl 2019 8595; 2020 1273).")
TEXT_94 = ("Die Ehe kann von zwei Personen eingegangen werden, die das 18. Altersjahr "
           "zurückgelegt haben und urteilsfähig sind.")

ROWS = [
    # old-shape row (built before 2026-09-05): the note doubles as the body
    {"sr_number": "210", "article_num": "10", "text": NOTE_10, "footnote": NOTE_10},
    # new-shape row: empty body, note kept
    {"sr_number": "210", "article_num": "15", "text": "", "footnote": NOTE_15},
    # live article that carries an amendment note
    {"sr_number": "210", "article_num": "94", "text": TEXT_94, "footnote": NOTE_94},
    # live article without a note
    {"sr_number": "210", "article_num": "1", "heading": "A. Anwendung des Rechts",
     "text": "1 Das Gesetz findet auf alle Rechtsfragen Anwendung, für die es nach Wortlaut "
             "oder Auslegung eine Bestimmung enthält."},
]


def _conn() -> sqlite3.Connection:
    conn = make_statutes_conn(ROWS)
    conn.row_factory = sqlite3.Row
    return conn


@pytest.fixture
def statutes(monkeypatch, tmp_path):
    fake = tmp_path / "statutes.db"
    fake.touch()
    monkeypatch.setattr(m, "STATUTES_DB_PATH", fake)
    monkeypatch.setattr(m, "_get_statutes_conn", _conn)
    monkeypatch.setattr(m, "_statute_text_cache", {})
    monkeypatch.setattr(m, "_FEDLEX_WORK_URI_MAP", None)
    monkeypatch.setattr(m, "_fetch_pending_changes", lambda sr: [])
    monkeypatch.setattr(m, "_get_materialien_for_doctrine", lambda *a, **k: None, raising=False)


def test_current_path_exposes_footnote_and_keeps_live_text(statutes):
    res = m.get_law(sr_number="210", article="94")
    art = res["articles"][0]
    assert art["text"] == TEXT_94
    assert art["footnote"] == NOTE_94
    assert "empty_body" not in art
    out = m._format_get_law_response(res)
    assert TEXT_94 in out
    assert f"> Footnote: {NOTE_94}" in out
    # the note is rendered once, as a footnote, never inside the body
    assert out.count("Fassung gemäss") == 1


def test_old_shape_repealed_row_is_blanked_and_labelled(statutes):
    res = m.get_law(sr_number="210", article="10")
    art = res["articles"][0]
    assert art["text"] == ""
    assert art["empty_body"] is True
    assert art["footnote"] == NOTE_10
    out = m._format_get_law_response(res)
    assert "### Art. 10" in out
    assert "No article text in this edition" in out
    assert f"> Footnote: {NOTE_10}" in out
    assert out.count("Aufgehoben durch") == 1


def test_new_shape_repealed_row_is_labelled(statutes):
    res = m.get_law(sr_number="210", article="15")
    art = res["articles"][0]
    assert art["text"] == "" and art["empty_body"] is True and art["footnote"] == NOTE_15


def test_article_without_note_is_untouched(statutes):
    res = m.get_law(sr_number="210", article="1")
    art = res["articles"][0]
    assert art["footnote"] is None
    assert "empty_body" not in art
    assert art["text"].startswith("1 Das Gesetz findet")


def test_quote_rail_source_for_a_stub_is_empty(statutes):
    """The quote rail must not verify a quote of the repeal note as the
    wording of Art. 10 ZGB: an old-shape row yields no statute text, a live
    article yields its body."""
    m._statute_text_cache.clear()
    stub = m._fetch_statute_text(law_code="ZGB", article="10", full=True)
    assert stub["sr_number"] == "210" and stub["text"] == ""
    live = m._fetch_statute_text(law_code="ZGB", article="94", full=True)
    assert live["text"] == TEXT_94


def test_label_helper_cases():
    rows = [
        {"text": "…", "footnote": "Aufgehoben."},            # ellipsis body + note
        {"text": "Aufgehoben.", "footnote": "Aufgehoben."},  # old shape: note as body
        {"text": "", "footnote": "Aufgehoben."},             # new shape
        {"text": "Live text.", "footnote": "Fassung gemäss Ziff. I."},
        {"text": ""},                                        # no note: not labelled
        {"text": "Aufgehoben."},                             # DB without the column
    ]
    m._label_statute_stubs(rows)
    assert rows[0] == {"text": "", "footnote": "Aufgehoben.", "empty_body": True}
    assert rows[1] == {"text": "", "footnote": "Aufgehoben.", "empty_body": True}
    assert rows[2] == {"text": "", "footnote": "Aufgehoben.", "empty_body": True}
    assert rows[3] == {"text": "Live text.", "footnote": "Fassung gemäss Ziff. I."}
    assert rows[4] == {"text": ""}
    assert rows[5] == {"text": "Aufgehoben."}


def test_historical_rendering_of_a_stub():
    """The as_of formatter takes the same article dicts; a stub renders the
    placeholder and the footnote, not an empty heading."""
    res = {
        "sr_number": "210", "abbreviation": "ZGB", "title": "Schweizerisches Zivilgesetzbuch",
        "canton": "CH", "level": "federal", "language": "de", "version": "historical",
        "as_of": "2022-01-01", "snapshot_date": "2022-01-01",
        "source_url": "https://www.fedlex.admin.ch/eli/cc/24/233_245_233/20220101/de",
        "source_label": "Fedlex (Fassung vom 2022-01-01)",
        "text_source": "fedlex_xml", "structure": "articles", "verbatim_quotation": "verbatim",
        "articles": [{"article_num": "10", "heading": None, "text": "",
                      "footnote": NOTE_10, "section": "", "empty_body": True}],
    }
    out = m._format_get_law_response(res)
    assert "### Art. 10" in out
    assert "No article text in this edition" in out
    assert f"> Footnote: {NOTE_10}" in out

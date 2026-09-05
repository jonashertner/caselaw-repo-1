"""Defect B (2026-09-03 review): parse_article skipped <authorialNote> only
inside <num>. Fedlex attaches amendment notes to headings and to individual
paragraphs too, so 23.5 % of rows on the dev slice carried "Fassung gemäss
Ziff. I des BG vom ..." spliced into the body, and 81 headings ended in
footnote prose (BV Art. 61a Abs. 3 quoted "Berichtigt von der
Redaktionskommission" as statute text).

Every note in the article now goes to `footnote`, in document order,
deduplicated; none stays in `heading` or `text`. Repealed articles, whose
only text IS the note, keep it as the body so they survive the build.
"""
from __future__ import annotations

import logging
import xml.etree.ElementTree as ET

import search_stack.build_statutes_db as b

AKN = b.AKN_NS


def _article(inner: str, eid: str = "art_1") -> ET.Element:
    return ET.fromstring(f'<article xmlns="{AKN}" eId="{eid}">{inner}</article>')


def _doc(articles_xml: str) -> str:
    return (f'<?xml version="1.0" encoding="UTF-8"?><akomaNtoso xmlns="{AKN}"><act><body>'
            f"{articles_xml}</body></act></akomaNtoso>")


NOTE_BODY = ("Fassung gemäss Ziff. I des BG vom 19. Dez. 2008, in Kraft seit 1. Jan. 2011 "
             "(AS 2010 1739; BBl 2006 7221).")


def test_note_in_body_paragraph_goes_to_footnote_not_text():
    art = _article(
        "<num>Art. 41</num>"
        "<paragraph><num>1</num><content><p>Wer einem andern widerrechtlich Schaden zufügt."
        f"<authorialNote><p>{NOTE_BODY}</p></authorialNote></p></content></paragraph>"
    )
    num, _heading, text, footnote = b.parse_article(art)
    assert num == "41"
    assert text == "1 Wer einem andern widerrechtlich Schaden zufügt."
    assert "Fassung gemäss" not in text
    assert footnote == NOTE_BODY


def test_note_in_heading_removed_from_heading():
    art = _article(
        "<num>Art. 61<i>a</i></num>"
        "<heading>Bildungsraum Schweiz<authorialNote><p>Berichtigt von der Redaktionskommission "
        "der BVers (Art. 58 Abs. 1 ParlG).</p></authorialNote></heading>"
        "<paragraph><content><p>Bund und Kantone sorgen gemeinsam.</p></content></paragraph>"
    )
    num, heading, text, footnote = b.parse_article(art)
    assert num == "61a"
    assert heading == "Bildungsraum Schweiz"
    assert "Berichtigt" not in text
    assert footnote.startswith("Berichtigt von der Redaktionskommission")


def test_heading_tail_star_stripped_bv_10a():
    art = _article(
        "<num><b>Art. 10</b><i>a</i><authorialNote><p>Angenommen in der Volksabstimmung vom "
        "7. März 2021.</p></authorialNote></num>"
        "<heading>Verbot der Verhüllung des eigenen Gesichts"
        "<authorialNote><p><sup>*</sup> Mit Übergangsbestimmung.</p></authorialNote>*</heading>"
        "<paragraph><num>1</num><content><p>Niemand darf sein Gesicht verhüllen.</p></content></paragraph>",
        eid="art_10_a",
    )
    num, heading, text, footnote = b.parse_article(art)
    assert num == "10a"
    assert heading == "Verbot der Verhüllung des eigenen Gesichts"
    assert text == "1 Niemand darf sein Gesicht verhüllen."
    assert footnote == ("Angenommen in der Volksabstimmung vom 7. März 2021. "
                        "* Mit Übergangsbestimmung.")


def test_notes_collected_in_document_order_and_deduplicated():
    art = _article(
        "<num>Art. 5<authorialNote><p>NOTE-NUM</p></authorialNote></num>"
        "<heading>Titel<authorialNote><p>NOTE-HEAD</p></authorialNote></heading>"
        "<paragraph><num>1</num><content><p>Eins.<authorialNote><p>NOTE-BODY</p></authorialNote></p></content></paragraph>"
        "<paragraph><num>2</num><content><p>Zwei.<authorialNote><p>NOTE-BODY</p></authorialNote></p></content></paragraph>"
    )
    _num, heading, text, footnote = b.parse_article(art)
    assert footnote == "NOTE-NUM NOTE-HEAD NOTE-BODY"
    assert heading == "Titel"
    assert text == "1 Eins.\n2 Zwei."


def test_pure_repeal_article_has_empty_body_and_keeps_the_note():
    # ZGB Art. 10: <num> plus a note and nothing else. The note is not the
    # article's wording (2026-09-05): body "", note in `footnote`.
    art = _article(
        "<num><b>Art. 10</b><authorialNote><p>Aufgehoben durch Anhang 1 Ziff. II 3 der "
        "Zivilprozessordnung vom 19. Dez. 2008.</p></authorialNote></num>",
        eid="art_10",
    )
    num, _heading, text, footnote = b.parse_article(art)
    assert num == "10"
    assert text == ""
    assert footnote.startswith("Aufgehoben durch")


def test_ellipsis_body_with_note_has_empty_body():
    # 32 rows on the slice have a body of just "…" plus the repeal note.
    art = _article(
        "<num>Art. 42<authorialNote><p>Aufgehoben in der Volksabstimmung vom 28. Nov. 2004.</p>"
        "</authorialNote></num><paragraph><content><p>…</p></content></paragraph>"
    )
    _n, _h, text, footnote = b.parse_article(art)
    assert text == ""
    assert footnote == "Aufgehoben in der Volksabstimmung vom 28. Nov. 2004."


def test_empty_content_paragraph_with_note_keeps_the_note():
    # OR it disp_u16/art_4: <content><paragraph><content/></paragraph></content>
    art = _article(
        "<num><b>Art</b><b>. 4</b><authorialNote><p>Abrogato dall’all. n. 2 della LF del 3 ott. "
        "2003.</p></authorialNote></num><content><paragraph><content /></paragraph></content>",
        eid="disp_u16/art_4",
    )
    num, _h, text, footnote = b.parse_article(art)
    assert num == "4"
    assert text == ""
    assert footnote.startswith("Abrogato")


def test_live_article_with_note_keeps_its_body():
    art = _article(
        "<num><b>Art. 94</b><authorialNote><p>Fassung gemäss Ziff. I des BG vom 18. Dez. 2020.</p>"
        "</authorialNote></num><paragraph><content><p>Die Ehe kann von zwei Personen eingegangen "
        "werden.</p></content></paragraph>",
        eid="art_94",
    )
    _n, _h, text, footnote = b.parse_article(art)
    assert text == "Die Ehe kann von zwei Personen eingegangen werden."
    assert footnote == "Fassung gemäss Ziff. I des BG vom 18. Dez. 2020."


def test_article_without_note_and_without_text_has_empty_text():
    art = _article("<num>Art. 7</num>")
    num, _h, text, footnote = b.parse_article(art)
    assert num == "7" and text == "" and footnote is None


def test_pure_repeal_row_survives_parse_xml(tmp_path, caplog):
    p = tmp_path / "de.xml"
    p.write_text(_doc(
        '<article eId="art_10"><num><b>Art. 10</b><authorialNote><p>Aufgehoben.</p></authorialNote>'
        "</num></article>"
        '<article eId="art_11"><num>Art. 11</num><paragraph><content><p>Text.</p></content></paragraph>'
        "</article>"
    ), encoding="utf-8")
    with caplog.at_level(logging.WARNING, logger="build_statutes"):
        arts = b.parse_xml(p)
    # The repealed article is a row (empty body, note kept), not a drop.
    assert [a["article_num"] for a in arts] == ["10", "11"]
    assert arts[0]["text"] == "" and arts[0]["footnote"] == "Aufgehoben."
    assert not [r for r in caplog.records if "drop" in r.getMessage()]


def test_article_with_neither_text_nor_note_is_still_dropped(tmp_path, caplog):
    p = tmp_path / "de.xml"
    p.write_text(_doc(
        '<article eId="art_7"><num>Art. 7</num></article>'
        '<article eId="art_8"><num>Art. 8</num><paragraph><content><p>Text.</p></content></paragraph>'
        "</article>"
    ), encoding="utf-8")
    with caplog.at_level(logging.WARNING, logger="build_statutes"):
        arts = b.parse_xml(p)
    assert [a["article_num"] for a in arts] == ["8"]
    assert [r for r in caplog.records if "drop (no text)" in r.getMessage()]


def test_fallback_branch_excludes_num_heading_and_notes():
    # No <paragraph>, no direct <content>: everything else is the body, but
    # never the number, the heading or a note.
    art = _article(
        "<num>Art. 3</num><heading>Kopf<authorialNote><p>NOTE</p></authorialNote></heading>"
        "<intro><p>Einleitung.</p></intro><wrapUp><p>Schluss.</p></wrapUp>"
    )
    num, heading, text, footnote = b.parse_article(art)
    assert num == "3" and heading == "Kopf" and footnote == "NOTE"
    assert text == "Einleitung. Schluss."

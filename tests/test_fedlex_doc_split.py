"""Article split of a Fedlex Word edition from its HTML export.

About a fifth of the pre-2021 consolidations Fedlex publishes only as PDF
also ship the Bundeskanzlei .doc, whose paragraph styles carry the article
structure the PDF loses. The fixture is a slice of real `textutil -convert
html` output for the 2017-04-01 edition of SR 220 (Art. 59-61 plus one
structural title and one final-provisions article), so the class discovery
is exercised against the markup the converter really produces.
"""
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

FIXTURE = REPO / "tests" / "fixtures" / "fedlex_or2017_doc_excerpt.html"


def test_splits_real_textutil_markup_into_articles():
    arts = m._split_fedlex_doc_html(FIXTURE.read_text(encoding="utf-8"))
    by = {a["article_num"]: a for a in arts}
    assert list(by) == ["59", "59a", "60", "61", "1"]
    a60 = by["60"]
    assert a60["heading"] == "G. Verjährung"
    paras = a60["text"].split("\n")
    assert len(paras) == 3
    assert paras[0].startswith("1 Der Anspruch auf Schadenersatz oder Genugtuung verjährt in einem Jahre")
    assert paras[1].startswith("2 Wird jedoch die Klage aus einer strafbaren Handlung")
    assert paras[2].startswith("3 Ist durch die unerlaubte Handlung")
    # "Art. 59" + "a" sit in two <b> runs; the marker is joined, not split.
    assert by["59a"]["heading"].startswith("F. Haftung für kryptografische Schlüssel")
    # The 12px structural title ("Erste Abteilung …") never lands in a body.
    assert all("Erste Abteilung" not in a["text"] for a in arts)
    # A one-sentence article without an Absatz number keeps its text.
    assert by["1"]["heading"] == "A. Schlusstitel des Zivilgesetzbuches"
    assert by["1"]["text"].startswith("Der Schlusstitel des Zivilgesetzbuches gilt")
    assert all(a["section"] == "" and a["footnote"] is None for a in arts)


def test_no_marker_class_means_no_structure():
    assert m._split_fedlex_doc_html("") == []
    assert m._split_fedlex_doc_html("<html><body><p class=\"x\">hello</p></body></html>") == []
    # Two markers are not enough evidence for a marker class.
    html = ('<style>p1 {font: 9.0px x} p2 {font: 9.0px x}</style>'
            '<p class="p1">Art. 1</p><p class="p2">a</p><p class="p1">Art. 2</p><p class="p2">b</p>')
    assert m._split_fedlex_doc_html(html) == []


def test_duplicate_numbers_keep_the_first_and_unnumbered_bodies_survive():
    html = '<style>p9 {font: 9.0px x} p11 {font: 9.0px x}</style>' + "".join(
        f'<p class="p9"><b>Art. {n}</b></p><p class="p11">{t}</p>'
        for n, t in [("1", "main one"), ("2", "main two"), ("3", "main three"), ("1", "final provisions one")])
    arts = m._split_fedlex_doc_html(html)
    assert [(a["article_num"], a["text"]) for a in arts] == [("1", "main one"), ("2", "main two"), ("3", "main three")]


def test_structural_title_ends_the_article_and_marginal_notes_become_headings():
    html = ('<style>p8 {font: 12.0px x} p9 {font: 9.0px x} p10 {font: 6.5px x} p11 {font: 9.0px x}</style>'
            '<p class="p9"><b>Art. 1</b></p><p class="p10">A. Zweck</p>'
            '<p class="p11"><span class="s4">1</span> Erster Absatz.</p>'
            '<p class="p11"><span class="s4">2</span> Zweiter Absatz.</p>'
            '<p class="p8">Zweiter Titel: Etwas anderes</p>'
            '<p class="p11">3 Dieser Text gehört keinem Artikel mehr.</p>'
            '<p class="p9"><b>Art. 2</b></p><p class="p10">B. Geltung</p>'
            '<p class="p11"><span class="s4">1</span> Gilt.</p>'
            '<p class="p9"><b>Art. 3</b></p><p class="p11"><span class="s4">1</span> Ohne Randtitel.</p>')
    arts = m._split_fedlex_doc_html(html)
    assert [(a["article_num"], a["heading"], a["text"]) for a in arts] == [
        ("1", "A. Zweck", "1 Erster Absatz.\n2 Zweiter Absatz."),
        ("2", "B. Geltung", "1 Gilt."),
        ("3", None, "1 Ohne Randtitel."),
    ]

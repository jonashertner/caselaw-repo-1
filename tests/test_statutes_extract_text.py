"""Defect C (2026-09-03 review): extract_text stripped every fragment and
joined with a space, so inline markup split tokens. "28<i>a</i>" became
"28 a", "1<sup>bis</sup>" became "1 bis", "Auf<inline>wertung</inline>"
became "Auf wertung", and 10 French StGB articles whose <num> is
"<b>Art</b><b>. 264</b><i>a</i>" were stored under ". 264 a" and were
unreachable. FTS "305bis" missed 9 of 12 rows.

The join now concatenates raw text; only block-level children add a space at
their boundaries, <sup> counts as inline only when purely alphabetic.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET

import search_stack.build_statutes_db as b

AKN = b.AKN_NS


def _el(inner: str, tag: str = "p") -> ET.Element:
    return ET.fromstring(f'<{tag} xmlns="{AKN}">{inner}</{tag}>')


def _text(inner: str, **kw) -> str:
    return b.extract_text(_el(inner), **kw)


# ── suffixes and split words join ────────────────────────────────────────────

def test_italic_letter_suffix_joins():
    assert _text("Art. 28<i>a</i>") == "Art. 28a"


def test_alphabetic_sup_ordinal_joins():
    assert _text("1<sup>bis</sup>") == "1bis"
    assert _text("Art. 305<sup>bis</sup>") == "Art. 305bis"


def test_french_ordinal_sup_joins():
    assert _text("le 1<sup>er</sup> janvier") == "le 1er janvier"


def test_inline_split_word_joins():
    assert _text("Auf<inline>wertung</inline>") == "Aufwertung"


def test_ref_in_parentheses_has_no_inner_spaces():
    assert _text("(<ref href=\"x\">SR 210</ref>)") == "(SR 210)"


def test_bold_split_number_with_dot_in_second_fragment():
    # StGB fr Art. 264a..264m: the dot and the number sit in the second <b>.
    assert _text("<b>Art</b><b>. 264</b><i>a</i>") == "Art. 264a"


def test_letter_plus_ordinal_join():
    assert _text("<b>Art. 268</b><i>a</i><sup>bis</sup>") == "Art. 268abis"


# ── things that must stay separated ──────────────────────────────────────────

def test_numeric_sup_stays_separate():
    # <sup>1</sup> is a paragraph marker, not a suffix.
    assert _text("<sup>1</sup> Die Ehe wird geschlossen.") == "1 Die Ehe wird geschlossen."


def test_two_numbers_in_sup_are_not_welded():
    assert _text("3953<sup>3957</sup>") == "3953 3957"


def test_block_children_get_a_space():
    assert b.extract_text(_el("<p>a</p><p>b</p>", tag="content")) == "a b"


def test_br_becomes_space():
    assert _text("Schlussbestimmungen<br/>vom 23. März 1962") == "Schlussbestimmungen vom 23. März 1962"


def test_list_items_separated():
    xml = "<blockList><item><num>a.</num><p>eins;</p></item><item><num>b.</num><p>zwei.</p></item></blockList>"
    assert b.extract_text(_el(xml, tag="content")) == "a. eins; b. zwei."


# ── normalisation ────────────────────────────────────────────────────────────

def test_nbsp_normalised_to_space():
    assert _text("Art. 663") == "Art. 663"


def test_non_breaking_hyphen_kept():
    assert _text("Basel‑Stadt") == "Basel‑Stadt"


def test_whitespace_collapsed_once_at_end():
    assert _text("  Bund \n\t und   Kantone  ") == "Bund und Kantone"


def test_no_per_fragment_strip_keeps_interior_spacing():
    # Old code stripped each fragment: "vom 7. März 2021" + ", in Kraft" gave
    # "2021 , in Kraft". Raw tails keep their own punctuation spacing.
    assert _text("<ref>Volksabstimmung vom 7. März 2021</ref>, in Kraft seit") == \
        "Volksabstimmung vom 7. März 2021, in Kraft seit"


# ── skipped tags ─────────────────────────────────────────────────────────────

def test_skipped_note_contributes_nothing_but_keeps_tail():
    xml = "Schadenersatz<authorialNote><p>Fassung gemäss AS 2020</p></authorialNote> und Genugtuung"
    assert _text(xml, skip_tags={"authorialNote"}) == "Schadenersatz und Genugtuung"


def test_skipped_note_tail_leading_star_removed():
    # BV Art. 10a heading: the "*" after the note is the marker paired with
    # the note's own <sup>*</sup>.
    xml = ("Verbot der Verhüllung des eigenen Gesichts"
           "<authorialNote><p><sup>*</sup> Mit Übergangsbestimmung.</p></authorialNote>*")
    assert _text(xml, skip_tags={"authorialNote"}) == "Verbot der Verhüllung des eigenen Gesichts"


def test_star_inside_normal_text_untouched():
    assert _text("a * b", skip_tags={"authorialNote"}) == "a * b"


# ── the same cases through parse_article ─────────────────────────────────────

def _article(eid: str, num_inner: str) -> ET.Element:
    return ET.fromstring(
        f'<article xmlns="{AKN}" eId="{eid}"><num>{num_inner}</num>'
        f"<paragraph><content><p>Text.</p></content></paragraph></article>"
    )


def test_parse_article_french_dot_split_number():
    num, *_ = b.parse_article(_article("art_264_a", "<b>Art</b><b>. 264</b><i>a</i>"))
    assert num == "264a"


def test_parse_article_letter_plus_ordinal():
    num, *_ = b.parse_article(_article("art_268_a_bis", "<b>Art. 268</b><i>a</i><sup>bis</sup>"))
    assert num == "268abis"


def test_parse_article_sup_ordinal():
    num, *_ = b.parse_article(_article("art_305_bis", "<b>Art. 305</b><sup>bis</sup>"))
    assert num == "305bis"


def test_parse_article_body_suffix_join():
    art = ET.fromstring(
        f'<article xmlns="{AKN}" eId="art_1"><num>Art. 1</num>'
        "<paragraph><content><p>Gemäss Art. 28<i>a</i> und Art. 1<sup>bis</sup>.</p></content></paragraph>"
        "</article>"
    )
    _n, _h, text, _f = b.parse_article(art)
    assert text == "Gemäss Art. 28a und Art. 1bis."

"""Issue #32: Fedlex splits some article numbers across sibling <b> tags
(<b>Art. 16</b><b>8</b> -> extract_text "Art. 16 8"), so parse_article's number
regex truncated "168" to "16" and the article was stored under the wrong number
(ZPO Art. 168 had no German row). parse_article now repairs pure-digit
truncations from the authoritative eId, leaving suffixed articles (38a, whose
eId is "art_38_a") in their corpus format, untouched.
"""
from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import search_stack.build_statutes_db as b  # noqa: E402

AKN = b.AKN_NS
BODY = "<paragraph><content><p>Beweismittel sind das Zeugnis ...</p></content></paragraph>"


def _article(eid, num_inner):
    xml = f'<article xmlns="{AKN}" eId="{eid}"><num>{num_inner}</num>{BODY}</article>'
    return ET.fromstring(xml)


def test_split_bold_number_repaired_from_eid():
    num, _h, text, _f = b.parse_article(_article("art_168", "<b>Art. 16</b><b>8</b>"))
    assert num == "168"
    assert text  # the body text is preserved


def test_three_way_split_repaired():
    num, *_ = b.parse_article(_article("art_270", "<b>Art. 2</b><b>7</b><b>0</b>"))
    assert num == "270"


def test_suffixed_article_keeps_corpus_format():
    # eId uses an underscore (art_38_a) but the corpus format is "38a"; it must
    # NOT become "38_a" and must NOT be "corrected".
    num, *_ = b.parse_article(_article("art_38_a", "Art. 38a"))
    assert num == "38a"


def test_correct_simple_number_unchanged():
    num, *_ = b.parse_article(_article("art_16", "<b>Art. 16</b>"))
    assert num == "16"


def test_correct_number_not_overridden_when_eid_matches():
    num, *_ = b.parse_article(_article("art_168", "<b>Art. 168</b>"))
    assert num == "168"


# ── GitHub #87: ordinals past "novies" ────────────────────────────────────────
# The suffix alternation stopped at "novies", so "322decies" fell through to the
# single-letter branch and was stored as "322d": present in the corpus, and
# unreachable by the number anyone would look it up with.


def test_decies_parsed_whole():
    num, *_ = b.parse_article(_article("art_322decies", "Art. 322decies"))
    assert num == "322decies"


def test_decies_parsed_whole_second_case():
    num, *_ = b.parse_article(_article("art_179decies", "Art. 179decies"))
    assert num == "179decies"


def test_undecies_and_duodecies_parsed_whole():
    assert b.parse_article(_article("art_5undecies", "Art. 5undecies"))[0] == "5undecies"
    assert b.parse_article(_article("art_5duodecies", "Art. 5duodecies"))[0] == "5duodecies"


def test_unrecognised_ordinal_kept_raw_not_truncated():
    # The point of the fix is the failure mode, not the lookup table. An ordinal
    # we have never seen must survive intact so it stays findable; the old regex
    # would have yielded "322t", and a lookahead that allowed \d+ to backtrack
    # would have yielded "32".
    num, *_ = b.parse_article(_article("art_322tredecies", "Art. 322tredecies"))
    assert num == "322tredecies"
    assert num not in ("322t", "32", "322")


def test_known_ordinals_still_parse():
    for suffix in ("bis", "ter", "quater", "quinquies", "sexies",
                   "septies", "octies", "novies"):
        num, *_ = b.parse_article(
            _article(f"art_322{suffix}", f"Art. 322{suffix}")
        )
        assert num == f"322{suffix}"


def test_footnote_text_after_number_still_stripped():
    # The regex earns its keep by cutting trailing footnote prose; that must
    # survive the anchoring change.
    num, *_ = b.parse_article(
        _article("art_5_a", "Art. 5 a Angenommen in der Volksabstimmung")
    )
    assert num == "5a"

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

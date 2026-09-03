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
    # The real BV Art. 5a structure: the suffix is an <i>, the footnote prose
    # is an <authorialNote> inside <num>. The number must come out clean and
    # the prose must not leak into it.
    num, *_ = b.parse_article(
        _article("art_5_a", "Art. 5<i>a</i><authorialNote><p>Angenommen in der "
                            "Volksabstimmung vom 9. Feb. 2014.</p></authorialNote>")
    )
    assert num == "5a"


# ── Defect H (2026-09-03 review): range eIds ─────────────────────────────────
# Commit 942c56f6's #32 repair searched `art_(\w+)` anywhere in the eId and
# concatenated the pieces, so range articles ("Art. 135–149", eId
# art_135_149) were stored as "135149", and "disp_u12/art_2_4" became "24",
# polluting OR Art. 24 with a second block. The repair now applies only when
# the eId names a single article.


def test_range_eid_keeps_first_number():
    num, *_ = b.parse_article(_article("art_135_149", "Art. 135–149"))
    assert num == "135"


def test_transitional_range_eid_keeps_first_number():
    num, *_ = b.parse_article(_article("disp_u12/art_2_4", "Art. 2–4"))
    assert num == "2"


def test_split_bold_number_in_transitional_block_still_repaired():
    num, *_ = b.parse_article(_article("disp_u2/art_451", "<b>Art. 45</b><b>1</b>"))
    assert num == "451"


def test_split_bold_number_repaired_from_single_article_eid():
    num, *_ = b.parse_article(_article("art_451", "<b>Art. 45</b><b>1</b>"))
    assert num == "451"


def test_correct_num_not_overridden_by_disagreeing_eid():
    # OR fr: eId art_221 carries <num>Art. 220</num>. 221 does not extend
    # 220, so the <num> wins.
    num, *_ = b.parse_article(_article("art_221", "Art. 220"))
    assert num == "220"


def test_italian_range_conjunction_not_read_as_suffix():
    # "Art. 135 a 149": the "a" is "to", not a letter suffix. Was "135a".
    num, *_ = b.parse_article(_article("art_135_149", "Art. 135 a 149"))
    assert num == "135"
    num, *_ = b.parse_article(_article("art_50_51", "Art. 50 e 51"))
    assert num == "50"


def test_french_range_keeps_first_number():
    num, *_ = b.parse_article(_article("art_135_149", "Art. 135 à 149"))
    assert num == "135"


def test_german_range_with_suffixes_keeps_first_number():
    num, *_ = b.parse_article(_article("art_663_a_663_b", "Art. 663<i>a</i> und 663<i>b</i>"))
    assert num == "663a"


def test_multi_article_num_first_token_rejected_by_regex_uses_first_article():
    # StGB fr: "<b>Art. 355</b><i>f</i>et <b>355</b><i>g</i>" joins to
    # "355fet 355g"; the strict regex refuses "355fe..." so fall back to the
    # first \d+[a-z]? token.
    num, *_ = b.parse_article(_article("art_355_f_355_g", "<b>Art. 355</b><i>f</i>et <b>355</b><i>g</i>"))
    assert num == "355f"


def test_missing_num_filled_from_single_article_eid_only():
    assert b.parse_article(_article("art_38_a", ""))[0] == "38a"
    assert b.parse_article(_article("disp_u1/art_7", ""))[0] == "7"
    assert b.parse_article(_article("art_135_149", ""))[0] == ""


def test_eid_article_num_helper():
    assert b._eid_article_num("art_41") == "41"
    assert b._eid_article_num("art_38_a") == "38a"
    assert b._eid_article_num("art_268_a_bis") == "268abis"
    assert b._eid_article_num("art_179_decies") == "179decies"
    # Fedlex separates every suffix with "_"; a glued suffix is not an eId shape
    assert b._eid_article_num("art_322decies") is None
    assert b._eid_article_num("disp_u2/art_1") == "1"
    assert b._eid_article_num("art_135_149") is None
    assert b._eid_article_num("art_663_a_663_b") is None
    assert b._eid_article_num("disp_u12/art_2_4") is None
    assert b._eid_article_num("") is None


def test_article_section_helper():
    assert b.article_section(_article("art_1", "Art. 1")) == ""
    assert b.article_section(_article("disp_u2/art_1", "Art. 1")) == "disp_u2"


def test_article_num_never_contains_whitespace():
    for eid, inner in [
        ("art_264_a", "<b>Art</b><b>. 264</b><i>a</i>"),
        ("art_663_b_bis", "<b>Art</b><b>.\u00a0663</b><i>b</i><sup>bis</sup>"),
        ("art_135_149", "Art. 135 a 149"),
        ("art_16", "<b>Art. 16</b><b>8</b>"),
    ]:
        num, *_ = b.parse_article(_article(eid, inner))
        assert " " not in num and num, (eid, num)

"""A stray "<" in OCR'd text must not delete the rest of the decision.

Found 2026-08-25 while diagnosing an unrelated drift alert. `_clean_text`
strips HTML with `_HTML_TAG_RE`, which was `<[^>]+>`. `[^>]` matches
newlines, so in scanned historical judgments a single OCR artefact — a
character misread as "<" — swallowed everything up to the next ">",
across thousands of characters and many lines, and left one space.

Measured against the source shards before the fix (real ids, real numbers):

    bge_historical_14_I_192   12,159 -> 2,317 chars served   80.9% lost
    bge_historical_11_I_5     13,081 -> 6,585                49.7% lost
    bge_historical_15_I_79    13,004 -> 8,645                33.5% lost

Each file held 1-6 stray "<" and no real HTML whatsoever. In force since
2026-02-22, and _clean_text runs over full_text, regeste AND title — so
official headnotes were being truncated too, which is what the verbatim
quotation contract (R2/R3) rests on.

These tests use the actual OCR fragments recovered from the corpus.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from build_fts5 import _clean_text  # noqa: E402

# Verbatim from bge_historical_15_I_79 — "1er" scanned as "1<r".
_OCR_FRAGMENT = "1<r Il s' agit dans l' espeee ineontestablement"
# Verbatim from bge_historical_14_I_192 — Fraktur noise.
_OCR_FRAGMENT_2 = "52 un.lereinbar ift, bug ange<"


def test_stray_angle_bracket_does_not_eat_the_decision():
    body = "\n".join(f"Erwägung {i}. Der Beschwerdeführer macht geltend."
                     for i in range(1, 60))
    text = f"{_OCR_FRAGMENT}\n{body}\nDemnach erkennt das Bundesgericht:"
    out = _clean_text(text)
    assert "Demnach erkennt das Bundesgericht" in out, \
        "the operative part was deleted by a single stray '<'"
    assert "Erwägung 59" in out
    # Allow for whitespace normalisation, not for wholesale deletion.
    assert len(out) > 0.9 * len(text)


def test_second_ocr_fragment_also_survives():
    text = f"{_OCR_FRAGMENT_2}\n" + ("Sachverhalt und Erwägungen. " * 300)
    out = _clean_text(text)
    assert len(out) > 0.9 * len(text)


def test_two_stray_brackets_far_apart_do_not_span():
    """The pathological case: an opening artefact on one line and another
    thousands of characters later. The old regex bridged them."""
    filler = "Der angefochtene Entscheid verletzt Bundesrecht. " * 200
    text = f"a<b\n{filler}\nc<d\nSchlussfolgerung."
    out = _clean_text(text)
    assert "Schlussfolgerung" in out
    assert filler.split(".")[0] in out


def test_real_html_tags_are_still_stripped():
    out = _clean_text("Art. 5 <b>OR</b> und <span class='x'>ZGB</span>.")
    assert "<b>" not in out and "</span>" not in out
    assert "OR" in out and "ZGB" in out


def test_multiline_tag_is_left_alone_rather_than_eating_the_body():
    """A tag broken across lines is not valid HTML in this corpus and is
    far likelier to be OCR noise. Leaving a stray '<' in the text is a
    cosmetic flaw; deleting the judgment is not."""
    text = "<div\nclass='x'>\nDas Bundesgericht zieht in Erwägung:\nEnde."
    out = _clean_text(text)
    assert "Das Bundesgericht zieht in Erwägung" in out
    assert "Ende." in out


def test_regeste_and_title_paths_are_covered_by_the_same_function():
    """_clean_text is applied to full_text, regeste and title alike, so the
    headnote is protected by the same fix."""
    regeste = f"Regeste\nArt. 60 OR; Verjährung. {_OCR_FRAGMENT}\n" + \
              "Die Klage ist verjährt. " * 40
    out = _clean_text(regeste)
    assert "Die Klage ist verjährt" in out
    assert "Art. 60 OR" in out


def test_empty_and_none_unchanged():
    assert _clean_text(None) is None
    assert _clean_text("") == ""

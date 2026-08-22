"""GitHub #82 (reproduction B): the grounding audit was handed claim spans that
were citation fragments, e.g. "September 2014, E. 5.1;", so a citation was
judged against text that is not a proposition.

Cause: the sentence-boundary rule keys on punctuation followed by a capital.
That already protects "Art. 28b" and "E. 3.4", because a digit follows the
period. It does not protect a date. German month names are always capitalised,
so "23. September 2014" is indistinguishable from a sentence end under that
rule, and the claim extractor started the claim mid-date.

Measured before/after on the same draft:
    before: 'September 2014 im Urteil'
    after:  'Das Bundesgericht bestätigte dies am 23. September 2014 im Urteil'
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402


def _n(text: str) -> int:
    return len(m._SENTENCE_END.findall(text))


# ── dates must not split ────────────────────────────────────────────────────

def test_german_date_is_not_a_sentence_boundary():
    assert _n("Das Urteil erging am 23. September 2014 und ist rechtskräftig.") == 0


def test_every_german_month_is_covered():
    months = ("Januar", "Februar", "März", "April", "Mai", "Juni", "Juli",
              "August", "September", "Oktober", "November", "Dezember")
    for mo in months:
        assert _n(f"Der Entscheid vom 3. {mo} 2020 ist rechtskräftig.") == 0, mo


def test_capitalised_french_and_italian_months_covered():
    assert _n("L'arrêt du 23. Septembre 2014 est définitif.") == 0
    assert _n("La sentenza del 23. Settembre 2014 è definitiva.") == 0


def test_lowercase_month_was_never_affected():
    # A lowercase month never reached the rule, which requires a capital.
    assert _n("L'arrêt du 23. septembre 2014 est définitif.") == 0


# ── real boundaries must survive ────────────────────────────────────────────

def test_ordinary_sentence_boundary_still_found():
    assert _n("Der Vertrag ist nichtig. Der Grund liegt im Inhalt.") == 1


def test_date_followed_by_a_real_sentence_end():
    assert _n("Der Entscheid datiert vom 1. Mai 2020. Er ist endgültig.") == 1


def test_article_and_erwaegung_suffixes_still_protected():
    assert _n("Nach Art. 28b ZGB gilt Folgendes.") == 0
    assert _n("Siehe E. 3.4 hiervor.") == 0


def test_paragraph_break_still_a_boundary():
    assert _n("Erster Absatz.\n\nZweiter Absatz.") >= 1


# ── the deliberate trade-off, recorded rather than hidden ───────────────────

def test_sentence_ending_in_a_year_before_a_month_no_longer_splits():
    # The one behaviour this change gives up: "Es war 2019. September war kalt."
    # used to split and no longer does, because the rule cannot tell that day
    # number from a year. In legal drafting a sentence starting with a bare
    # capitalised month is rare, while dates are everywhere, so the trade is
    # heavily favourable — but it is a real loss and belongs in a test.
    assert _n("Es war 2019. September war kalt.") == 0


# ── end to end on the reported draft ────────────────────────────────────────

def test_claim_extraction_no_longer_starts_mid_date():
    draft = ("Eine Bestechungszusage ist widerrechtlich und nichtig. "
             "Das Bundesgericht bestätigte dies am 23. September 2014 im Urteil "
             "(BGE 119 II 380 E. 4b).")
    claim = m._extract_preceding_claim(draft, draft.index("(BGE"))
    assert claim is not None
    assert claim.startswith("Das Bundesgericht bestätigte")
    assert not claim.startswith("September")

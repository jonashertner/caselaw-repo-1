"""Direct-portal cantonal formats (ne/ti/so) — markers the es-era copies
carried but the direct texts express differently (letter-spacing, glued
words from PDF extraction). Cutover-blocker fix 2026-07-03."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from search_stack.extract_decision_structure import extract  # noqa: E402


def test_ne_letter_spaced_considerant():
    t = ("Réf. : ARAN.2003.1 A. Sanction disciplinaire. B. Recours. "
         "C O N S I D E R A N T en droit 1. Déposé en les formes et délai "
         "prévus par la LPJA, le recours est recevable. 2. Sur le fond, il "
         "est rejeté pour les motifs suivants. " + "x " * 300)
    s = extract(t, "fr", "ne_test")
    assert s.erwaegungen_method == "ranked_fr_ne_spaced"
    assert s.erwaegungen_paragraphs


def test_ne_plain_considerant_not_matched_by_spaced_rule():
    t = ("Faits: la cour, considérant que le recours est recevable, statue. "
         "Considérant en droit 1. Le recours est recevable. " + "x " * 300)
    s = extract(t, "fr", "ne_test2")
    assert s.erwaegungen_method != "ranked_fr_ne_spaced"


def test_ti_glued_considerato_in_diritto():
    t = ("Incarto n.91.2011.199 Sentenza. Ritenuto in fatto: i fatti. "
         "e consideratoin diritto: che secondo l'art. 23 n. 1 la condotta "
         "è punibile. 1. Il ricorso è respinto. " + "x " * 300)
    s = extract(t, "it", "ti_test")
    assert s.erwaegungen_method == "ranked_it_considerato_glued"


def test_so_glued_in_erwaegung():
    t = ("BKBES.2016.129 betreffend Nichteintreten auf Einsprache zieht die "
         "Beschwerdekammer des Obergerichts inErwägung: 1. Die Beschwerde "
         "ist zulässig. 2. Sie ist unbegründet. " + "x " * 300)
    s = extract(t, "de", "so_test")
    assert s.erwaegungen_method in ("ranked_de_zieht_cantonal",
                                    "ranked_de_inerwaegung_colon")
    assert s.erwaegungen_paragraphs

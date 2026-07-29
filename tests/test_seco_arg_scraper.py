"""SECO Wegleitungen zum Arbeitsgesetz — parser tests against golden HTML.

Offline (invariant #8): fixtures captured from the live index pages
2026-07-29, no network in the test path.

This closes the biggest single gap in the Verwaltungspraxis corpus for
employment-law practice: SECO's article-by-article commentary on the ArG
and ArGV 1-5, ~368 documents per language.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.practice.seco_arg import SecoArgScraper  # noqa: E402

FIX = REPO / "tests" / "fixtures" / "practice"


def _parse(fixture: str, lang: str, erlass: str = "ArG"):
    """Parse without __init__ so no output dir / session is created."""
    s = SecoArgScraper.__new__(SecoArgScraper)
    html = (FIX / fixture).read_text(encoding="utf-8")
    return list(SecoArgScraper._parse_index(
        s, html, f"https://www.seco.admin.ch/{lang}/x", lang, erlass))


def test_de_index_yields_every_article():
    stubs = _parse("seco_arg_de_arbeitsgesetz.html", "de")
    # ArG has 71 articles incl. sub-articles (3a, 6a, ...)
    assert len(stubs) >= 70, len(stubs)
    nums = {s["doc_number"] for s in stubs}
    assert "ArG Art. 1" in nums
    assert "ArG Art. 3a" in nums          # sub-article letters survive
    assert "ArG Art. 2" in nums


def test_every_record_is_citable():
    """A practice record without a number or a date cannot be cited; we
    treat those as defects, not as acceptable output."""
    for lang, fx in (("de", "seco_arg_de_arbeitsgesetz.html"),
                     ("fr", "seco_arg_fr_loi.html")):
        for s in _parse(fx, lang):
            assert s["doc_number"], s
            assert s["date"], s
            assert s["pdf_url"].startswith("https://www.seco.admin.ch/dam/"), s
            assert s["language"] == lang


def test_localized_dates_parse_in_both_languages():
    de = {s["doc_number"]: s["date"] for s in _parse("seco_arg_de_arbeitsgesetz.html", "de")}
    fr = {s["doc_number"]: s["date"] for s in _parse("seco_arg_fr_loi.html", "fr")}
    # "18. November 2025" and "18 novembre 2025" are the same revision
    assert de["ArG Art. 2"] == "2025-11-18"
    assert fr["ArG Art. 2"] == "2025-11-18"
    # a much older article keeps its own date rather than inheriting
    assert de["ArG Art. 1"] == "2012-02-03"


def test_cross_language_dam_hash_is_shared():
    """The DAM hash is identical across languages — that is what makes the
    three language versions of an article linkable."""
    de = {s["doc_number"]: s["pdf_url"] for s in _parse("seco_arg_de_arbeitsgesetz.html", "de")}
    fr = {s["doc_number"]: s["pdf_url"] for s in _parse("seco_arg_fr_loi.html", "fr")}
    h_de = de["ArG Art. 2"].split("/sd-web/")[1].split("/")[0]
    h_fr = fr["ArG Art. 2"].split("/sd-web/")[1].split("/")[0]
    assert h_de == h_fr, (h_de, h_fr)
    assert "/dam/de/" in de["ArG Art. 2"] and "/dam/fr/" in fr["ArG Art. 2"]


def test_titles_carry_no_pdf_size_noise():
    for s in _parse("seco_arg_de_arbeitsgesetz.html", "de"):
        t = s["title"]
        assert "PDF" not in t.upper().split(), t
        assert "kB" not in t and "MB" not in t, t
        assert not t.endswith(":"), t


def test_doc_id_is_language_scoped():
    s = SecoArgScraper.__new__(SecoArgScraper)
    de = SecoArgScraper._make_doc_id(s, {"doc_number": "ArG Art. 2", "language": "de"})
    fr = SecoArgScraper._make_doc_id(s, {"doc_number": "ArG Art. 2", "language": "fr"})
    assert de != fr
    assert de.startswith("seco_arg_") and de.endswith("_de")


def test_topics_carry_erlass_and_sr_number():
    stubs = _parse("seco_arg_de_arbeitsgesetz.html", "de")
    assert "ArG" in stubs[0]["topics"]
    assert "SR 822.11" in stubs[0]["topics"]


def test_reissue_detection_is_enabled():
    """SECO revises articles in place under a new DAM hash while our doc_id
    stays stable — without REVISION_FIELD the corpus would freeze at the
    first edition ever fetched."""
    assert SecoArgScraper.REVISION_FIELD == "pdf_url"


def test_gesamtdokumente_doc_ids_do_not_collide():
    """Caught in the first live run: 'Wegleitung zum Arbeitsgesetz und den
    Verordnungen 1 und 2' and '... 3 und 4' share their first 40 characters,
    so a title-derived id truncated to the SAME slug and the second document
    silently overwrote the first. Keyed on the PDF filename stem instead."""
    import collections
    s = SecoArgScraper.__new__(SecoArgScraper)
    stubs = _parse("seco_arg_de_wegleitungen.html", "de", erlass="Gesamtdokumente")
    assert len(stubs) >= 6, len(stubs)
    ids = [SecoArgScraper._make_doc_id(s, st) for st in stubs]
    dupes = [k for k, v in collections.Counter(ids).items() if v > 1]
    assert not dupes, dupes
    # and the ids stay human-readable
    assert any(i.endswith("argv_1_2_de") for i in ids), ids


def test_no_doc_id_collisions_on_any_fixture_page():
    import collections
    s = SecoArgScraper.__new__(SecoArgScraper)
    for fx, lang, erlass in (
        ("seco_arg_de_arbeitsgesetz.html", "de", "ArG"),
        ("seco_arg_fr_loi.html", "fr", "ArG"),
        ("seco_arg_de_wegleitungen.html", "de", "Gesamtdokumente"),
    ):
        ids = [SecoArgScraper._make_doc_id(s, st) for st in _parse(fx, lang, erlass)]
        dupes = [k for k, v in collections.Counter(ids).items() if v > 1]
        assert not dupes, (fx, dupes)

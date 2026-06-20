"""Offline unit tests for the (consolidated) ESTV Kreisschreiben practice scraper.

Issue #16 follow-up: the practice-pipeline ESTV scraper is the single home for
ESTV Kreisschreiben (the decisions-pipeline duplicate from PR #26 was retired).
It now covers all three tax types (DBST/VST/STA) in DE/FR/IT — every PDF uses
the ``dbst-ks-`` filename prefix regardless of tax type, with the applicable
tax(es) encoded in the suffix letters (d/v/s).
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.practice.estv_kreisschreiben import (  # noqa: E402
    INDEX_SOURCES,
    EstvKreisschreibenScraper,
    _topics_from_filename,
)


def test_index_sources_cover_three_taxes_three_languages():
    taxes = {s["tax_type"] for s in INDEX_SOURCES}
    langs = {s["lang"] for s in INDEX_SOURCES}
    assert taxes == {"DBST", "VST", "STA"}
    assert langs == {"de", "fr", "it"}
    assert len(INDEX_SOURCES) == 9


def test_topics_from_filename_decodes_tax_suffix():
    # suffix 'dv' → Direkte Bundessteuer + Verrechnungssteuer
    assert _topics_from_filename("dbst-ks-2020-1-049-dv-de.pdf") == [
        "Direkte Bundessteuer", "Verrechnungssteuer"]
    # suffix 'dvs' → all three
    assert _topics_from_filename("dbst-ks-2013-1-039-dvs-de.pdf") == [
        "Direkte Bundessteuer", "Verrechnungssteuer", "Stempelabgaben"]
    # suffix 's' → Stempelabgaben only
    assert _topics_from_filename("dbst-ks-2011-1-033-s-de.pdf") == ["Stempelabgaben"]


def test_doc_id_is_unique_per_pdf():
    """doc_id keys on the PDF stem, so DE/FR/IT versions AND annexes of one KS
    stay distinct — a doc_number-based id would collapse (and drop) them."""
    s = EstvKreisschreibenScraper.__new__(EstvKreisschreibenScraper)
    s.SOURCE_KEY = "estv_ks"
    base = "https://www.estv.admin.ch/dam/de/sd-web/X/"
    de = s._make_doc_id({"pdf_url": base + "dbst-ks-2020-1-049-dv-de.pdf"})
    fr = s._make_doc_id({"pdf_url": base + "dbst-ks-2020-1-049-dv-fr.pdf"})
    anhang = s._make_doc_id({"pdf_url": base + "dbst-ks-2020-1-049-anhang1-dv-de.pdf"})
    assert de == "estv_ks_dbst_ks_2020_1_049_dv_de"
    assert len({de, fr, anhang}) == 3  # language + annex all distinct


_PAGE_HTML = """
<html><body><ul>
  <li><a href="/dam/de/sd-web/AAA/dbst-ks-2020-1-049-dv-de.pdf">KS Nr. 49 — Test Datum: 12.03.2020</a></li>
  <li><a href="/dam/de/sd-web/BBB/dbst-ks-2011-1-033-s-de.pdf">KS Nr. 33 — Stempel</a></li>
</ul></body></html>
"""


class _FakeResp:
    def __init__(self, text):
        self.text = text
        self.status_code = 200

    def raise_for_status(self):
        pass


def test_discover_yields_language_keyed_stubs_with_topics(monkeypatch):
    s = EstvKreisschreibenScraper.__new__(EstvKreisschreibenScraper)
    s.SOURCE_KEY = "estv_ks"
    s.ISSUING_AUTHORITY = "ESTV"
    s.DEFAULT_DOC_TYPE = "kreisschreiben"
    s.languages = ("de",)
    monkeypatch.setattr(s, "get", lambda url, **kw: _FakeResp(_PAGE_HTML))

    stubs = {st["doc_number"]: st for st in s.discover_documents()}
    assert "KS Nr. 49" in stubs
    ks49 = stubs["KS Nr. 49"]
    assert ks49["language"] == "de"
    assert ks49["pdf_url"].endswith("dbst-ks-2020-1-049-dv-de.pdf")
    assert ks49["topics"] == ["Direkte Bundessteuer", "Verrechnungssteuer"]
    assert ks49["date"] == "2020-03-12"
    assert s._make_doc_id(ks49) == "estv_ks_dbst_ks_2020_1_049_dv_de"

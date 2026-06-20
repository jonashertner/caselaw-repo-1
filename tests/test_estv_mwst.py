"""Offline unit tests for the ESTV MWST-Infos / Branchen-Infos scraper.

Issue #16. The source is a PrimeFaces/JSF app, but the read path is plain
static HTML: tableOfContent.xhtml lists publications, a per-publication
tableOfContent lists cipherDisplay leaves, and each cipherDisplay returns the
verbatim prose in #formular:cipherText. Language is the Accept-Language header,
not a URL param. These tests pin the three pure parsers + the publication-level
record model against captured-shape fixtures (no network).
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.practice.estv_mwst import (  # noqa: E402
    EstvMwstScraper,
    _leading_number,
    _parse_cipher,
    _parse_publication_ciphers,
    _parse_toc,
)


TOC_HTML = """
<html><body><ul>
  <li><a href="/mwst-webpublikationen/public/pages/taxInfos/tableOfContent.xhtml?publicationId=1000283">MWST-Info 05 Subventionen und Spenden</a></li>
  <li><a href="/mwst-webpublikationen/public/pages/taxInfos/tableOfContent.xhtml?publicationId=1010164">MWST-Info 02 Steuerpflicht</a></li>
  <!-- a navigation/dup link to the same publication must collapse -->
  <li><a href="tableOfContent.xhtml?publicationId=1000283">MWST-Info 05 Subventionen und Spenden</a></li>
  <li><a href="/some/unrelated/page.xhtml">Kontakt</a></li>
</ul></body></html>
"""

PUBLICATION_TOC_HTML = """
<html><body><ul>
  <li><a href="cipherDisplay.xhtml?publicationId=1000283&amp;componentId=2001">1 Einleitung</a></li>
  <li><a href="cipherDisplay.xhtml?publicationId=1000283&amp;componentId=2002">1.1 Betreiben eines Unternehmens</a></li>
  <li><a href="cipherDisplay.xhtml?publicationId=1000283&amp;componentId=2002">1.1 (duplicate nav link)</a></li>
  <li><a href="cipherDisplay.xhtml?publicationId=1000283&amp;componentId=2003">2 Subventionen</a></li>
</ul></body></html>
"""

CIPHER_HTML = """
<html><body>
  <span id="formular:cipherTitelLabel">1.1 Betreiben eines Unternehmens</span>
  <div id="formular:cipherText">
    Eine Subvention liegt vor, wenn das Gemeinwesen einem Empfaenger ohne
    entsprechende marktwirtschaftliche Gegenleistung Mittel ausrichtet, um ein
    bestimmtes Verhalten zu foerdern. Subventionen gelten nicht als Entgelt.
  </div>
  <div class="meta">Publiziert am 15.01.2021</div>
</body></html>
"""

CIPHER_HEADING_ONLY_HTML = """
<html><body>
  <span id="formular:cipherTitelLabel">2 Subventionen</span>
  <div id="formular:cipherText">  </div>
</body></html>
"""


# ── pure parsers ────────────────────────────────────────────────────────────

def test_parse_toc_extracts_publications_deduped_in_order():
    pubs = _parse_toc(TOC_HTML)
    assert pubs == [
        ("1000283", "MWST-Info 05 Subventionen und Spenden"),
        ("1010164", "MWST-Info 02 Steuerpflicht"),
    ]


def test_parse_publication_ciphers_dedups_preserves_order():
    comps = _parse_publication_ciphers(PUBLICATION_TOC_HTML)
    assert comps == ["2001", "2002", "2003"]


def test_parse_cipher_extracts_body_and_date():
    body, date = _parse_cipher(CIPHER_HTML)
    assert "Subvention liegt vor" in body
    assert "marktwirtschaftliche Gegenleistung" in body
    assert date == "2021-01-15"


def test_parse_cipher_heading_only_returns_empty_body():
    body, date = _parse_cipher(CIPHER_HEADING_ONLY_HTML)
    assert body.strip() == ""
    assert date == ""


def test_leading_number_from_publication_title():
    assert _leading_number("MWST-Info 05 Subventionen und Spenden") == "05"
    assert _leading_number("MWST-Branchen-Info 02 Gemeinwesen") == "02"
    assert _leading_number("Info TVA 21 Gestion immobiliere") == "21"
    assert _leading_number("Ohne Nummer") is None


# ── record model / discovery (offline) ──────────────────────────────────────

class _FakeResp:
    def __init__(self, text):
        self.text = text
        self.status_code = 200

    def raise_for_status(self):
        pass


def test_doc_id_is_language_keyed():
    s = EstvMwstScraper.__new__(EstvMwstScraper)
    s.SOURCE_KEY = "estv_mwst"
    stub = {"language": "fr", "publication_id": "1000283"}
    assert s._make_doc_id(stub) == "estv_mwst_fr_1000283"


def test_discover_documents_yields_publication_stubs(monkeypatch, tmp_path):
    s = EstvMwstScraper.__new__(EstvMwstScraper)
    s.SOURCE_KEY = "estv_mwst"
    s.ISSUING_AUTHORITY = "ESTV"
    s.DEFAULT_DOC_TYPE = "mwst_info"
    s.languages = ("de",)
    # only the taxInfos tree, to keep the fixture focused
    s.TREES = [("taxInfos", "mwst_info", "MWST-Info")]
    monkeypatch.setattr(s, "get", lambda url, **kw: _FakeResp(TOC_HTML))

    stubs = list(s.discover_documents())
    assert len(stubs) == 2
    first = stubs[0]
    assert first["publication_id"] == "1000283"
    assert first["language"] == "de"
    assert first["doc_type"] == "mwst_info"
    assert first["doc_number"] == "MWST-Info 05"
    assert first["title"] == "MWST-Info 05 Subventionen und Spenden"

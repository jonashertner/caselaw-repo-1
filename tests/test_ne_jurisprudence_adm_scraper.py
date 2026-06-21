"""Offline test for the NE administrative jurisprudence scraper (Omnis fork of ne_gerichte).

Golden fixture of the Omnis search_resulttable HTML (invariant #8). Asserts the total-count
parse, docket/nF30_KEY/date extraction from result rows, and that the doc URL uses the
NE_JURWEB/port-8000 config (NOT the courts NE_WEB/port-7000 config).
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.cantonal.ne_jurisprudence_adm import NEJurisprudenceAdmScraper  # noqa: E402

FIXTURE = """<html><body>
 <div class="resultheader">résultats: 1 - 2 de 1648 fiche(s) trouvée(s)</div>
 <table>
  <tr><td>15.01.2026</td>
      <td><a href="omnisapi.dll?Aufruf=getMarkupDocument&nF30_KEY=789&Schema=NE_JURWEB">REC.2025.42 Recours en matière de construction</a></td></tr>
  <tr><td>03.12.2025</td>
      <td><a href="omnisapi.dll?Aufruf=getMarkupDocument&nF30_KEY=790&Schema=NE_JURWEB">DECI.2025.7 Décision du DDTE</a></td></tr>
 </table>
</body></html>"""


def test_parse_total():
    s = NEJurisprudenceAdmScraper(state_dir=Path("/tmp"))
    assert s._parse_total("blah de 1648 fiche(s) trouvée(s)") == 1648


def test_discover(monkeypatch, tmp_path):
    s = NEJurisprudenceAdmScraper(state_dir=tmp_path)
    monkeypatch.setattr(s, "_init_session", lambda: None)

    class R:
        text = FIXTURE

    monkeypatch.setattr(s, "post", lambda url, **k: R())
    stubs = list(s.discover_new())

    assert s.portal_count == 1648
    assert len(stubs) == 2                      # total parsed=1648 but only 2 rows on page 1; no W10 key -> no paginate
    by = {x["docket_number"]: x for x in stubs}
    assert "REC.2025.42" in by and "DECI.2025.7" in by
    assert by["REC.2025.42"]["nf30_key"] == "789"
    assert by["REC.2025.42"]["decision_date"] == date(2026, 1, 15)
    # doc URL must target the ADMIN library, not the courts one
    url = by["REC.2025.42"]["url"]
    assert "Schema=NE_JURWEB" in url and "Parametername=NEJURWEB" in url and "JURISWEB,8000" in url
    assert "NE_WEB" not in url.replace("NE_JURWEB", "") and "7000" not in url


def test_docket_id_distinct_from_courts():
    # ne_jurisprudence_adm must not collide with ne_gerichte ids
    s = NEJurisprudenceAdmScraper(state_dir=Path("/tmp"))
    assert s.court_code == "ne_jurisprudence_adm"


def test_extract_labelled_avoids_megacell():
    # The NE-adm doc has a concatenated mega-cell AND clean label/value pairs; the EXACT
    # match must land on the real value, not grab the next label (the original bug).
    from bs4 import BeautifulSoup
    from scrapers.cantonal.ne_jurisprudence_adm import _extract_labelled
    DOC = """<table>
      <tr><td>Dossier: DECI.2018.85 Domaine: Eaux Autorité: DDTE Titre: Foo</td></tr>
      <tr><td>Autorité:</td><td>DDTE</td></tr>
      <tr><td>Domaine:</td><td>Economie des eaux</td></tr>
      <tr><td><b>Titre:</b></td><td>Demande de récusation</td></tr>
      <tr><td>Date décision/avis:</td><td></td></tr>
    </table>"""
    soup = BeautifulSoup(DOC, "html.parser")
    assert _extract_labelled(soup, "Autorité") == "DDTE"
    assert _extract_labelled(soup, "Domaine") == "Economie des eaux"
    assert _extract_labelled(soup, "Titre") == "Demande de récusation"
    assert _extract_labelled(soup, "Date décision") is None   # empty value -> None

"""Offline test for the BE directorate scraper (scrapers/cantonal/be_direktionen.py).

Golden fixture of the GSI Beschwerdeentscheide table (Datum | Nummer | Gegenstand),
invariant #8. Asserts docket/date/title/chamber extraction and that non-docket PDFs
(forms) are skipped.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.cantonal.be_direktionen import BEDirektionenScraper  # noqa: E402

GSI_FIX = """<html><body><table>
 <tr><th>Datum</th><th>Nummer</th><th>Gegenstand</th></tr>
 <tr><th>16.04.2026</th>
     <td><a href="/content/dam/gsi/.../beschwerdeentscheid-anonymisiert-2025gsi2700.pdf">2025.GSI.2700</a></td>
     <td>Sozialhilfe: Bemessung der wirtschaftlichen Asylsozialhilfe</td></tr>
 <tr><th>02.04.2026</th>
     <td><a href="/content/dam/gsi/.../beschwerdeentscheid-anonymisiert-2025gsi3131.pdf">2025.GSI.3131</a></td>
     <td>Krankenversicherung: Prämienverbilligung</td></tr>
 <tr><td><a href="/content/dam/gsi/.../merkblatt-sozialhilfe.pdf">Merkblatt (PDF)</a></td></tr>
</table></body></html>"""


def test_be_direktionen_discover(monkeypatch, tmp_path):
    s = BEDirektionenScraper(state_dir=tmp_path)

    def fake_get(url, **k):
        class R:
            text = GSI_FIX if "gsi.be.ch" in url else "<html><body></body></html>"
        return R()

    monkeypatch.setattr(s, "get", fake_get)
    stubs = list(s.discover_new())

    assert len(stubs) == 2                         # the Merkblatt form PDF (no docket) is skipped
    by = {x["docket_number"]: x for x in stubs}
    assert "2025.GSI.2700" in by
    st = by["2025.GSI.2700"]
    assert st["decision_date"] == "16.04.2026"
    assert st["chamber"] == "GSI"
    assert st["title"].startswith("Sozialhilfe")
    assert st["pdf_url"].startswith("https://www.gsi.be.ch/")


def test_be_direktionen_since_filter(monkeypatch, tmp_path):
    s = BEDirektionenScraper(state_dir=tmp_path)
    monkeypatch.setattr(s, "get", lambda url, **k: type("R", (), {"text": GSI_FIX if "gsi.be.ch" in url else "<html></html>"})())
    stubs = list(s.discover_new(since_date=date(2026, 4, 10)))
    # only the 16.04.2026 decision survives (02.04.2026 is filtered out)
    assert {x["docket_number"] for x in stubs} == {"2025.GSI.2700"}

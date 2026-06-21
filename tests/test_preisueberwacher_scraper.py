"""Offline test for the Preisüberwacher scraper (scrapers/preisueberwacher.py).

Golden fixture mixing the Price Supervisor's own Verfügung with an appellate court ruling
(invariant #8). Asserts the court ruling is filtered out (it lives in our corpus under its
own court) and only the own decision is yielded, with the date parsed from the link text.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.preisueberwacher import PreisueberwacherScraper  # noqa: E402

FIX = """<html><body>
 <a href="/dam/pue/de/dokumente/formelle_entscheide/Booking.com%20B.V._Verf%C3%BCgung%20vom%2020.%20Mai%202025.pdf.download.pdf/Booking.com%20B.V._Verf%C3%BCgung%20vom%2020.%20Mai%202025.pdf">20.05.2025 - Verfügung gegen Booking.com (PDF, 2 MB, 15 Seiten)</a>
 <a href="/dam/pue/de/dokumente/formelle_entscheide/10.11.2023%20-%20Bundesverwaltungsgerichtsurteil.pdf">10.11.2023 - Bundesverwaltungsgerichtsurteil betr. interkommunale Anstalt Limeco (PDF, 1 MB)</a>
</body></html>"""
EMPTY = "<html><body></body></html>"


def test_preisueberwacher_filters_court_rulings(monkeypatch, tmp_path):
    s = PreisueberwacherScraper(state_dir=tmp_path)

    def fake_get(url, **k):
        class R:
            text = FIX if "formelle-entscheide" in url else EMPTY
        return R()

    monkeypatch.setattr(s, "get", fake_get)
    stubs = list(s.discover_new())

    assert len(stubs) == 1                          # the BVGer ruling was filtered out
    st = stubs[0]
    assert "booking" in st["docket_number"].lower()
    assert st["decision_date"] == "20.05.2025"
    assert st["decision_type"] == "Verfügung"
    assert "Booking.com" in st["title"]

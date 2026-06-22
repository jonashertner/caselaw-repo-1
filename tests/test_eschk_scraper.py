"""Offline test for the ESchK scraper (scrapers/eschk.py).

Golden fixture of a /de/beschluesse-{year} page (admin.ch download-item, invariant #8).
Asserts docket = PDF filename stem, date parsed from the "(Beschluss vom ...)" title, and
that the since-filter narrows the year range so older year-pages are never fetched.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.eschk import ESchKScraper  # noqa: E402

FIX_2023 = """<html><body>
 <a class="download-item"
    href="https://www.eschk.admin.ch/dam/de/sd-web/HASH1/tarif-a-suisa-2024.pdf">
   <h4 class="download-item__title">Tarif A SUISA (Beschluss vom 3. November 2023)</h4>
   <p class="download-item__description">Sendungen der SRG SSR</p></a>
</body></html>"""
EMPTY = "<html><body></body></html>"


def _scraper(monkeypatch, tmp_path):
    s = ESchKScraper(state_dir=tmp_path)

    def fake_get(url, **k):
        class R:
            text = FIX_2023 if "beschluesse-2023" in url else EMPTY
        return R()

    monkeypatch.setattr(s, "get", fake_get)
    return s


def test_eschk_discover(monkeypatch, tmp_path):
    stubs = list(_scraper(monkeypatch, tmp_path).discover_new())
    assert len(stubs) == 1
    st = stubs[0]
    assert st["docket_number"] == "tarif-a-suisa-2024"
    assert st["decision_date"] == "3. November 2023"
    assert "Tarif A SUISA" in st["title"]


def test_eschk_since_narrows_year_range(monkeypatch, tmp_path):
    # since 2024 -> start_year=2024, so the 2023 page is never fetched
    stubs = list(_scraper(monkeypatch, tmp_path).discover_new(since_date=date(2024, 1, 1)))
    assert stubs == []


NO_DATE_FIX = """<html><body>
 <a class="download-item" href="https://www.eschk.admin.ch/dam/de/sd-web/HX/tarif-z-2022.pdf">
   <h4 class="download-item__title">Tarif Z (kein Datum im Titel)</h4></a>
</body></html>"""


def test_eschk_no_date_not_fabricated(monkeypatch, tmp_path):
    # regression: a title with no parseable date must yield decision_date "" — NOT a fabricated
    # "1. Januar {year}" (which looked real and corrupted date filtering/sorting).
    s = ESchKScraper(state_dir=tmp_path)

    def fake_get(url, **k):
        class R:
            text = NO_DATE_FIX if "beschluesse-2022" in url else "<html><body></body></html>"
        return R()

    monkeypatch.setattr(s, "get", fake_get)
    stubs = list(s.discover_new())
    assert len(stubs) == 1
    assert stubs[0]["decision_date"] == ""

"""Offline test for the BS recourse-commissions scraper (scrapers/cantonal/bs_rekurskommissionen.py).

Golden fixtures of the two Nuxt-SSR listing pages (invariant #8). Asserts one scraper emits
TWO distinct courts, docket from link text (STRK) or slug (PRK/RRB), date from PRK link text,
boilerplate stripped from titles, and RRB items flagged chamber=Regierungsrat.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.cantonal.bs_rekurskommissionen import BSRekurskommissionenScraper  # noqa: E402

STRK_FIX = """<html><body>
 <a href="https://media.bs.ch/original_file/abc123/strk-2020-171-unterhaltsabzuege.pdf">STRK.2020.171, Unterhaltsabzüge, Kinderabzüge Externer Link, wird in einem neuen Fenster geöffnet</a>
 <a href="https://media.bs.ch/original_file/def456/strk-2021-51-selbstaendig.pdf">STRK.2021.51, Selbständige Erwerbstätigkeit Externer Link</a>
</body></html>"""

PRK_FIX = """<html><body>
 <a href="https://media.bs.ch/original_file/ghi789/prk-101.pdf">Fall Nr. 101: Schriftlicher Verweis (29.10.2013) Externer Link, wird in einem neuen Fenster geöffnet</a>
 <a href="https://media.bs.ch/original_file/jkl012/rrb-64.pdf">Entscheid des Regierungsrates vom 17.02.2009 zum PRK-Fall Nr. 64 Externer Link</a>
</body></html>"""


def _scraper(monkeypatch, tmp_path):
    s = BSRekurskommissionenScraper(state_dir=tmp_path)

    def fake_get(url, **k):
        class R:
            text = STRK_FIX if "steuerrekurs" in url else PRK_FIX
        return R()

    monkeypatch.setattr(s, "get", fake_get)
    return s


def test_two_courts_and_dockets(monkeypatch, tmp_path):
    stubs = list(_scraper(monkeypatch, tmp_path).discover_new())
    assert len(stubs) == 4
    by = {x["docket_number"]: x for x in stubs}

    assert by["STRK.2020.171"]["court"] == "bs_steuerrekurskommission"
    assert by["STRK.2020.171"]["legal_area"] == "Steuerrecht"
    assert "Externer Link" not in (by["STRK.2020.171"]["title"] or "")
    assert "Unterhaltsabzüge" in by["STRK.2020.171"]["title"]
    assert by["STRK.2020.171"]["decision_date"] == ""    # STRK listing carries no date

    assert by["PRK-101"]["court"] == "bs_personalrekurskommission"
    assert by["PRK-101"]["decision_date"] == "29.10.2013"
    assert by["PRK-101"]["chamber"] is None

    assert by["RRB-64"]["court"] == "bs_personalrekurskommission"
    assert by["RRB-64"]["decision_date"] == "17.02.2009"
    assert by["RRB-64"]["chamber"] == "Regierungsrat"   # Regierungsrat appeal-instance, flagged


def test_court_code_is_storage_key(tmp_path):
    s = BSRekurskommissionenScraper(state_dir=tmp_path)
    assert s.court_code == "bs_rekurskommissionen"


def test_embedded_year_slug_not_matched(monkeypatch, tmp_path):
    # regression: a non-decision PDF whose slug merely *contains* a year-number must NOT be
    # ingested. The bare YYYY-NN docket form is only accepted as the WHOLE slug stem (fullmatch).
    FIX = ('<html><body><a href="https://media.bs.ch/original_file/zzz/'
           'merkblatt-prk-verfahren-2024-05.pdf">Merkblatt zum Verfahren Externer Link</a></body></html>')
    s = BSRekurskommissionenScraper(state_dir=tmp_path)

    def fake_get(url, **k):
        class R:
            text = FIX if "personalrekurs" in url else "<html><body></body></html>"
        return R()

    monkeypatch.setattr(s, "get", fake_get)
    assert list(s.discover_new()) == []


def test_prk_docket_normalised(monkeypatch, tmp_path):
    # a dotted PRK docket in the slug normalises to dash form
    FIX = ('<html><body><a href="https://media.bs.ch/original_file/zzz/prk-205.pdf">'
           'Fall Nr. 205 (01.02.2024) Externer Link</a></body></html>')
    s = BSRekurskommissionenScraper(state_dir=tmp_path)
    monkeypatch.setattr(s, "get", lambda url, **k: type("R", (), {
        "text": FIX if "personalrekurs" in url else "<html></html>"})())
    stubs = list(s.discover_new())
    assert [x["docket_number"] for x in stubs] == ["PRK-205"]

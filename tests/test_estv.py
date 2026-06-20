"""Offline unit tests for the ESTV Kreisschreiben scraper.

Covers the pure parsing helpers and the listing-page discovery logic against an
embedded HTML sample (no network), plus a regression test for the decision_id
collision that the multi-PDF-per-Kreisschreiben layout originally caused.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models import make_decision_id
from scrapers import estv
from scrapers.estv import (
    ESTVScraper,
    _date_from_text,
    _doc_type,
    _ks_number,
    _part_marker,
)


# ── pure helpers ────────────────────────────────────────────────────────────


def test_ks_number_from_title_with_suffix():
    assert (
        _ks_number("Kreisschreiben Nr. 50a: Unzulässigkeit", "x-050a-de.pdf") == "50a"
    )


def test_ks_number_falls_back_to_filename_for_non_german_titles():
    # French title has no "Nr." — must fall back to the (language-agnostic) filename.
    href = "https://www.estv.admin.ch/dam/fr/sd-web/H/dbst-ks-2019-1-045-d-fr.pdf"
    assert _ks_number("Circulaire n° 45: Imposition à la source", href) == "45"


def test_doc_type_classification():
    assert _doc_type("Merkblatt über die Quellensteuer") == "Merkblatt"
    assert _doc_type("Rundschreiben Zinssätze 2025") == "Rundschreiben"
    assert _doc_type("Wegleitung zur Verrechnungssteuer") == "Wegleitung"
    assert _doc_type("Kreisschreiben Nr. 50: Bestechungsgelder") == "Kreisschreiben"


def test_part_marker():
    assert (
        _part_marker("Kreisschreiben Nr. 45 Anhang I-1: Anmeldeformular")
        == "Anhang I-1"
    )
    assert _part_marker("Kreisschreiben Nr. 45: Fragen und Antworten") == "FAQ"
    assert _part_marker("Kreisschreiben Nr. 50: Bestechungsgelder") is None


def test_date_from_text_long_and_numeric_forms():
    assert _date_from_text("ESTV\nBern, 12. März 2025\nKreisschreiben") == date(
        2025, 3, 12
    )
    assert _date_from_text("Gültig ab 01.01.2024 für alle") == date(2024, 1, 1)
    assert _date_from_text("kein Datum hier") is None


# ── discovery (offline) ─────────────────────────────────────────────────────

_DAM = "https://www.estv.admin.ch/dam/de/sd-web/HASH"

SAMPLE_LISTING_HTML = f"""
<html><body>
  <ul>
    <li><a href="{_DAM}/dbst-ks-2020-1-050-d-de.pdf">
        Kreisschreiben Nr. 50: Unzulässiger Abzug PDF, 250 KB, 13.07.2020</a></li>
    <!-- same PDF linked a second time on the page -->
    <li><a href="{_DAM}/dbst-ks-2020-1-050-d-de.pdf">Kreisschreiben Nr. 50</a></li>
    <li><a href="{_DAM}/dbst-ks-2020-1-050-d-anhang1-1-de.pdf">
        Kreisschreiben Nr. 50 Anhang I-1: Formular PDF, 80 KB, 13.07.2020</a></li>
    <li><a href="{_DAM}/dbst-mb-2021-1-001-d-de.pdf">
        Merkblatt über die Quellensteuer PDF, 120 KB, 01.01.2021</a></li>
  </ul>
</body></html>
"""


class _FakeResponse:
    def __init__(self, text):
        self.text = text
        self.status_code = 200


def _discover(monkeypatch, tmp_path):
    monkeypatch.setattr(
        estv,
        "SOURCES",
        [
            {
                "url": "https://example/de",
                "tax_type": "DBST",
                "legal_area": "Direkte Bundessteuer",
                "lang": "de",
            }
        ],
    )
    scraper = ESTVScraper(state_dir=tmp_path)
    monkeypatch.setattr(
        scraper, "get", lambda url, **kw: _FakeResponse(SAMPLE_LISTING_HTML)
    )
    return list(scraper.discover_new())


def test_discover_dedups_repeated_links(monkeypatch, tmp_path):
    stubs = _discover(monkeypatch, tmp_path)
    # 4 anchors, but the duplicated main KS link collapses to 3 documents.
    assert len(stubs) == 3
    stems = [s["stem"] for s in stubs]
    assert len(set(stems)) == 3


def test_discover_ids_are_collision_free_across_annexes(monkeypatch, tmp_path):
    # Regression: main KS and its Anhang share "Nr. 50" but must get distinct ids.
    stubs = _discover(monkeypatch, tmp_path)
    ids = [make_decision_id("estv", s["stem"]) for s in stubs]
    assert len(set(ids)) == len(ids)


def test_discover_metadata_fields(monkeypatch, tmp_path):
    stubs = {s["docket_number"]: s for s in _discover(monkeypatch, tmp_path)}

    main = stubs["KS DBST Nr. 50"]
    assert main["decision_type"] == "Kreisschreiben"
    assert main["decision_date"] == "13.07.2020"
    assert main["lang"] == "de"

    assert stubs["KS DBST Nr. 50 (Anhang I-1)"]["decision_type"] == "Kreisschreiben"
    # Merkblatt has no KS number -> docket has no "Nr.", type is derived from title.
    merkblatt = next(s for s in stubs.values() if s["decision_type"] == "Merkblatt")
    assert "Merkblatt" in merkblatt["title"]

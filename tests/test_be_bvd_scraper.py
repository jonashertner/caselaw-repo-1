"""be_bvd direct scraper — registration + config (offline).

Drafted 2026-06-16 for the entscheidsuche-independence program (workflow
w7olj6loq): es_be_bvd is an orphan-live court (live upstream, no covering
sibling). The GWT-RPC parsing is inherited from TribunaBaseScraper (covered by
the sibling Tribuna tests); this asserts the be_bvd-specific config and keeps
the zero-result scraper parked until a productive upstream is validated.

NOTE: a LIVE validation run (`python3 run_scraper.py be_bvd --max 3 -v`, gated)
is still REQUIRED before the scraper is trusted — the registry previously
recorded BVD as 'DB disconnected' at /tribunapublikation, and the 2026-06-16
probe found it live at /tribunavtplus. The run resolves that conflict + the
SEARCH_FIELD_COUNT 21-vs-20 detail.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def test_be_bvd_not_registered_until_productive():
    from run_scraper import SCRAPERS
    # The protocol implementation is intentionally parked: the live portal
    # currently returns zero rows. Keep it out of production dispatch until a
    # productive source/filter is validated (see the module status note).
    assert "be_bvd" not in SCRAPERS


def test_be_bvd_imports_and_subclasses_tribuna():
    from scrapers.cantonal.be_bvd import BEBvdScraper
    from scrapers.cantonal.base_tribuna import TribunaBaseScraper
    assert issubclass(BEBvdScraper, TribunaBaseScraper)


def test_be_bvd_config():
    from scrapers.cantonal.be_bvd import BEBvdScraper as s
    assert s.CANTON == "BE"
    assert s.COURT_CODE_STR == "be_bvd"
    assert s.BASE_URL == "https://www.bvd-entscheide.apps.be.ch"
    assert s.TRIBUNA_PATH == "tribunavtplus"
    assert s.COURT_FILTER == "BVD"
    assert s.LOCALE == "de"
    # VERIFY_SSL was removed 2026-08-26: bvd-entscheide.apps.be.ch verifies
    # cleanly against the scraper CA bundle, and the opt-out had silently
    # stopped working once REQUESTS_CA_BUNDLE shipped. Assert it stays gone —
    # reinstating it would genuinely disable verification now that
    # BaseScraper.get/post honour it.
    assert getattr(s, "VERIFY_SSL", True) is not False
    # service URL composes as BASE_URL/TRIBUNA_PATH (base_tribuna convention)
    assert f"{s.BASE_URL}/{s.TRIBUNA_PATH}" == "https://www.bvd-entscheide.apps.be.ch/tribunavtplus"

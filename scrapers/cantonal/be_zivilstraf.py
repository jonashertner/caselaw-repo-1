"""
Bern Zivil- und Strafgerichte Scraper
=======================================
Scrapes civil and criminal court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (old protocol, 46-param search)
Coverage: Zivil- und Strafgerichte des Kantons Bern
Volume: ~5,637 decisions (OG=5,632, BM=2, WSG=3)
Language: de/fr

Source: https://www.zsg-entscheide.apps.be.ch/tribunapublikation
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class BEZivilStrafScraper(TribunaBaseScraper):
    CANTON = "BE"
    COURT_CODE_STR = "be_zivilstraf"
    BASE_URL = "https://www.zsg-entscheide.apps.be.ch/tribunapublikation"
    COURT_FILTERS = ["OG", "BM", "BJS", "EO", "O", "WSG"]
    LOCALE = "de"
    REQUEST_DELAY = 2.5
    VERIFY_SSL = False
    SEARCH_FIELD_COUNT = 20  # Old Tribuna version (46-param search)

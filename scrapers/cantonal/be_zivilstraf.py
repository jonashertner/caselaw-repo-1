"""
Bern Zivil- und Strafgerichte Scraper
=======================================
Scrapes civil and criminal court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Zivil- und Strafgerichte des Kantons Bern
Volume: ~6,000 decisions
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

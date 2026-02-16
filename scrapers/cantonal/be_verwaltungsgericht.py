"""
Bern Verwaltungsgericht Scraper
================================
Scrapes administrative court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Verwaltungsgericht des Kantons Bern
Volume: ~18,000 decisions
Language: de/fr

Source: https://www.vg-urteile.apps.be.ch/tribunapublikation
NOTE: Server runs a newer Tribuna version (Feb 2026) where the search() method
was replaced. Needs protocol update to use saveSearch + getInitialSearch.
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class BEVerwaltungsgerichtScraper(TribunaBaseScraper):
    CANTON = "BE"
    COURT_CODE_STR = "be_verwaltungsgericht"
    BASE_URL = "https://www.vg-urteile.apps.be.ch/tribunapublikation"
    COURT_FILTER = "VG"  # Verwaltungsgericht
    LOCALE = "de"
    REQUEST_DELAY = 4.0  # Increased from 2.5 to avoid 503 rate limit
    VERIFY_SSL = False  # SSL verification issues
    SEARCH_FIELD_COUNT = 21  # New Tribuna version (47-param search)

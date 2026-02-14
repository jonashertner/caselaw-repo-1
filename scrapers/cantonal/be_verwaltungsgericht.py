"""
Bern Verwaltungsgericht Scraper
================================
Scrapes administrative court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Verwaltungsgericht des Kantons Bern
Volume: ~18,000 decisions
Language: de/fr

Source: https://www.vg-urteile.apps.be.ch/tribunapublikation
NOTE: Server was returning 503 as of Feb 2026 — may be temporarily down.
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class BEVerwaltungsgerichtScraper(TribunaBaseScraper):
    CANTON = "BE"
    COURT_CODE_STR = "be_verwaltungsgericht"
    BASE_URL = "https://www.vg-urteile.apps.be.ch/tribunapublikation"
    COURT_FILTER = ""
    LOCALE = "de"
    REQUEST_DELAY = 2.5

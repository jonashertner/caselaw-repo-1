"""
Zug Courts Scraper (ZG Gerichte)
=================================
Scrapes court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Verwaltungsgericht Zug
Volume: ~3,200 decisions
Language: de

Source: https://verwaltungsgericht.zg.ch
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class ZGVerwaltungsgerichtScraper(TribunaBaseScraper):
    CANTON = "ZG"
    COURT_CODE_STR = "zg_verwaltungsgericht"
    BASE_URL = "https://verwaltungsgericht.zg.ch"
    COURT_FILTER = "TRI"  # "TRI (Publikation Verwaltungsgericht)"
    LOCALE = "de"
    REQUEST_DELAY = 2.5

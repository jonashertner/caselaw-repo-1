"""
Schwyz Verwaltungsgericht Scraper (SZ VG)
==========================================
Scrapes court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Verwaltungsgericht Schwyz
Volume: ~2,000 decisions
Language: de

Source: https://gerichte.sz.ch/vg/
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class SZVerwaltungsgerichtScraper(TribunaBaseScraper):
    CANTON = "SZ"
    COURT_CODE_STR = "sz_verwaltungsgericht"
    BASE_URL = "https://gerichte.sz.ch/vg"
    COURT_FILTER = ""  # Empty = all courts
    LOCALE = "de"
    REQUEST_DELAY = 2.5

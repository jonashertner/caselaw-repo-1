"""
Schwyz Courts Scraper (SZ Gerichte)
====================================
Scrapes court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Kantonsgericht Schwyz
Volume: ~3,200 decisions (2017-present)
Language: de

Source: https://gerichte.sz.ch/kg/
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class SZGerichteScraper(TribunaBaseScraper):
    CANTON = "SZ"
    COURT_CODE_STR = "sz_gerichte"
    BASE_URL = "https://gerichte.sz.ch/kg"
    COURT_FILTER = ""  # Empty = all courts
    LOCALE = "de"
    REQUEST_DELAY = 2.5

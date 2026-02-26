"""
Zug Obergericht Scraper (ZG OG)
================================
Scrapes court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC
Coverage: Obergericht Zug
Volume: ~830+ decisions
Language: de

Source: https://obergericht.zg.ch
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class ZGObergerichtScraper(TribunaBaseScraper):
    CANTON = "ZG"
    COURT_CODE_STR = "zg_obergericht"
    BASE_URL = "https://obergericht.zg.ch"
    COURT_FILTER = "TRI"
    LOCALE = "de"
    REQUEST_DELAY = 2.5

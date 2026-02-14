"""
Jura Courts Scraper (JU Gerichte)
==================================
Scrapes court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Tribunal cantonal du Jura
Volume: ~1,050 decisions (2011-present)
Language: fr

Source: https://jurisprudence.jura.ch
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class JUGerichteScraper(TribunaBaseScraper):
    CANTON = "JU"
    COURT_CODE_STR = "ju_gerichte"
    BASE_URL = "https://jurisprudence.jura.ch"
    COURT_FILTERS = ["TC", "TPI"]
    LOCALE = "fr"
    REQUEST_DELAY = 2.5

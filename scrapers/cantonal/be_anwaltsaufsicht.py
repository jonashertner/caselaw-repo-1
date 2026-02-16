"""
Bern Anwaltsaufsichtsbehörde Scraper
=====================================
Scrapes attorney oversight authority decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (old protocol)
Coverage: Anwaltsaufsichtsbehörde des Kantons Bern
Volume: ~65 decisions
Language: de/fr

Source: https://www.aa-entscheide.apps.be.ch/tribunapublikation
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class BEAnwaltsaufsichtScraper(TribunaBaseScraper):
    CANTON = "BE"
    COURT_CODE_STR = "be_anwaltsaufsicht"
    BASE_URL = "https://www.aa-entscheide.apps.be.ch/tribunapublikation"
    COURT_FILTER = "OG_AA"
    LOCALE = "de"
    REQUEST_DELAY = 2.5
    VERIFY_SSL = False
    SEARCH_FIELD_COUNT = 20  # Old Tribuna version (46-param search)

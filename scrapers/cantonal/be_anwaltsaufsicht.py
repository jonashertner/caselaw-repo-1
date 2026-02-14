"""
Bern Anwaltsaufsichtsbehörde Scraper
======================================
Scrapes decisions from the Weblaw portal at be-anwaltsaufsicht.weblaw.ch.

Platform: Weblaw (query_ticket model)
Coverage: Anwaltsaufsichtsbehörde des Kantons Bern
Language: de/fr

Source: https://be-anwaltsaufsicht.weblaw.ch
"""
from __future__ import annotations

from scrapers.cantonal.base_weblaw import WeblawBaseScraper


class BEAnwaltsaufsichtScraper(WeblawBaseScraper):
    CANTON = "BE"
    COURT_CODE_STR = "be_anwaltsaufsicht"
    DOMAIN = "https://be-anwaltsaufsicht.weblaw.ch"
    SUCH_URL = "/de/recherche"
    START_YEAR = 2000
    PAGE_SIZE = 10
    REQUEST_DELAY = 2.0

"""
Bern Steuerrekurskommission Scraper
====================================
Scrapes tax appeal decisions from the Weblaw portal at be-steuerrekurs.weblaw.ch.

Platform: Weblaw (query_ticket model)
Coverage: Steuerrekurskommission des Kantons Bern
Language: de/fr

Source: https://be-steuerrekurs.weblaw.ch
"""
from __future__ import annotations

from scrapers.cantonal.base_weblaw import WeblawBaseScraper


class BESteuerrekursScraper(WeblawBaseScraper):
    CANTON = "BE"
    COURT_CODE_STR = "be_steuerrekurs"
    DOMAIN = "https://be-steuerrekurs.weblaw.ch"
    SUCH_URL = "/de/recherche"
    START_YEAR = 1990
    PAGE_SIZE = 10
    REQUEST_DELAY = 2.0

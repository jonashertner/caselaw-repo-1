"""
Bern Steuerrekurskommission Scraper
====================================
Scrapes tax appeal commission decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Steuerrekurskommission des Kantons Bern
Volume: ~343 decisions (entscheidsuche), possibly more on portal
Language: de/fr

Source: https://www.strk-entscheide.apps.be.ch/tribunapublikation
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class BESteuerrekursScraper(TribunaBaseScraper):
    CANTON = "BE"
    COURT_CODE_STR = "be_steuerrekurs"
    BASE_URL = "https://www.strk-entscheide.apps.be.ch/tribunapublikation"
    COURT_FILTER = "STRK"  # Steuerrekurskommission
    LOCALE = "de"
    REQUEST_DELAY = 2.5
    VERIFY_SSL = False
    SEARCH_FIELD_COUNT = 21  # New Tribuna version (47-param search)

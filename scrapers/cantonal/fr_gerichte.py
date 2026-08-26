"""
Fribourg Courts Scraper (FR Gerichte)
======================================
Scrapes court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Tribunal cantonal du canton de Fribourg
Volume: ~14,000 decisions
Language: fr/de (bilingual canton)

Source: https://publicationtc.fr.ch (formerly entscheidsuche.fr.ch)
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class FRGerichteScraper(TribunaBaseScraper):
    CANTON = "FR"
    COURT_CODE_STR = "fr_gerichte"
    BASE_URL = "https://publicationtc.fr.ch"
    COURT_FILTER = "TC"  # Tribunal cantonal
    LOCALE = "fr"
    REQUEST_DELAY = 2.5
    # VERIFY_SSL removed 2026-08-26: publicationtc.fr.ch verifies cleanly against
    # the scraper CA bundle (deploy/certs). The old opt-out silently did
    # nothing anyway once REQUESTS_CA_BUNDLE shipped — see base_scraper._build_session.
    SEARCH_FIELD_COUNT = 21  # New Tribuna version (47-param search)

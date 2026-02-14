"""
Fribourg Courts Scraper (FR Gerichte)
======================================
Scrapes court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Tribunal cantonal du canton de Fribourg
Volume: ~14,000 decisions
Language: fr/de (bilingual canton)

Source: https://entscheidsuche.fr.ch
NOTE: DNS was not resolving as of Feb 2026 — portal may be decommissioned.
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class FRGerichteScraper(TribunaBaseScraper):
    CANTON = "FR"
    COURT_CODE_STR = "fr_gerichte"
    BASE_URL = "https://entscheidsuche.fr.ch"
    COURT_FILTER = ""
    LOCALE = "fr"
    REQUEST_DELAY = 2.5

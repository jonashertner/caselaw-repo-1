"""
Graubunden Courts Scraper (GR Gerichte)
========================================
Scrapes court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Kantonsgericht / Verwaltungsgericht Graubunden
Volume: ~14,000 decisions
Language: de/rm/it (trilingual canton)

Source: https://entscheidsuche.gr.ch
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class GRGerichteScraper(TribunaBaseScraper):
    CANTON = "GR"
    COURT_CODE_STR = "gr_gerichte"
    BASE_URL = "https://entscheidsuche.gr.ch"
    COURT_FILTER = "OG"  # Obergericht — returns all courts
    LOCALE = "de"
    REQUEST_DELAY = 2.5

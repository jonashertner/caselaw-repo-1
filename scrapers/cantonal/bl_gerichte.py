"""
Basel-Landschaft Courts Scraper (BL Gerichte)
==============================================
Scrapes court decisions from the Weblaw Vaadin portal at blekg.weblaw.ch.

Platform: Weblaw Vaadin (UIDL-based)
Coverage: Kantonsgericht, Regierungsrat
Volume: ~20,000 decisions
Language: de

Source: https://blekg.weblaw.ch
"""
from __future__ import annotations

from scrapers.cantonal.base_vaadin import WeblawVaadinBaseScraper


class BLGerichteScraper(WeblawVaadinBaseScraper):
    CANTON = "BL"
    COURT_CODE_STR = "bl_gerichte"
    HOST = "https://blekg.weblaw.ch"
    SUCHFORM = "/le/?v-{}"
    REQUEST_DELAY = 2.5

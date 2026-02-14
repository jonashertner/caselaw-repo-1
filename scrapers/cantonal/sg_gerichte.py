"""
St. Gallen Courts Scraper (SG Gerichte)
========================================
Scrapes court decisions from the Weblaw Vaadin portal at sg-entscheide.weblaw.ch.

Platform: Weblaw Vaadin (UIDL-based)
Coverage: Kantonsgericht, Verwaltungsgericht, Versicherungsgericht
Volume: ~13,000 decisions
Language: de

Source: https://sg-entscheide.weblaw.ch
"""
from __future__ import annotations

from scrapers.cantonal.base_vaadin import WeblawVaadinBaseScraper


class SGGerichteScraper(WeblawVaadinBaseScraper):
    CANTON = "SG"
    COURT_CODE_STR = "sg_gerichte"
    HOST = "https://sg-entscheide.weblaw.ch"
    SUCHFORM = "/le/?v-{}"
    REQUEST_DELAY = 2.5

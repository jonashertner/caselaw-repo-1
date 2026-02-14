"""
Obwalden Courts Scraper (OW Gerichte)
======================================
Scrapes court decisions from the Weblaw Vaadin portal.

Platform: Weblaw Vaadin LEv3
Coverage: Obergericht / Verwaltungsgericht Obwalden (OGVE series)
Volume: ~2,200 decisions (1976-present)
Language: de

Source: https://rechtsprechung.ow.ch
"""
from __future__ import annotations

from scrapers.cantonal.base_vaadin import WeblawVaadinBaseScraper


class OWGerichteScraper(WeblawVaadinBaseScraper):
    CANTON = "OW"
    COURT_CODE_STR = "ow_gerichte"
    HOST = "https://rechtsprechung.ow.ch"
    SUCHFORM = "/le/?v-{}"
    REQUEST_DELAY = 2.5

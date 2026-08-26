"""
Bern Verwaltungsgericht Scraper
================================
Scrapes administrative court decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC (new protocol, Feb 2026)
Coverage: Verwaltungsgericht des Kantons Bern
Volume: ~18,000 decisions
Language: de/fr

Source: https://www.vg-urteile.apps.be.ch/tribunapublikation
NOTE: The portal under-fills deep offset pages (offset pagination over an
unstable date sort), so a single pass reaches only ~9,009 of 11,420. Discovery
is therefore date-windowed by year (DATE_WINDOW_FIELD); see base_tribuna.
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class BEVerwaltungsgerichtScraper(TribunaBaseScraper):
    CANTON = "BE"
    COURT_CODE_STR = "be_verwaltungsgericht"
    BASE_URL = "https://www.vg-urteile.apps.be.ch/tribunapublikation"
    COURT_FILTER = "VG"  # Verwaltungsgericht
    LOCALE = "de"
    REQUEST_DELAY = 4.0  # Increased from 2.5 to avoid 503 rate limit
    # VERIFY_SSL removed 2026-08-26: www.vg-urteile.apps.be.ch verifies cleanly against
    # the scraper CA bundle (deploy/certs). The old opt-out silently did
    # nothing anyway once REQUESTS_CA_BUNDLE shipped — see base_scraper._build_session.
    SEARCH_FIELD_COUNT = 21  # New Tribuna version (47-param search)

    # Defeat the deep-offset under-fill: field[16]="YYYY" partitions the corpus
    # exactly (verified: per-year totals sum to the full 11,420). MAX_DEPTH=0
    # (year only) is the confirmed-safe setting; month/day splitting awaits live
    # confirmation of the finer ISO date-filter format.
    DATE_WINDOW_FIELD = 16
    DATE_WINDOW_START_YEAR = 2002
    DATE_WINDOW_MAX_DEPTH = 0

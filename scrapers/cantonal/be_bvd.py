"""
Bern BVD (Bau-, Verkehrs- und Energiedirektion) Scraper
=======================================================
Scrapes BVD administrative-appeal decisions from the Tribuna VTPlus platform.

Platform: Tribuna GWT-RPC. The GWT app at /tribunavtplus IS live (permutation
auto-discovers, config loads, credential len=128), and nf=20 (classic) is the
correct protocol — nf=21 raised a GWT IncompatibleRemoteServiceException.

⚠️ STATUS (live-validated 2026-06-17, --max 3): loadTable for COURT_FILTER="BVD"
returns **Total results: 0** with no error. The portal serves the app but
returns NO data for BVD — consistent with the ORIGINAL registry note "DB
disconnected" (same class as be_steuerrekurs/strk), NOT the es-independence
workflow's "live to 2025-12-11" claim (its probe only confirmed the config
loads, not that loadTable yields rows). So this scraper is protocol-correct but
non-productive as-is. NEXT: enumerate the live DLAConfig's actual court-filter
codes to confirm BVD-has-no-data vs a wrong filter value; if the backend data
is genuinely gone, reclassify be_bvd as dead-upstream and recover the 2,094
decisions from the FROZEN es_be_bvd archive (one-time), like ch_vb. NOT deployed
pending that decision.

Coverage: Bau-, Verkehrs- und Energiedirektion des Kantons Bern
Volume: es archive es_be_bvd = ~2,094 (tail 2025-12-11) — the record of truth
until/unless the live portal yields data.
Language: de
Source: https://www.bvd-entscheide.apps.be.ch/tribunavtplus/
"""
from __future__ import annotations

from scrapers.cantonal.base_tribuna import TribunaBaseScraper


class BEBvdScraper(TribunaBaseScraper):
    CANTON = "BE"
    COURT_CODE_STR = "be_bvd"
    BASE_URL = "https://www.bvd-entscheide.apps.be.ch"
    TRIBUNA_PATH = "tribunavtplus"
    COURT_FILTER = "BVD"            # read verbatim from the live DLAConfig
    LOCALE = "de"
    REQUEST_DELAY = 4.0            # match the BE siblings; avoid 503 rate-limit
    VERIFY_SSL = False             # apps.be.ch SSL-chain issues (same as BE siblings)
    SEARCH_FIELD_COUNT = 20        # classic Tribuna (matches the WORKING be_zivilstraf/be_anwaltsaufsicht; nf=21 yielded a GWT IncompatibleRemoteServiceException on live validation 2026-06-17)

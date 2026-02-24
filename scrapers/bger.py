"""
Scraper for the Swiss Federal Supreme Court (Bundesgericht / BGer).

Based on reverse-engineering of the Eurospider AZA platform

Reimplemented for our pipeline (requests-based, no Scrapy dependency).

CRITICAL: BGer Eurospider now requires Proof-of-Work (PoW) cookies.
Without valid PoW, requests redirect to pow.php and return no data.
The PoW is SHA-256 mining: find nonce such that SHA256(data+nonce) has
N leading zero bits (currently difficulty=16 ≈ 65536 hashes ≈ <1s).
See PoW implementation below.

Sources and URL patterns:
  - Search (AZA):    https://www.bger.ch/ext/eurospider/live/de/php/aza/http/index.php
  - Search (CLIR):   https://search.bger.ch/ext/eurospider/live/de/php/clir/http/index_atf.php
  - Decision (Jump): http://relevancy.bger.ch/cgi-bin/JumpCGI?id={DD.MM.YYYY}_{DOCKET}
  - RSS:             https://search.bger.ch/ext/eurospider/live/de/php/aza/rss/index_aza.php
  - Neuheiten:       https://search.bger.ch/ext/eurospider/live/{lang}/php/aza/http/index_aza.php

Entscheidsuche reference:
  - Spider name: CH_BGer (regular decisions), CH_BGE (Leitentscheide)
  - Signature: CH_BGer_{year}_{docket_normalized}
  - Config CSV: https://docs.google.com/spreadsheets/d/e/2PACX-1vR2sZY8...

BGer Abteilungen (docket prefix mapping):
  CH_BGer_001  I.  Öffentl.-rechtl.:    1B, 1C, 1D, 1E, 1F, 1G, 1X, 1Y
  CH_BGer_002  II. Öffentl.-rechtl.:    2B, 2C, 2D, 2E, 2F, 2G, 2X, 2Y
  CH_BGer_004  I.  Zivilrechtl.:        4A, 4D, 4E, 4F, 4G, 4X, 4Y
  CH_BGer_005  II. Zivilrechtl.:        5A, 5D, 5E, 5F, 5G, 5X, 5Y
  CH_BGer_006  I.  Strafrecht.:         6B, 6D, 6E, 6F, 6G, 6S, 6P, 6X, 6Y
  CH_BGer_007  II. Strafrecht.:         7B, 7D, 7E, 7F, 7G, 7X, 7Y
  CH_BGer_008  III. Öffentl.-r. (Soz.): 8C, 8D, 8E, 8F, 8G, 8X, 8Y
  CH_BGer_009  IV.  Öffentl.-r. (Soz.): 9C, 9D, 9E, 9F, 9G, 9X, 9Y
  CH_BGer_015  Verwaltungskommission:   12T

Rate limiting: 2s delay, 1 concurrent request (Eurospider is sensitive).
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import Decision, extract_citations, make_decision_id
from incapsula_bypass import IncapsulaCookieManager

logger = logging.getLogger(__name__)

# Optional: AES-CBC encryption for PoW data (not strictly required)
try:
    from Cryptodome.Cipher import AES
    import base64 as _b64
except ImportError:
    AES = None


# ═══════════════════════════════════════════════════════════════════════════════
# URL TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════

# Eurospider host
HOST = "https://www.bger.ch"
SEARCH_HOST = "https://search.bger.ch"

# AZA search — regular decisions
# AZA search URL template with date params
AZA_INITIAL_URL = (
    HOST + "/ext/eurospider/live/de/php/aza/http/index.php"
)
AZA_SEARCH_URL = (
    HOST + "/ext/eurospider/live/de/php/aza/http/index.php?"
    "lang=de&type=simple_query&query_words=&top_subcollection_aza=all"
    "&from_date={von}&to_date={bis}"
)

# CLIR search — BGE Leitentscheide
CLIR_SEARCH_URL = (
    SEARCH_HOST + "/ext/eurospider/live/de/php/clir/http/index_atf.php?"
    "lang=de&zoom=&system=clir"
)

# JumpCGI — direct decision URL (confirmed working)
JUMP_URL = "http://relevancy.bger.ch/cgi-bin/JumpCGI?id={date}_{docket}"

# RSS feed
RSS_URL = (
    SEARCH_HOST + "/ext/eurospider/live/de/php/aza/rss/index_aza.php"
)

# Neuheiten (recently published)
NEUHEITEN_URL = (
    SEARCH_HOST
    + "/ext/eurospider/live/{lang}/php/aza/http/index_aza.php"
)


# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST HEADERS
# ═══════════════════════════════════════════════════════════════════════════════

BGER_HEADERS = {
    # Chrome UA — must match TLS fingerprint from Playwright/Chromium
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de,en-US;q=0.7,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


# ═══════════════════════════════════════════════════════════════════════════════
# PROOF OF WORK
#
# BGer Eurospider deploys JS-based PoW. Without valid cookies, requests
# redirect to pow.php. The spider must mine a SHA-256 hash with N leading
# zero bits, then send the result as cookies.
# ═══════════════════════════════════════════════════════════════════════════════

POW_DIFFICULTY = 16  # bits — required by Eurospider
POW_AES_KEY = "9f3c1a8e7b4d62f1e0b5c47a2d8f93bc"  # From BGer's public JavaScript (not a secret)


def _has_leading_zero_bits(b: bytes, difficulty_bits: int) -> bool:
    """Check if byte string has N leading zero bits."""
    bits = difficulty_bits
    i = 0
    while bits >= 8:
        if i >= len(b) or b[i] != 0:
            return False
        i += 1
        bits -= 8
    if bits > 0:
        mask = (0xFF << (8 - bits)) & 0xFF
        if i >= len(b) or (b[i] & mask) != 0:
            return False
    return True


def mine_pow(difficulty_bits: int = POW_DIFFICULTY) -> dict:
    """
    Mine a Proof-of-Work hash for BGer Eurospider.

    Returns dict with keys: pow_data, pow_hash, pow_nonce, pow_difficulty.
    These become cookies on the request.
    """
    # Generate random fingerprint (sha256 of random bytes)
    pow_data_raw = hashlib.sha256(os.urandom(32)).hexdigest()

    nonce = 0
    t0 = time.time()
    while True:
        h = hashlib.sha256(f"{pow_data_raw}{nonce}".encode("utf-8")).digest()
        if _has_leading_zero_bits(h, difficulty_bits):
            elapsed = time.time() - t0
            pow_hash = h.hex()
            logger.info(
                f"PoW mined: difficulty={difficulty_bits}, nonce={nonce}, "
                f"elapsed={elapsed:.2f}s"
            )

            # Optionally encrypt pow_data with AES-CBC
            pow_data_cookie = pow_data_raw
            if AES is not None:
                try:
                    key = bytes.fromhex(POW_AES_KEY)
                    iv = os.urandom(16)
                    plaintext = pow_data_raw.encode("utf-8")
                    # Zero-padding (PKCS-style)
                    rem = len(plaintext) % 16
                    if rem:
                        plaintext += b"\x00" * (16 - rem)
                    cipher = AES.new(key, AES.MODE_CBC, iv)
                    ct = cipher.encrypt(plaintext)
                    pow_data_cookie = _b64.b64encode(iv + ct).decode("ascii")
                except Exception as e:
                    logger.debug(f"AES encryption optional, using raw: {e}")

            return {
                "pow_data": pow_data_cookie,
                "pow_data_raw": pow_data_raw,
                "pow_hash": pow_hash,
                "pow_nonce": nonce,
                "pow_difficulty": difficulty_bits,
            }
        nonce += 1


def make_pow_cookies(pow_result: dict) -> dict:
    """Create cookie dict from PoW mining result."""
    return {
        "powData": pow_result["pow_data"],
        "powDifficulty": str(pow_result["pow_difficulty"]),
        "powHash": pow_result["pow_hash"],
        "powNonce": str(pow_result["pow_nonce"]),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Abteilung <-> docket prefix mapping
# ═══════════════════════════════════════════════════════════════════════════════

ABTEILUNG_MAP: dict[str, dict] = {
    "CH_BGer_001": {
        "de": "I. Öffentlich-rechtliche Abteilung",
        "fr": "Ire Cour de droit public",
        "it": "I Corte di diritto pubblico",
        "prefixes": ["1B", "1C", "1D", "1E", "1F", "1G", "1X", "1Y"],
    },
    "CH_BGer_002": {
        "de": "II. Öffentlich-rechtliche Abteilung",
        "fr": "IIe Cour de droit public",
        "it": "II Corte di diritto pubblico",
        "prefixes": ["2B", "2C", "2D", "2E", "2F", "2G", "2X", "2Y"],
    },
    "CH_BGer_004": {
        "de": "I. Zivilrechtliche Abteilung",
        "fr": "Ire Cour de droit civil",
        "it": "I Corte di diritto civile",
        "prefixes": ["4A", "4D", "4E", "4F", "4G", "4X", "4Y"],
    },
    "CH_BGer_005": {
        "de": "II. Zivilrechtliche Abteilung",
        "fr": "IIe Cour de droit civil",
        "it": "II Corte di diritto civile",
        "prefixes": ["5A", "5D", "5E", "5F", "5G", "5X", "5Y"],
    },
    "CH_BGer_006": {
        "de": "I. Strafrechtliche Abteilung",
        "fr": "Cour de droit pénal",
        "it": "Corte di diritto penale",
        "prefixes": ["6B", "6D", "6E", "6F", "6G", "6S", "6P", "6X", "6Y"],
    },
    "CH_BGer_007": {
        "de": "Beschwerdekammer des Bundesstrafgerichts",
        "fr": "Cour des plaintes du Tribunal pénal fédéral",
        "it": "Corte dei reclami penali del Tribunale penale federale",
        "prefixes": ["7B", "7D", "7E", "7F", "7G", "7X", "7Y"],
    },
    "CH_BGer_008": {
        "de": "III. Öffentlich-rechtliche Abteilung",
        "fr": "IIIe Cour de droit public",
        "it": "III Corte di diritto pubblico",
        "prefixes": ["8C", "8D", "8E", "8F", "8G", "8X", "8Y"],
    },
    "CH_BGer_009": {
        "de": "IV. Öffentlich-rechtliche Abteilung",
        "fr": "IVe Cour de droit public",
        "it": "IV Corte di diritto pubblico",
        "prefixes": ["9C", "9D", "9E", "9F", "9G", "9X", "9Y"],
    },
    "CH_BGer_015": {
        "de": "Verwaltungskommission",
        "fr": "Commission administrative",
        "it": "Commissione amministrativa",
        "prefixes": ["12T"],
    },
}

# Reverse lookup: prefix -> (signatur, info)
PREFIX_TO_ABTEILUNG: dict[str, tuple[str, dict]] = {}
for _sig, _info in ABTEILUNG_MAP.items():
    for _p in _info["prefixes"]:
        PREFIX_TO_ABTEILUNG[_p] = (_sig, _info)


# ═══════════════════════════════════════════════════════════════════════════════
# Outcome patterns (de/fr/it — searched in Dispositiv section)
# ═══════════════════════════════════════════════════════════════════════════════

OUTCOME_PATTERNS: list[tuple[str, str]] = [
    # DE
    ("teilweise gutgeheissen", "partial_approval"),
    ("gutgeheissen", "approved"),
    ("abgewiesen", "dismissed"),
    ("nichteintreten", "inadmissible"),
    ("nicht eingetreten", "inadmissible"),
    ("gegenstandslos", "moot"),
    ("als gegenstandslos abgeschrieben", "moot"),
    ("vereinigt", "consolidated"),
    # FR
    ("partiellement admis", "partial_approval"),
    ("admis", "approved"),
    ("rejeté", "dismissed"),
    ("irrecevable", "inadmissible"),
    ("sans objet", "moot"),
    ("rayé du rôle", "moot"),
    # IT
    ("parzialmente accolto", "partial_approval"),
    ("accolto", "approved"),
    ("respinto", "dismissed"),
    ("inammissibile", "inadmissible"),
    ("privo d'oggetto", "moot"),
    ("privo di oggetto", "moot"),
]


# ═══════════════════════════════════════════════════════════════════════════════
# Language detection word lists
# ═══════════════════════════════════════════════════════════════════════════════

_LANG_WORDS = {
    "de": re.compile(
        r"\b(?:der|die|das|ein|eine|einer|er|sie|ihn|hat|hatte|hätte"
        r"|ist|war|sind)\b"
    ),
    "fr": re.compile(
        r"\b(?:le|lui|elle|je|on|vous|nous|leur|qui|quand|parce"
        r"|que|faire|sont|vont)\b"
    ),
    "it": re.compile(
        r"\b(?:della|del|di|casi|una|al|questa|più|primo|grado"
        r"|che|diritto|leggi|corte)\b"
    ),
}


# ═══════════════════════════════════════════════════════════════════════════════
# SCRAPER CLASS
# ═══════════════════════════════════════════════════════════════════════════════


class BgerScraper(BaseScraper):
    """
    Scraper for the Swiss Federal Supreme Court (Bundesgericht).

    Scrapes search.bger.ch AZA platform with PoW authentication.
    Key features:
    - Proof-of-Work mining for Eurospider anti-scraping
    - Session-based requests with PoW cookies
    - 4-day windowed backfill with daily fallback for >100 results
    - Retry on pow.php redirects (up to 5 attempts)
    - Confirmed HTML selectors from production code
    """

    REQUEST_DELAY = 2.0  # Eurospider rate limit

    # Search in 4-day windows for manageable result sets
    WINDOW_DAYS = 4
    MAX_RETRIES = 5  # pow.php redirect retries

    def __init__(self, state_dir: Path = Path("state")):
        super().__init__(state_dir=state_dir)
        self._pow: dict | None = None
        self._session_cookies: dict = {}
        self._pow_required: bool = False  # Determined at runtime by probing
        self._incapsula = IncapsulaCookieManager(cache_dir=state_dir)

    @property
    def court_code(self) -> str:
        return "bger"

    # ───────────────────────────────────────────────────────────────────────
    # SESSION & POW
    # ───────────────────────────────────────────────────────────────────────

    def _ensure_pow(self) -> dict:
        """Ensure we have a valid PoW result. Mine if needed."""
        if self._pow is None:
            self._pow = mine_pow(POW_DIFFICULTY)
            self._session_cookies = make_pow_cookies(self._pow)
        return self._pow

    def _init_session(self) -> None:
        """
        Initialize session by solving Incapsula and optionally PoW.

        Flow:
        1. Harvest Incapsula cookies via browser automation (cached)
        2. Apply to requests.Session
        3. Probe: does AZA redirect to pow.php?
           - Yes → mine PoW, set PoW cookies
           - No  → skip PoW (saves time and avoids infinite mining loops)
        4. If still blocked by Incapsula, force-refresh cookies
        """
        # Step 1: Incapsula cookies (browser automation) for both domains
        for domain in ["www.bger.ch", "search.bger.ch"]:
            try:
                incap_cookies = self._incapsula.get_cookies(domain)
                self.session.cookies.update(incap_cookies)
                logger.info(f"Applied {len(incap_cookies)} Incapsula cookies for {domain}")
            except Exception as e:
                logger.warning(f"Incapsula cookie harvest failed for {domain}: {e}")

        # Step 2: Probe — is PoW still required?
        logger.info("Probing AZA to determine if PoW is required")
        try:
            resp = self.get(AZA_INITIAL_URL, headers=BGER_HEADERS)

            if "pow.php" in resp.url:
                # PoW redirect detected — mine and retry
                logger.info("PoW redirect detected, mining proof-of-work")
                self._pow_required = True
                self._ensure_pow()
                self.session.cookies.update(self._session_cookies)
                resp = self.get(
                    AZA_INITIAL_URL,
                    headers=BGER_HEADERS,
                    cookies=self._session_cookies,
                )
            else:
                logger.info("No PoW redirect — skipping PoW mining")
                self._pow_required = False

            # Check if Incapsula is still blocking
            if self._incapsula.is_incapsula_blocked(resp.text):
                logger.warning("Still blocked after initial cookies, force-refreshing")
                incap_cookies = self._incapsula.refresh_cookies("www.bger.ch")
                self.session.cookies.update(incap_cookies)
                resp = self.get(
                    AZA_INITIAL_URL,
                    headers=BGER_HEADERS,
                    cookies=self._session_cookies if self._pow_required else {},
                )

            logger.info(
                f"Session initialized, status={resp.status_code}, "
                f"len={len(resp.text)}, pow_required={self._pow_required}"
            )
        except Exception as e:
            logger.warning(f"Session init failed (continuing anyway): {e}")

    def _get_with_pow(self, url: str, retry: int = 0) -> "requests.Response":
        """
        GET with Incapsula + optional PoW cookies and retry logic.

        Handles:
        - Incapsula JS challenge blocks (refresh via browser automation)
        - pow.php redirects (mine PoW if needed)
        - help-hilfe redirects (re-mine PoW)
        - Short/empty responses (re-mine PoW)

        Adds exponential backoff between retries.
        """
        # Only send PoW cookies if PoW is required
        cookies = self._session_cookies if self._pow_required else {}
        if self._pow_required:
            self._ensure_pow()

        resp = self.get(
            url,
            headers=BGER_HEADERS,
            cookies=cookies,
        )

        # Determine which domain to refresh cookies for
        from urllib.parse import urlparse
        domain = urlparse(url).hostname or "www.bger.ch"
        if domain not in ("www.bger.ch", "search.bger.ch"):
            domain = "www.bger.ch"

        # Check for Incapsula block (JS challenge page or very short response)
        is_blocked = (
            self._incapsula.is_incapsula_blocked(resp.text)
            or (len(resp.text) < 10 and resp.status_code == 200)
        )
        if is_blocked and retry < self.MAX_RETRIES:
            logger.info(f"Block detected ({len(resp.text)} chars), refreshing {domain} cookies ({retry+1}/{self.MAX_RETRIES})")
            time.sleep(2 + retry * 2)  # Backoff before retry
            try:
                incap_cookies = self._incapsula.refresh_cookies(domain)
                self.session.cookies.update(incap_cookies)
            except Exception as e:
                logger.error(f"Incapsula refresh failed for {domain}: {e}")
            return self._get_with_pow(url, retry + 1)

        # Check for pow.php redirect
        if "pow.php" in resp.url and retry < self.MAX_RETRIES:
            logger.info(f"PoW redirect detected, mining PoW ({retry+1}/{self.MAX_RETRIES})")
            self._pow_required = True
            self._pow = mine_pow(POW_DIFFICULTY)
            self._session_cookies = make_pow_cookies(self._pow)
            time.sleep(2 + retry * 2)
            fixed_url = resp.url.replace("pow.php", "index.php")
            return self._get_with_pow(fixed_url, retry + 1)

        # Check for help page redirect (another form of PoW rejection)
        if "help-hilfe" in resp.url and retry < self.MAX_RETRIES:
            logger.info(f"Help page redirect (PoW rejected), re-mining ({retry+1}/{self.MAX_RETRIES})")
            self._pow_required = True
            self._pow = mine_pow(POW_DIFFICULTY)
            self._session_cookies = make_pow_cookies(self._pow)
            time.sleep(2 + retry * 2)
            return self._get_with_pow(url, retry + 1)

        # Check for empty/garbage response (not the PoW page but still bad)
        if len(resp.text) < 200 and retry < self.MAX_RETRIES:
            logger.info(f"Short response ({len(resp.text)} chars), retrying")
            time.sleep(2 + retry * 2)
            # Re-mine PoW if it was required
            if self._pow_required:
                self._pow = mine_pow(POW_DIFFICULTY)
                self._session_cookies = make_pow_cookies(self._pow)
            return self._get_with_pow(url, retry + 1)

        return resp

    # ═══════════════════════════════════════════════════════════════════════
    # DISCOVERY
    # ═══════════════════════════════════════════════════════════════════════

    # BGer publishes decisions with a 2-8 week lag after the decision date.
    # A 14-day AZA search window misses late-published decisions.
    DAILY_LOOKBACK_DAYS = 90

    def discover_new(self, since_date: date | None = None) -> Iterator[dict]:
        """
        Discover new BGer decisions.

        Uses AZA search on www.bger.ch (the only reliable path since
        search.bger.ch blocks requests at the TLS level as of Feb 2026).

        Also checks Neuheiten (recently added) to catch decisions published
        after the AZA search window has moved past their decision date.

        Daily mode (since_date within last 60 days or None):
            1. Neuheiten page (recently published, any decision date)
            2. AZA search for the last 60 days
        Backfill mode (since_date > 60 days ago):
            AZA search in 4-day windows from since_date.
        """
        if isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        self._init_session()

        if not since_date or since_date >= date.today() - timedelta(days=self.DAILY_LOOKBACK_DAYS):
            # Daily mode: Neuheiten first (catches late-published decisions),
            # then AZA search for broader coverage
            logger.info("Checking Neuheiten for recently published decisions")
            yield from self._discover_via_neuheiten()

            search_from = since_date or (date.today() - timedelta(days=self.DAILY_LOOKBACK_DAYS))
            logger.info(f"Daily mode: AZA search from {search_from}")
            yield from self._discover_via_search(search_from)
        else:
            logger.info(f"Backfill mode from {since_date}")
            yield from self._discover_via_search(since_date)

    # ───────────────────────────────────────────────────────────────────────
    # Discovery via RSS
    # ───────────────────────────────────────────────────────────────────────

    def _discover_via_rss(self) -> Iterator[dict]:
        """Parse RSS feed for newest decisions. Lightweight first pass."""
        try:
            resp = self.get(RSS_URL)
            root = ET.fromstring(resp.content)

            for item in root.iter("item"):
                title_el = item.find("title")
                link_el = item.find("link")
                if title_el is None or link_el is None:
                    continue

                title = title_el.text or ""
                link = link_el.text or ""

                docket = self._extract_docket(title) or self._extract_docket(link)
                if not docket:
                    continue

                decision_id = make_decision_id("bger", docket)
                if self.state.is_known(decision_id):
                    continue

                pub_date_el = item.find("pubDate")
                decision_date = None
                if pub_date_el is not None and pub_date_el.text:
                    decision_date = self._parse_rss_date(pub_date_el.text)

                yield {
                    "docket_number": docket,
                    "decision_date": decision_date,
                    "url": link,
                    "language": "de",
                    "decision_id": decision_id,
                }

        except Exception as e:
            logger.warning(f"RSS feed error (non-fatal): {e}")

    # ───────────────────────────────────────────────────────────────────────
    # Discovery via Neuheiten page
    # ───────────────────────────────────────────────────────────────────────

    def _discover_via_neuheiten(self) -> Iterator[dict]:
        """
        Parse 'Liste der Neuheiten' for recently added decisions.

        Note: Scraping only German for backfill (all decisions
        appear regardless of language). For Neuheiten we also check de only
        since the content is the same across all 3 interface languages.
        """
        url = NEUHEITEN_URL.format(lang="de")
        try:
            resp = self._get_with_pow(url)
            soup = BeautifulSoup(resp.text, "html.parser")
            yield from self._parse_search_results(soup, "de")
        except Exception as e:
            logger.error(f"Neuheiten: {e}")

    # ───────────────────────────────────────────────────────────────────────
    # Discovery via AZA search (backfill)
    # ───────────────────────────────────────────────────────────────────────

    def _discover_via_search(self, since_date: date) -> Iterator[dict]:
        """
        Search AZA endpoint by date range for backfill.

        Strategy:
        - Windows of TAGSCHRITTE=4 days
        - If >100 results in a window, split into individual daily requests
        - Only German (all languages appear in AZA regardless of interface)
        - Paginate up to 10 pages (10 results/page) per query
        """
        current = since_date
        today = date.today()

        while current <= today:
            end = min(current + timedelta(days=self.WINDOW_DAYS - 1), today)
            von_str = current.strftime("%d.%m.%Y")
            bis_str = end.strftime("%d.%m.%Y")

            logger.info(f"Searching {von_str} - {bis_str}")

            url = AZA_SEARCH_URL.format(von=von_str, bis=bis_str)
            try:
                resp = self._get_with_pow(url)
                soup = BeautifulSoup(resp.text, "html.parser")

                # Check hit count
                treffer_count = self._get_hit_count(soup)

                if treffer_count is not None and treffer_count > 100:
                    # Too many results — split into daily requests

                    logger.warning(
                        f"Window {von_str}-{bis_str}: {treffer_count} hits > 100, "
                        f"splitting to daily"
                    )
                    day = current
                    while day <= end:
                        day_str = day.strftime("%d.%m.%Y")
                        day_url = AZA_SEARCH_URL.format(von=day_str, bis=day_str)
                        try:
                            day_resp = self._get_with_pow(day_url)
                            day_soup = BeautifulSoup(day_resp.text, "html.parser")
                            yield from self._parse_search_results(
                                day_soup, "de", fallback_date=day
                            )
                            yield from self._follow_pagination(
                                day_soup, "de", day
                            )
                        except Exception as e:
                            logger.error(f"Daily search {day_str}: {e}")
                        day += timedelta(days=1)
                elif treffer_count == 0 or self._is_no_results(soup):
                    logger.debug(f"No results for {von_str}-{bis_str}")
                else:
                    yield from self._parse_search_results(
                        soup, "de", fallback_date=current
                    )
                    yield from self._follow_pagination(soup, "de", current)

            except Exception as e:
                logger.error(f"Search {von_str}-{bis_str}: {e}")

            current = end + timedelta(days=1)

    # ───────────────────────────────────────────────────────────────────────
    # Search result parsing
    # ───────────────────────────────────────────────────────────────────────

    def _get_hit_count(self, soup: BeautifulSoup) -> int | None:
        """
        Extract total hit count from search results page.

        Extract total hit count from response.
        XPath: //div[@class='content']/div[@class='ranklist_header center']/text()
        Returns e.g. "42 Urteile gefunden" -> 42
        """
        # ranklist_header center contains "N Urteile gefunden"
        header = soup.select_one("div.content div.ranklist_header.center")
        if header:
            text = header.get_text(strip=True)
            m = re.match(r"(\d+)", text)
            if m:
                return int(m.group(1))
        return None

    def _is_no_results(self, soup: BeautifulSoup) -> bool:
        """
        Check if page says "keine Urteile gefunden".

        Check for empty result indicator.
        XPath: //div[@class='content']/div[@class='ranklist_content center']/text()
        """
        no_hit = soup.select_one("div.content div.ranklist_content.center")
        if no_hit and "keine Urteile gefunden" in no_hit.get_text():
            return True
        return False

    def _parse_search_results(
        self, soup: BeautifulSoup, lang: str, fallback_date: date | None = None
    ) -> Iterator[dict]:
        """
        Parse search results page.

        Parse result rows into decision metadata:
        XPath selectors:
          - Result list: //div[@class='ranklist_content']/ol/li
          - Decision link: ./span/a/@href
          - Date + Docket: ./span/a/text()  (first 10 chars = DD.MM.YYYY, rest = docket)
          - VKammer: ./div/div[1]/text()
          - Rechtsgebiet: ./div/div[2]/text()
          - Titel: ./div/div[3]/text()
        """
        # Find the ordered list within ranklist_content
        ranklist = soup.select_one("div.ranklist_content ol")
        if not ranklist:
            # Fallback: try any ol with li items containing decision links
            ranklist = soup.find("ol")
            if not ranklist:
                return

        for li in ranklist.find_all("li", recursive=False):
            # Extract link and meta from span > a
            link = li.select_one("span > a")
            if not link:
                # Fallback: any a tag in the li
                link = li.find("a", href=True)
            if not link:
                continue

            href = link.get("href", "")
            meta_text = link.get_text(strip=True)

            # Parse meta_text: "DD.MM.YYYY DOCKET_NUMBER"
            # EDatum from first 10 chars, Num from char 11+
            decision_date = fallback_date
            docket = None

            if len(meta_text) >= 10:
                date_part = meta_text[:10].strip()
                dm = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", date_part)
                if dm:
                    try:
                        decision_date = date(
                            int(dm.group(3)), int(dm.group(2)), int(dm.group(1))
                        )
                    except ValueError:
                        pass
                    # Docket is everything after the date
                    docket_part = meta_text[10:].strip()
                    docket = self._extract_docket(docket_part)

            if not docket:
                docket = self._extract_docket(meta_text) or self._extract_docket(href)

            if not docket:
                continue

            decision_id = make_decision_id("bger", docket)
            if self.state.is_known(decision_id):
                continue

            # Extract additional metadata from list item
            # div/div[1] = VKammer, div/div[2] = Rechtsgebiet,
            #              ./div/div[3] = Titel
            stub: dict = {
                "docket_number": docket,
                "decision_date": decision_date,
                "url": self._abs_url(href),
                "language": lang,
                "decision_id": decision_id,
            }

            divs = li.select("div > div")
            if len(divs) >= 1:
                stub["vkammer"] = divs[0].get_text(strip=True)
            if len(divs) >= 2:
                stub["rechtsgebiet"] = divs[1].get_text(strip=True)
            if len(divs) >= 3:
                stub["title"] = divs[2].get_text(strip=True)

            yield stub

    def _follow_pagination(
        self, soup: BeautifulSoup, lang: str, search_date: date
    ) -> Iterator[dict]:
        """
        Follow pagination in search results.

        Handle pagination:
        Paginates if anfangsposition + len(urteile) < treffer.
        Max 10 pages (page 10 = results 91-100).
        """
        total = self._get_hit_count(soup)
        if total is None or total <= 10:
            return

        # Determine how many pages we need (10 results per page)
        max_page = min((total + 9) // 10, 10)  # Cap at 10 pages

        for page in range(2, max_page + 1):
            # Always construct URL from known search template — never
            # extract from response HTML (PoW redirects produce garbage URLs)
            von_str = search_date.strftime("%d.%m.%Y")
            page_url = (
                AZA_SEARCH_URL.format(von=von_str, bis=von_str)
                + f"&page={page}"
            )

            try:
                resp = self._get_with_pow(page_url)
                page_soup = BeautifulSoup(resp.text, "html.parser")

                # Verify we got actual results, not a PoW/help redirect
                if self._is_no_results(page_soup) and "help" in resp.url:
                    logger.warning(
                        f"Pagination page {page}: got help page redirect, "
                        f"PoW may have expired"
                    )
                    # Re-mine PoW and retry once
                    self._pow = None
                    self._ensure_pow()
                    resp = self._get_with_pow(page_url)
                    page_soup = BeautifulSoup(resp.text, "html.parser")

                yield from self._parse_search_results(
                    page_soup, lang, fallback_date=search_date
                )
            except Exception as e:
                logger.error(f"Pagination page {page}: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # FETCH
    # ═══════════════════════════════════════════════════════════════════════

    def fetch_decision(self, stub: dict) -> Decision | None:
        """
        Fetch full decision text and metadata.

        Strategy (updated after live testing 2026-02-08):
        1. Try relevancy.bger.ch direct URL (NO PoW required — confirmed)
        2. Try Eurospider URL from discovery (with PoW + Incapsula)
        3. Fall back to JumpCGI URL
        """
        html = None
        source_url = None

        # Primary: relevancy.bger.ch (no PoW needed — confirmed by live testing)
        jump_url = self._make_jump_url(stub)
        if jump_url:
            try:
                # Use plain session without PoW cookies for relevancy.bger.ch
                resp = self.get(jump_url, headers=BGER_HEADERS)
                if (resp.ok and len(resp.text) > 500
                        and "pow.php" not in resp.url
                        and not self._incapsula.is_incapsula_blocked(resp.text)):
                    html = resp.text
                    source_url = jump_url
                    logger.debug(f"Fetched via relevancy.bger.ch (no PoW): {stub['docket_number']}")
            except Exception as e:
                logger.debug(f"relevancy.bger.ch failed for {stub['docket_number']}: {e}")

        # Fallback: Eurospider URL from search results (with PoW)
        if not html and stub.get("url"):
            try:
                resp = self._get_with_pow(stub["url"])
                if resp.ok and len(resp.text) > 500:
                    html = resp.text
                    source_url = stub["url"]
            except Exception as e:
                logger.debug(f"Eurospider URL failed for {stub['docket_number']}: {e}")

        if not html:
            logger.error(f"Could not fetch {stub['docket_number']}")
            return None

        return self._parse_decision_html(html, stub, source_url or "")

    def _parse_decision_html(
        self, html: str, stub: dict, source_url: str
    ) -> Decision | None:
        """Parse decision HTML into a Decision object."""
        soup = BeautifulSoup(html, "html.parser")

        # ── Full text (confirmed selector) ──
        full_text = self._extract_full_text(soup)
        if not full_text or len(full_text) < 50:
            logger.warning(
                f"Too short text for {stub['docket_number']}: "
                f"{len(full_text or '')} chars"
            )
            return None

        # ── Page metadata ──
        meta = self._extract_metadata(soup, full_text)

        # ── Merge stub metadata from search results ──
        if not meta.get("title") and stub.get("title"):
            meta["title"] = stub["title"]
        if not meta.get("legal_area") and stub.get("rechtsgebiet"):
            meta["legal_area"] = stub["rechtsgebiet"]

        # ── Decision date ──
        decision_date = self._resolve_date(stub.get("decision_date"))
        if not decision_date:
            logger.warning(f"[{self.court_code}] No date for {stub.get('docket_number', '?')}")

        # ── Language ──
        language = self._detect_language(full_text)

        # ── Chamber from docket prefix or page text ──
        docket = stub["docket_number"]
        chamber = (
            meta.get("chamber")
            or stub.get("vkammer")
            or self._docket_to_abteilung(docket, language)
        )

        return Decision(
            decision_id=make_decision_id("bger", docket),
            court="bger",
            canton="CH",
            chamber=chamber,
            docket_number=docket,
            decision_date=decision_date,
            language=language,
            title=meta.get("title"),
            legal_area=meta.get("legal_area"),
            regeste=meta.get("regeste"),
            full_text=full_text,
            outcome=meta.get("outcome"),
            judges=meta.get("judges"),
            clerks=meta.get("clerks"),
            source_url=source_url,
            bge_reference=meta.get("bge_reference"),
            cited_decisions=extract_citations(full_text),
        )

    # ═══════════════════════════════════════════════════════════════════════
    # TEXT EXTRACTION
    # ═══════════════════════════════════════════════════════════════════════

    def _extract_full_text(self, soup: BeautifulSoup) -> str:
        """
        Extract the full decision text from the HTML page.

        Primary selector:
          //div[@id='highlight_content']/div[@class='content']

        Fallback strategies for edge cases:
        1. div#highlight_content > div.content  (CONFIRMED primary)
        2. div.paraatf (BGE Leitentscheide via CLIR endpoint)
        3. div.content / div#content (generic Eurospider)
        4. td.content (older layout)
        5. div.WordSection1 (some newer formats)
        6. Largest text block with legal markers (last resort)
        """
        # Strategy 1: CONFIRMED primary selector
        # Primary: highlight_content div
        #   html=response.xpath("//div[@id='highlight_content']/div[@class='content']")
        content = soup.select_one("div#highlight_content > div.content")
        if content and len(content.get_text(strip=True)) > 100:
            return self._clean_text(content.get_text(separator="\n"))

        # Strategy 2: paraatf (BGE decisions via CLIR)
        # Fallback: textcontent div
        #   meta=response.xpath("//div[@class='paraatf']/text()")
        paraatf = soup.find("div", class_="paraatf")
        if paraatf and len(paraatf.get_text(strip=True)) > 100:
            return self._clean_text(paraatf.get_text(separator="\n"))

        # Strategy 3-5: other known containers
        for selector in [
            "div#highlight_content",  # without .content child
            "div.content",
            "div#content",
            "td.content",
            "div.WordSection1",
        ]:
            el = soup.select_one(selector)
            if el and len(el.get_text(strip=True)) > 200:
                return self._clean_text(el.get_text(separator="\n"))

        # Strategy 6: score block elements for legal content markers
        legal_markers = [
            "Sachverhalt", "Erwägung", "Dispositiv", "Beschwerde",
            "Besetzung", "Gegenstand", "Bundesrichter",
            "Faits", "Considérant", "Dispositif", "Recours",
            "Composition", "Objet", "Juge fédéral",
            "Fatto", "Considerando", "Dispositivo", "Ricorso",
            "Composizione", "Oggetto", "Giudice federale",
        ]

        best_el = None
        best_score = 0
        for tag in soup.find_all(["div", "td", "article", "section"]):
            text = tag.get_text(strip=True)
            if len(text) < 200:
                continue
            score = len(text)
            text_lower = text.lower()
            for marker in legal_markers:
                if marker.lower() in text_lower:
                    score += 5000
            if score > best_score:
                best_score = score
                best_el = tag

        if best_el:
            return self._clean_text(best_el.get_text(separator="\n"))

        body = soup.find("body")
        if body:
            return self._clean_text(body.get_text(separator="\n"))
        return ""

    # ═══════════════════════════════════════════════════════════════════════
    # METADATA EXTRACTION
    # ═══════════════════════════════════════════════════════════════════════

    def _extract_metadata(self, soup: BeautifulSoup, full_text: str) -> dict:
        """
        Extract structured metadata from the decision page.

        Fields match NeuescraperItem (items.py):
        - Gericht/Kammer (chamber)
        - Judges, Clerks
        - Title/Subject
        - Regeste (BGE)
        - BGE reference
        - Outcome
        """
        meta: dict = {}
        text = soup.get_text()

        # ── Chamber/Abteilung from page text ──
        # Sort by name length descending to match "II. Öffentlich-" before "I. Öffentlich-"
        text_lower = text.lower()
        sorted_abt = sorted(
            ABTEILUNG_MAP.items(),
            key=lambda kv: max(len(kv[1][lang]) for lang in ["de", "fr", "it"]),
            reverse=True,
        )
        for _, info in sorted_abt:
            for lang_key in ["de", "fr", "it"]:
                name = info[lang_key].lower()
                if name in text_lower:
                    meta["chamber"] = info["de"]
                    break
            if "chamber" in meta:
                break

        # ── Judges ──
        judges_m = re.search(
            r"(?:Besetzung|Composition|Composizione)\s*:?\s*(.*?)"
            r"(?:\.\s*\n|\n\s*\n|Parteien|Parties|Parti|Verfahrensbeteiligte)",
            text, re.DOTALL | re.IGNORECASE,
        )
        if judges_m:
            raw = re.sub(r"\s+", " ", judges_m.group(1).strip())
            if len(raw) > 10:
                meta["judges"] = raw[:300]

        # ── Clerks ──
        clerk_m = re.search(
            r"(?:Gerichtsschreiber(?:in)?|Greffièr?e?|Cancellier[ea])"
            r"\s+([\w][\w\s\-]{2,40}?)(?:\.|,|\n)",
            text, re.IGNORECASE,
        )
        if clerk_m:
            meta["clerks"] = clerk_m.group(1).strip()

        # ── Title / Subject ──
        for pattern in [
            r"(?:Gegenstand|Objet|Oggetto)\s*:?\s*\n?\s*(.*?)"
            r"(?:\n\s*\n|Beschwerde|Recours|Ricorso)",
            r"(?:Gegenstand|Objet|Oggetto)\s*:?\s*(.*?)(?:\n|;)",
        ]:
            m = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
            if m:
                title = re.sub(r"\s+", " ", m.group(1).strip())
                if 10 < len(title) < 500:
                    meta["title"] = title
                    break

        # ── Legal area (Rechtsgebiet) — may come from search results stub ──

        # ── Outcome (from Dispositiv — last 2000 chars) ──
        dispositiv_text = text[-2000:].lower()
        for pattern, label in OUTCOME_PATTERNS:
            if pattern in dispositiv_text:
                meta["outcome"] = label
                break

        # ── Regeste (BGE Leitentscheide) ──
        regeste_m = re.search(
            r"(?:Regeste|Regesto)\s*(?:\([^)]*\))?\s*:?\s*\n"
            r"(.*?)"
            r"(?:\nSachverhalt|\nFaits|\nFatti|\nAus den Erwägungen|\nExtrait)",
            text, re.DOTALL,
        )
        if regeste_m:
            meta["regeste"] = regeste_m.group(1).strip()[:3000]

        # ── BGE reference ──
        bge_m = re.search(
            r"\b((?:BGE|ATF|DTF)\s+\d{1,3}\s+[IV]+[a-z]?\s+\d+)\b", text
        )
        if bge_m:
            meta["bge_reference"] = bge_m.group(1)

        return meta

    # ═══════════════════════════════════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════════════════════════════════

    # BGer docket — new format: 6B_1234/2025, 12T_1/2020
    # Also matches space-separated from search results: "1C 372/2024"
    DOCKET_RE = re.compile(r"\b(\d{1,2}[A-Z][_ ]\d+/\d{4})\b")
    # Old format (pre-BGG, before 2007): 6S.123/2005
    DOCKET_OLD_RE = re.compile(r"\b(\d[A-Z]\.\d+/\d{4})\b")

    def _extract_docket(self, text: str) -> str | None:
        """Extract a BGer docket number from text or URL.
        
        Normalizes space-separated dockets (from search results) to
        underscore format: '1C 372/2024' → '1C_372/2024'
        """
        for pattern in [self.DOCKET_RE, self.DOCKET_OLD_RE]:
            m = pattern.search(text)
            if m:
                # Normalize spaces to underscores for consistent IDs
                return m.group(1).replace(" ", "_")
        return None

    def _make_jump_url(self, stub: dict) -> str | None:
        """Build decision URL.
        
        Primary: Direct relevancy.bger.ch URL (confirmed working format from live testing).
        Fallback: JumpCGI URL (may redirect but works for older decisions).
        """
        d = self._resolve_date(stub.get("decision_date"))
        if not d:
            return None
        # Direct format (confirmed: http://relevancy.bger.ch/php/aza/http/index.php?
        #   highlight_docid=aza://DD-MM-YYYY-DOCKET_DASHED&lang=de&type=show_document)
        docket_dashed = stub["docket_number"].replace("/", "-")
        return (
            f"http://relevancy.bger.ch/php/aza/http/index.php?"
            f"highlight_docid=aza://{d.strftime('%d-%m-%Y')}-{docket_dashed}"
            f"&lang=de&type=show_document"
        )

    def _docket_to_abteilung(self, docket: str, lang: str = "de") -> str | None:
        """Map docket prefix to Abteilung name."""
        m = re.match(r"(\d{1,2}[A-Z])", docket)
        if not m:
            return None
        prefix = m.group(1)
        result = PREFIX_TO_ABTEILUNG.get(prefix)
        if not result:
            return None
        _, info = result
        return info.get(lang, info["de"])

    def _detect_language(self, text: str) -> str:
        """
        Detect language using word-frequency method.

        Uses common word lists for de/fr/it detection
        (pipelines.py line 524). Count matches of common function words
        for each language; highest count wins.
        """
        sample = text[:5000]
        scores = {
            lang: len(pattern.findall(sample))
            for lang, pattern in _LANG_WORDS.items()
        }
        return max(scores, key=scores.get)  # type: ignore

    def _resolve_date(self, val) -> date | None:
        """Coerce a date value from str or date."""
        if isinstance(val, date):
            return val
        if isinstance(val, str):
            for fmt in ["%d.%m.%Y", "%Y-%m-%d"]:
                try:
                    return datetime.strptime(val.strip(), fmt).date()
                except ValueError:
                    continue
        return None

    @staticmethod
    def _parse_rss_date(s: str) -> date | None:
        """Parse RSS pubDate (RFC 822 format)."""
        try:
            dt = datetime.strptime(s.strip(), "%a, %d %b %Y %H:%M:%S %z")
            return dt.date()
        except ValueError:
            pass
        m = re.search(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", s)
        if m:
            month_map = {
                "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
            }
            try:
                return date(int(m.group(3)), month_map[m.group(2)], int(m.group(1)))
            except (ValueError, KeyError):
                pass
        return None

    def _abs_url(self, href: str) -> str:
        """Ensure URL is absolute."""
        if href.startswith("http"):
            return href
        return urljoin("https://search.bger.ch/", href)

    @staticmethod
    def _clean_text(text: str) -> str:
        """Normalize whitespace in extracted text."""
        lines = [line.strip() for line in text.split("\n")]
        text = "\n".join(lines)
        return re.sub(r"\n{3,}", "\n\n", text).strip()

    # ═══════════════════════════════════════════════════════════════════════
    # ENTSCHEIDSUCHE-COMPATIBLE SIGNATURE
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def make_external_signature(docket: str, decision_date: date) -> str:
        """
        Generate external cross-reference signature.

        Format:
        {spider_name}/{Signatur}_{num_sanitized[:20]}_{EDatum}

        Signature format from CSV: CH_BGer_{year}_{docket_normalized}
        e.g. "1C_607/2025" + 2025 -> "CH_BGer_2025_1C_607_2025"
        """
        normalized = docket.replace("/", "_").replace(".", "_")
        return f"CH_BGer_{decision_date.year}_{normalized}"


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Scrape BGer decisions")
    parser.add_argument(
        "--since", type=str,
        help="Backfill from YYYY-MM-DD (default: last 14 days)",
    )
    parser.add_argument("--max", type=int, help="Max decisions to scrape")
    parser.add_argument("--output", type=str, default="output")
    parser.add_argument("--state-dir", type=str, default="state")
    args = parser.parse_args()

    since = datetime.strptime(args.since, "%Y-%m-%d").date() if args.since else None

    scraper = BgerScraper(state_dir=Path(args.state_dir))
    decisions = scraper.run(since_date=since, max_decisions=args.max)

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"bger_{date.today().isoformat()}.jsonl"

    with open(out_file, "w") as f:
        for d in decisions:
            f.write(d.model_dump_json() + "\n")

    scraper.mark_run_complete(decisions)
    print(f"Scraped {len(decisions)} decisions -> {out_file}")

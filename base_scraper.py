"""
Abstract base class for all court scrapers.

Provides:
- Rate limiting (configurable delay between requests)
- State management (tracks already-scraped decision IDs)
- HTTP session with retry logic
- Proof-of-Work mining for BGer Eurospider
- Language detection
- Date normalization
- Common logging and error handling

Base scraper with rate limiting, state management, and PoW support.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from models import Decision

logger = logging.getLogger(__name__)


def _redact_proxy_url(proxy_url: str) -> str:
    """Mask proxy credentials before logging."""
    try:
        parsed = urlsplit(proxy_url)
        if "@" not in parsed.netloc:
            return proxy_url

        creds, host = parsed.netloc.rsplit("@", 1)
        user = creds.split(":", 1)[0]
        safe_creds = f"{user}:***" if ":" in creds else "***"
        return urlunsplit(
            (parsed.scheme, f"{safe_creds}@{host}", parsed.path, parsed.query, parsed.fragment)
        )
    except Exception:
        return "<redacted>"


# ============================================================
# State management
# ============================================================


class ScraperState:
    """
    Tracks which decisions have already been scraped.
    Uses a simple JSONL file (one decision_id per line).
    Fast for membership checks (loaded into a set), append-only writes.
    """

    def __init__(self, state_file: Path):
        self.state_file = state_file
        self._seen: set[str] = set()
        self._load()

    def _load(self) -> None:
        if self.state_file.exists():
            with open(self.state_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self._seen.add(line)
        logger.info(f"Loaded {len(self._seen)} known decision IDs from {self.state_file}")

    def is_known(self, decision_id: str) -> bool:
        return decision_id in self._seen

    def mark_scraped(self, decision_id: str) -> None:
        if decision_id not in self._seen:
            self._seen.add(decision_id)
            with open(self.state_file, "a") as f:
                f.write(decision_id + "\n")

    def count(self) -> int:
        return len(self._seen)


# ============================================================
# Proof-of-Work mining for BGer Eurospider
# ============================================================


def _has_leading_zero_bits(hash_bytes: bytes, difficulty: int) -> bool:
    """Check if a SHA-256 hash has the required number of leading zero bits."""
    full_bytes = difficulty // 8
    remaining_bits = difficulty % 8

    for i in range(full_bytes):
        if hash_bytes[i] != 0:
            return False

    if remaining_bits > 0:
        mask = (0xFF << (8 - remaining_bits)) & 0xFF
        if (hash_bytes[full_bytes] & mask) != 0:
            return False

    return True


def mine_pow(data: str, difficulty: int = 16, start_nonce: int = 0) -> dict:
    """
    Mine a Proof-of-Work nonce for BGer Eurospider anti-scraping.

    Algorithm:
    1. Generate fingerprint = SHA-256 of random bytes
    2. Find nonce where SHA-256(fingerprint + str(nonce)) has `difficulty` leading zero bits
    3. Return {hash, nonce, data}

    With difficulty=16, typically requires ~65,536 hashes (<1 second).

    Args:
        data: The fingerprint/challenge string.
        difficulty: Number of leading zero bits required (default 16).
        start_nonce: Starting nonce value (default 0).

    Returns:
        Dict with 'hash' (hex string) and 'nonce' (int).
    """
    nonce = start_nonce
    while True:
        candidate = f"{data}{nonce}".encode()
        h = hashlib.sha256(candidate).digest()
        if _has_leading_zero_bits(h, difficulty):
            return {"hash": h.hex(), "nonce": nonce}
        nonce += 1
        if nonce % 500_000 == 0:
            logger.debug(f"PoW mining: {nonce} hashes tried...")


def make_pow_cookies(difficulty: int = 16) -> dict[str, str]:
    """
    Generate a complete set of PoW cookies for BGer Eurospider.

    Optionally encrypts the fingerprint using AES-CBC with the public key
    from BGer's client-side JavaScript (CH_BGer.py).

    Returns:
        Dict of cookie name -> cookie value.
    """
    fingerprint = hashlib.sha256(os.urandom(32)).hexdigest()
    result = mine_pow(fingerprint, difficulty)

    cookies = {
        "powData": fingerprint,
        "powDifficulty": str(difficulty),
        "powHash": result["hash"],
        "powNonce": str(result["nonce"]),
    }

    # Optional AES-CBC encryption of powData
    # Key extracted from BGer's publicly served JavaScript (not a secret)
    # This is optional — basic PoW cookies work without it
    try:
        from Crypto.Cipher import AES
        from Crypto.Util.Padding import pad
        import base64

        key = bytes.fromhex("9f3c1a8e7b4d62f1e0b5c47a2d8f93bc")
        iv = os.urandom(16)
        cipher = AES.new(key, AES.MODE_CBC, iv)
        encrypted = cipher.encrypt(pad(fingerprint.encode(), AES.block_size))
        cookies["powData"] = base64.b64encode(iv + encrypted).decode()
    except ImportError:
        # PyCryptodome not installed — use unencrypted PoW (still works)
        pass

    return cookies


# ============================================================
# Base scraper class
# ============================================================


class BaseScraper(ABC):
    """
    Abstract base for all court scrapers.

    Subclasses must implement:
    - court_code: property returning the court identifier
    - discover_new(since_date) -> Iterator of decision stubs/URLs
    - fetch_decision(stub) -> Decision
    """

    # Default delay between HTTP requests (seconds).
    # Override in subclass for court-specific rate limits.
    REQUEST_DELAY: float = 2.0

    # HTTP timeout in seconds
    TIMEOUT: int = 30

    # User agent — be transparent
    USER_AGENT: str = (
        "SwissCaselawBot/1.0 (https://github.com/jonashertner/caselaw-repo; "
        "legal research; respects rate limits)"
    )

    # Maximum consecutive errors before stopping
    MAX_ERRORS: int = 20

    # SOCKS5/HTTP proxy URL (e.g. "socks5h://127.0.0.1:1080")
    # Set per-scraper or via environment variable SCRAPER_PROXY
    PROXY: str = ""

    def __init__(self, state_dir: Path = Path("state")):
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state = ScraperState(self.state_dir / f"{self.court_code}.jsonl")
        self.session = self._build_session()
        self._last_request_time: float = 0

    def _build_session(self) -> requests.Session:
        """Build an HTTP session with retry logic and proper headers."""
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": self.USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "de-CH,de;q=0.9,fr;q=0.8,it;q=0.7,en;q=0.5",
            }
        )

        # Proxy support: class attribute > environment variable
        proxy = self.PROXY or os.environ.get("SCRAPER_PROXY", "")
        if proxy:
            session.proxies = {"http": proxy, "https": proxy}
            logger.info(f"[{self.court_code}] Using proxy: {_redact_proxy_url(proxy)}")

        retry = Retry(
            total=3,
            backoff_factor=2,  # 2s, 4s, 8s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _rate_limit(self) -> None:
        """Enforce minimum delay between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.REQUEST_DELAY:
            sleep_time = self.REQUEST_DELAY - elapsed
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def get(self, url: str, **kwargs) -> requests.Response:
        """Rate-limited GET request."""
        self._rate_limit()
        kwargs.setdefault("timeout", self.TIMEOUT)
        logger.debug(f"GET {url}")
        response = self.session.get(url, **kwargs)
        response.raise_for_status()
        return response

    def post(self, url: str, **kwargs) -> requests.Response:
        """Rate-limited POST request."""
        self._rate_limit()
        kwargs.setdefault("timeout", self.TIMEOUT)
        logger.debug(f"POST {url}")
        response = self.session.post(url, **kwargs)
        response.raise_for_status()
        return response

    @property
    @abstractmethod
    def court_code(self) -> str:
        """Unique court identifier (e.g., 'bger', 'bvger', 'zh_obergericht')."""
        ...

    @abstractmethod
    def discover_new(self, since_date=None) -> Iterator[dict]:
        """
        Discover new decisions that haven't been scraped yet.

        Yields dicts with at least:
        - 'docket_number': str
        - 'decision_date': str or date
        - 'url': str (URL to fetch full text)
        - Any other metadata available from the listing

        Implementations should check self.state.is_known() and skip known IDs.
        """
        ...

    @abstractmethod
    def fetch_decision(self, stub: dict) -> Decision | None:
        """
        Fetch the full text and metadata of a single decision.

        Args:
            stub: Dict from discover_new() with at least docket_number, date, url.

        Returns:
            Decision object, or None if fetch failed (logged, not raised).
        """
        ...

    def run(self, since_date=None, max_decisions: int | None = None) -> list[Decision]:
        """
        Main entry point: discover and fetch new decisions.

        Args:
            since_date: Only look at decisions from this date onward.
            max_decisions: Stop after this many new decisions (for testing/quota).

        Returns:
            List of newly scraped Decision objects.
        """
        new_decisions = []
        errors = 0

        logger.info(
            f"[{self.court_code}] Starting scrape. "
            f"Known decisions: {self.state.count()}"
        )

        for i, stub in enumerate(self.discover_new(since_date)):
            if max_decisions and len(new_decisions) >= max_decisions:
                logger.info(f"[{self.court_code}] Reached max_decisions={max_decisions}")
                break

            try:
                decision = self.fetch_decision(stub)
                if decision:
                    self.state.mark_scraped(decision.decision_id)
                    new_decisions.append(decision)
                    logger.info(
                        f"[{self.court_code}] Scraped: {decision.decision_id} "
                        f"({decision.decision_date})"
                    )
            except Exception as e:
                errors += 1
                logger.error(
                    f"[{self.court_code}] Error scraping {stub.get('docket_number', '?')}: {e}",
                    exc_info=True,
                )
                if errors > self.MAX_ERRORS:
                    logger.error(f"[{self.court_code}] Too many errors ({errors}), stopping.")
                    break

        logger.info(
            f"[{self.court_code}] Done. "
            f"New: {len(new_decisions)}, Errors: {errors}, "
            f"Total known: {self.state.count()}"
        )

        return new_decisions

    # =======================================================================
    # Common utilities
    # =======================================================================

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean up extracted text: normalize whitespace, remove artifacts."""
        text = re.sub(r"\n{3,}", "\n\n", text)
        lines = [line.strip() for line in text.split("\n")]
        text = "\n".join(lines)
        return text.strip()

    @staticmethod
    def normalize_url(href: str, base: str) -> str:
        """Ensure URL is absolute."""
        from urllib.parse import urljoin
        return urljoin(base, href)

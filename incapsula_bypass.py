"""
Incapsula/Imperva WAF bypass via Playwright cookie harvesting.

bger.ch and search.bger.ch are behind Imperva's CDN which serves a JS challenge.
Plain `requests` can't execute JS, so the challenge fails and we get a 212-byte
stub page. Playwright runs a real Chromium browser that solves the challenge,
and we extract the resulting cookies for use with requests.Session.

Cookie lifecycle:
  - Incapsula cookies (visid_incap_*, incap_ses_*) typically last 20-30 min
  - We cache them to disk and refresh when a request detects expiry
  - PoW cookies (powData, powHash, etc.) are separate and still needed

Usage:
    from incapsula_bypass import IncapsulaCookieManager

    mgr = IncapsulaCookieManager(cache_dir=Path("state"))
    cookies = mgr.get_cookies("https://www.bger.ch")
    session.cookies.update(cookies)

Dependencies:
    pip install playwright
    playwright install chromium --with-deps
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Lazy import — Playwright is only needed when cookies must be refreshed
_playwright_available: Optional[bool] = None


def _check_playwright() -> bool:
    global _playwright_available
    if _playwright_available is None:
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
            _playwright_available = True
        except ImportError:
            _playwright_available = False
            logger.warning(
                "playwright not installed. Run: "
                "pip install playwright && playwright install chromium --with-deps"
            )
    return _playwright_available


# ═══════════════════════════════════════════════════════════════════════════════
# DOMAIN CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

# Known Incapsula-protected domains and their seed URLs
PROTECTED_DOMAINS = {
    "www.bger.ch": {
        "seed_url": "https://www.bger.ch/ext/eurospider/live/de/php/aza/http/index.php",
        "success_marker": "Eurospider",  # text present on real page
        "fail_markers": ["Incapsula", "_Incapsula_Resource", "robots"],
    },
    "search.bger.ch": {
        "seed_url": "https://search.bger.ch/ext/eurospider/live/de/php/clir/http/index_atf.php?lang=de",
        "success_marker": "Eurospider",
        "fail_markers": ["Incapsula", "_Incapsula_Resource", "robots"],
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# COOKIE HARVESTER
# ═══════════════════════════════════════════════════════════════════════════════


def harvest_cookies(
    url: str,
    timeout_seconds: int = 45,
    headless: bool = True,
) -> dict[str, str]:
    """
    Launch Chromium via Playwright, navigate to URL, wait for Incapsula
    JS challenge to resolve, and extract cookies.

    Args:
        url: Target URL (must be on an Incapsula-protected domain)
        timeout_seconds: Max time to wait for challenge resolution
        headless: Run browser headlessly (set False for debugging)

    Returns:
        Dict of cookie name -> value for the domain

    Raises:
        RuntimeError: If Playwright not available or challenge not solved
    """
    if not _check_playwright():
        raise RuntimeError(
            "playwright not installed. "
            "pip install playwright && playwright install chromium --with-deps"
        )

    from playwright.sync_api import sync_playwright

    logger.info(f"Harvesting Incapsula cookies for {url}")
    t0 = time.time()

    with sync_playwright() as p:
        # Launch with realistic browser fingerprint
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) "
                "Gecko/20100101 Firefox/141.0"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="de-CH",
            timezone_id="Europe/Zurich",
        )

        # Remove webdriver indicator
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        page = context.new_page()

        try:
            # Navigate — Incapsula will serve JS challenge first
            page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Poll until real content appears or timeout
            deadline = time.time() + timeout_seconds
            solved = False

            while time.time() < deadline:
                content = page.content()

                # Check if we've passed the challenge
                has_fail = any(
                    marker in content
                    for marker in ["_Incapsula_Resource", "robots"]
                    if marker  # skip empty
                )

                # Real page is usually >1000 bytes and lacks Incapsula markers
                if len(content) > 1000 and not has_fail:
                    solved = True
                    logger.info(
                        f"Incapsula challenge solved in {time.time()-t0:.1f}s "
                        f"(page size: {len(content)} chars)"
                    )
                    break

                # Also check if we got redirected to the actual content
                if "Eurospider" in content or "eurospider" in content.lower():
                    solved = True
                    logger.info(f"Eurospider content detected in {time.time()-t0:.1f}s")
                    break

                time.sleep(1)

            if not solved:
                # One more attempt — wait for networkidle
                try:
                    page.wait_for_load_state("networkidle", timeout=10000)
                    content = page.content()
                    if len(content) > 1000:
                        solved = True
                        logger.info("Solved after networkidle wait")
                except Exception:
                    pass

            if not solved:
                logger.warning(
                    f"Incapsula challenge may not be fully solved "
                    f"(page size: {len(page.content())} chars after {timeout_seconds}s). "
                    f"Extracting cookies anyway."
                )

            # Extract all cookies for this domain
            cookies = context.cookies()
            cookie_dict = {}
            for c in cookies:
                cookie_dict[c["name"]] = c["value"]

            elapsed = time.time() - t0
            incap_cookies = [
                k for k in cookie_dict
                if k.startswith("visid_incap") or k.startswith("incap_ses")
                or k.startswith("nlbi_") or k.startswith("_Incap")
            ]
            logger.info(
                f"Harvested {len(cookie_dict)} cookies "
                f"({len(incap_cookies)} Incapsula) in {elapsed:.1f}s"
            )

            return cookie_dict

        finally:
            browser.close()


# ═══════════════════════════════════════════════════════════════════════════════
# COOKIE MANAGER (with caching and auto-refresh)
# ═══════════════════════════════════════════════════════════════════════════════


class IncapsulaCookieManager:
    """
    Manages Incapsula cookies with disk caching and automatic refresh.

    Cookies are cached per-domain in the state directory as JSON files.
    When cookies expire (detected by Incapsula response or age), they
    are re-harvested automatically.

    Usage:
        mgr = IncapsulaCookieManager(cache_dir=Path("state"))

        # Get fresh cookies (from cache or by harvesting)
        cookies = mgr.get_cookies("www.bger.ch")

        # Apply to requests.Session
        session.cookies.update(cookies)

        # After a request, check if we need to refresh
        if mgr.is_incapsula_blocked(response):
            cookies = mgr.refresh_cookies("www.bger.ch")
            session.cookies.update(cookies)
    """

    # Cookies are good for ~20 minutes typically, but be conservative
    COOKIE_MAX_AGE_SECONDS = 900  # 15 minutes

    def __init__(self, cache_dir: Path = Path("state"), headless: bool = True):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.headless = headless
        self._cookies: dict[str, dict[str, str]] = {}
        self._timestamps: dict[str, float] = {}

    def _cache_path(self, domain: str) -> Path:
        safe_domain = domain.replace(".", "_")
        return self.cache_dir / f"incapsula_{safe_domain}.json"

    def _load_cache(self, domain: str) -> Optional[dict[str, str]]:
        """Load cached cookies from disk if still fresh."""
        path = self._cache_path(domain)
        if not path.exists():
            return None

        try:
            data = json.loads(path.read_text())
            timestamp = data.get("timestamp", 0)
            age = time.time() - timestamp

            if age > self.COOKIE_MAX_AGE_SECONDS:
                logger.info(
                    f"Cached cookies for {domain} expired "
                    f"({age:.0f}s > {self.COOKIE_MAX_AGE_SECONDS}s)"
                )
                return None

            cookies = data.get("cookies", {})
            logger.info(
                f"Loaded {len(cookies)} cached cookies for {domain} "
                f"(age: {age:.0f}s)"
            )
            self._cookies[domain] = cookies
            self._timestamps[domain] = timestamp
            return cookies

        except Exception as e:
            logger.warning(f"Failed to load cookie cache for {domain}: {e}")
            return None

    def _save_cache(self, domain: str, cookies: dict[str, str]) -> None:
        """Save cookies to disk cache."""
        path = self._cache_path(domain)
        timestamp = time.time()
        data = {
            "domain": domain,
            "timestamp": timestamp,
            "cookies": cookies,
        }
        path.write_text(json.dumps(data, indent=2))
        self._cookies[domain] = cookies
        self._timestamps[domain] = timestamp
        logger.debug(f"Cached {len(cookies)} cookies for {domain}")

    def get_cookies(self, domain: str) -> dict[str, str]:
        """
        Get cookies for a domain. Returns cached if fresh, harvests if not.

        Args:
            domain: e.g. "www.bger.ch" or "search.bger.ch"

        Returns:
            Dict of cookie name -> value
        """
        # Try in-memory cache first
        if domain in self._cookies:
            age = time.time() - self._timestamps.get(domain, 0)
            if age < self.COOKIE_MAX_AGE_SECONDS:
                return self._cookies[domain]

        # Try disk cache
        cached = self._load_cache(domain)
        if cached:
            return cached

        # Harvest fresh cookies
        return self.refresh_cookies(domain)

    def refresh_cookies(self, domain: str) -> dict[str, str]:
        """
        Force-refresh cookies for a domain via Playwright.

        Args:
            domain: e.g. "www.bger.ch"

        Returns:
            Fresh cookie dict
        """
        config = PROTECTED_DOMAINS.get(domain)
        if config is None:
            raise ValueError(
                f"Unknown domain: {domain}. "
                f"Known: {list(PROTECTED_DOMAINS.keys())}"
            )

        url = config["seed_url"]
        cookies = harvest_cookies(url, headless=self.headless)
        self._save_cache(domain, cookies)
        return cookies

    @staticmethod
    def is_incapsula_blocked(response_text: str) -> bool:
        """
        Check if a response is an Incapsula block page.

        Args:
            response_text: The response body text

        Returns:
            True if the response appears to be an Incapsula challenge/block
        """
        if len(response_text) < 500:
            markers = ["_Incapsula_Resource", "Incapsula", "robots"]
            return any(m in response_text for m in markers)
        return False

    @staticmethod
    def is_incapsula_blocked_response(response) -> bool:
        """
        Check if a requests.Response is an Incapsula block.

        Args:
            response: requests.Response object

        Returns:
            True if blocked
        """
        return IncapsulaCookieManager.is_incapsula_blocked(response.text)


# ═══════════════════════════════════════════════════════════════════════════════
# CLI for testing
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    domain = sys.argv[1] if len(sys.argv) > 1 else "www.bger.ch"

    if domain not in PROTECTED_DOMAINS:
        # Treat as full URL
        url = domain
        cookies = harvest_cookies(url, headless=True)
    else:
        mgr = IncapsulaCookieManager(cache_dir=Path("state"))
        cookies = mgr.get_cookies(domain)

    print(f"\n{'='*60}")
    print(f"Cookies for {domain}:")
    print(f"{'='*60}")
    for name, value in sorted(cookies.items()):
        print(f"  {name}: {value[:60]}{'...' if len(value)>60 else ''}")

    # Verify with plain requests
    import requests as req
    config = PROTECTED_DOMAINS.get(domain, {"seed_url": domain})
    url = config["seed_url"] if isinstance(config, dict) else domain
    r = req.get(url, cookies=cookies, timeout=15)
    print(f"\nVerification: GET {url}")
    print(f"  Status: {r.status_code}, Length: {len(r.text)}")
    print(f"  Blocked: {IncapsulaCookieManager.is_incapsula_blocked(r.text)}")
    if "Eurospider" in r.text or "eurospider" in r.text.lower():
        print("  ✅ SUCCESS — Eurospider content reached!")
    else:
        print(f"  ⚠️  Content preview: {r.text[:300]}")
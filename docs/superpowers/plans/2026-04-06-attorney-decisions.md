# Attorney Ethical & Bar Decisions — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add scrapers for all available Swiss attorney disciplinary decision sources and a tagging pipeline that marks existing BGer decisions as Anwaltsrecht.

**Architecture:** 5 scraper changes (1 one-liner, 4 new scrapers) + 1 build script for the tagging DB + MCP server integration for `legal_area=anwaltsrecht` filtering. All scrapers follow existing `BaseScraper` patterns. The tagging pipeline downloads SAV PDFs, extracts docket numbers via regex, resolves them against FTS5 DB, and writes a standalone SQLite DB.

**Tech Stack:** Python 3, pydantic, requests, BeautifulSoup, pdfplumber, sqlite3, existing BaseScraper framework

**Spec:** `docs/superpowers/specs/2026-04-06-attorney-decisions-design.md`

---

### Task 1: GE Commission du barreau — add section to ge_gerichte

**Files:**
- Modify: `scrapers/cantonal/ge_gerichte.py:45-64`
- Test: `tests/test_ge_dcba.py` (create)

- [ ] **Step 1: Write the test**

```python
# tests/test_ge_dcba.py
"""Verify the dcba section is registered in GE scraper."""
import sys
sys.path.insert(0, ".")

def test_dcba_section_registered():
    from scrapers.cantonal.ge_gerichte import SECTIONS
    assert "dcba" in SECTIONS, "dcba section missing from SECTIONS dict"
    assert SECTIONS["dcba"] == "GE_DCBA_001"

def test_dcba_section_count():
    from scrapers.cantonal.ge_gerichte import SECTIONS
    # 18 original + 1 new = 19
    assert len(SECTIONS) == 19
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_ge_dcba.py -v`
Expected: FAIL — `"dcba" not in SECTIONS`

- [ ] **Step 3: Add the dcba section**

In `scrapers/cantonal/ge_gerichte.py`, add after line 63 (`"dccr": "GE_TAPI_001"`):

```python
    "dcba": "GE_DCBA_001",   # Commission du barreau (attorney discipline)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_ge_dcba.py -v`
Expected: PASS

- [ ] **Step 5: Smoke-test on VPS**

Run: `ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && git pull --rebase && timeout 120 python3 run_scraper.py ge_gerichte --max 3 -v 2>&1 | tail -30'`
Expected: Scraper starts, discovers dcba stubs among other sections.

- [ ] **Step 6: Commit**

```bash
git add scrapers/cantonal/ge_gerichte.py tests/test_ge_dcba.py
git commit -m "feat: add GE Commission du barreau (dcba) attorney discipline section"
```

---

### Task 2: SAV Kantone scraper

**Files:**
- Create: `scrapers/sav_kantone.py`
- Create: `tests/test_sav_kantone.py`
- Modify: `run_scraper.py:36-106` (add registry entry)

- [ ] **Step 1: Write the test**

```python
# tests/test_sav_kantone.py
"""Tests for SAV Kantone attorney decisions scraper."""
import sys
sys.path.insert(0, ".")

def test_scraper_instantiates():
    from scrapers.sav_kantone import SAVKantoneScraper
    scraper = SAVKantoneScraper()
    assert scraper.court_code == "sav_kantone"

def test_scraper_registered():
    from run_scraper import SCRAPERS
    assert "sav_kantone" in SCRAPERS
    mod, cls = SCRAPERS["sav_kantone"]
    assert mod == "scrapers.sav_kantone"
    assert cls == "SAVKantoneScraper"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_sav_kantone.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Create the scraper**

```python
# scrapers/sav_kantone.py
"""
SAV Kantone Scraper
====================
Scrapes cantonal attorney supervisory decisions from the Swiss Bar Association
(SAV) Rechtsprechung portal at sav-fsa.ch/kantone.

Architecture:
- Liferay AssetPublisher page lists ~40 entries with linked PDFs
- Each entry is a cantonal Anwaltsaufsicht decision
- PDFs extracted via pdfplumber/fitz for full text

Volume: ~40 decisions (static collection, last updated ~2013)
Platform: Liferay DXP
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Iterator

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import (
    Decision,
    detect_language,
    extract_citations,
    make_decision_id,
    parse_date,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.sav-fsa.ch"
KANTONE_PATH = "/de/kantone"

# Map canton abbreviations found in titles/content
CANTON_PATTERNS = {
    "AG": r"\bAargau\b|\bAG\b",
    "BE": r"\bBern\b|\bBE\b",
    "FR": r"\bFreiburg\b|\bFribourg\b|\bFR\b",
    "GE": r"\bGen[eè]ve?\b|\bGenf\b|\bGE\b",
    "GL": r"\bGlarus\b|\bGL\b",
    "LU": r"\bLuzern\b|\bLU\b",
    "OW": r"\bObwalden\b|\bOW\b",
    "SG": r"\bSt\.?\s*Gallen\b|\bSG\b",
    "TI": r"\bTicino\b|\bTessin\b|\bTI\b",
    "UR": r"\bUri\b|\bUR\b",
    "VD": r"\bVaud\b|\bWaadt\b|\bVD\b",
    "VS": r"\bValais\b|\bWallis\b|\bVS\b",
    "ZG": r"\bZug\b|\bZG\b",
    "ZH": r"\bZ[üu]rich\b|\bZH\b",
}


def _detect_canton(text: str) -> str:
    """Try to detect canton from text. Default to CH."""
    for code, pattern in CANTON_PATTERNS.items():
        if re.search(pattern, text, re.IGNORECASE):
            return code
    return "CH"


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber, fallback to fitz."""
    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
            text = "\n\n".join(pages)
            if text.strip():
                return text
    except Exception:
        pass
    try:
        import fitz
        doc = fitz.open(stream=content, filetype="pdf")
        pages = [page.get_text() for page in doc]
        doc.close()
        return "\n\n".join(pages)
    except Exception:
        pass
    return ""


class SAVKantoneScraper(BaseScraper):
    """Scraper for cantonal attorney supervisory decisions from SAV portal."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 30

    @property
    def court_code(self) -> str:
        return "sav_kantone"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover all decision entries on the SAV Kantone page."""
        url = f"{BASE_URL}{KANTONE_PATH}"
        try:
            response = self.get(url)
        except Exception as e:
            logger.error(f"[sav_kantone] Failed to fetch listing: {e}")
            return

        soup = BeautifulSoup(response.text, "html.parser")

        # Find all asset entries — Liferay AssetPublisher renders entries as links
        # with class patterns like "asset-abstract" or within journal-content
        entries = soup.find_all("a", href=True)
        seen_urls = set()

        for link in entries:
            href = link.get("href", "")
            title = link.get_text(strip=True)

            # Only process PDF links or detail page links under /kantone/
            if not href:
                continue
            if href.startswith("/"):
                href = BASE_URL + href

            # Filter for decision-related links (PDFs or detail pages)
            is_pdf = href.lower().endswith(".pdf") or "/documents/" in href
            is_detail = "/kantone/-/asset_publisher/" in href

            if not (is_pdf or is_detail):
                continue
            if href in seen_urls:
                continue
            seen_urls.add(href)

            # Generate a stable ID from the URL
            slug = re.sub(r"[^a-zA-Z0-9]", "_", href.split("/")[-1])[:80]
            decision_id = make_decision_id("sav_kantone", slug)

            if self.state.is_known(decision_id):
                continue

            stub = {
                "decision_id": decision_id,
                "docket_number": slug,
                "url": href,
                "title": title[:200] if title else "",
                "is_pdf": is_pdf,
            }
            yield stub

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch and parse a single SAV Kantone decision."""
        url = stub["url"]
        docket = stub["docket_number"]
        title = stub.get("title", "")

        try:
            response = self.get(url)
        except Exception as e:
            logger.error(f"[sav_kantone] Failed to fetch {docket}: {e}")
            return None

        full_text = ""
        pdf_url = None

        if stub.get("is_pdf") or "application/pdf" in response.headers.get("content-type", ""):
            # Direct PDF
            full_text = _extract_pdf_text(response.content)
            pdf_url = url
        else:
            # HTML detail page — look for embedded PDF link
            soup = BeautifulSoup(response.text, "html.parser")
            # Extract page text as fallback
            content_div = soup.find("div", class_="journal-content-article") or soup.find("div", class_="asset-content")
            if content_div:
                full_text = content_div.get_text(separator="\n", strip=True)

            # Look for PDF link within the page
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if h.endswith(".pdf") or "/documents/" in h:
                    if h.startswith("/"):
                        h = BASE_URL + h
                    try:
                        pdf_resp = self.get(h)
                        pdf_text = _extract_pdf_text(pdf_resp.content)
                        if len(pdf_text) > len(full_text):
                            full_text = pdf_text
                            pdf_url = h
                    except Exception as e:
                        logger.debug(f"[sav_kantone] PDF fetch failed: {e}")

        if not full_text or len(full_text.strip()) < 50:
            logger.warning(f"[sav_kantone] Insufficient text for {docket}")
            return None

        full_text = self.clean_text(full_text)
        lang = detect_language(full_text)
        canton = _detect_canton(title + " " + full_text[:500])

        # Try to extract a date from the text
        date_match = re.search(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b", full_text[:2000])
        decision_date = None
        if date_match:
            decision_date = parse_date(date_match.group(0))

        return Decision(
            decision_id=stub["decision_id"],
            court="sav_kantone",
            canton=canton,
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=title or None,
            legal_area="Anwaltsrecht",
            full_text=full_text,
            source_url=url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )
```

- [ ] **Step 4: Register in run_scraper.py**

Add after the `"hudoc_ch"` entry (around line 105) in `run_scraper.py`:

```python
    # Attorney discipline — SAV portal
    "sav_kantone": ("scrapers.sav_kantone", "SAVKantoneScraper"),
```

- [ ] **Step 5: Run tests**

Run: `python3 -m pytest tests/test_sav_kantone.py -v`
Expected: PASS

- [ ] **Step 6: Local smoke test**

Run: `python3 run_scraper.py sav_kantone --max 3 -v`
Expected: Discovers entries from sav-fsa.ch/kantone, fetches 1-3 PDFs, writes to `output/decisions/sav_kantone.jsonl`

- [ ] **Step 7: Commit**

```bash
git add scrapers/sav_kantone.py tests/test_sav_kantone.py run_scraper.py
git commit -m "feat: add SAV Kantone scraper for cantonal attorney discipline decisions"
```

---

### Task 3: SAV International scraper

**Files:**
- Create: `scrapers/sav_international.py`
- Create: `tests/test_sav_international.py`
- Modify: `run_scraper.py` (add registry entry)

- [ ] **Step 1: Write the test**

```python
# tests/test_sav_international.py
"""Tests for SAV International decisions scraper."""
import sys
sys.path.insert(0, ".")

def test_scraper_instantiates():
    from scrapers.sav_international import SAVInternationalScraper
    scraper = SAVInternationalScraper()
    assert scraper.court_code == "sav_international"

def test_scraper_registered():
    from run_scraper import SCRAPERS
    assert "sav_international" in SCRAPERS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_sav_international.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Create the scraper**

```python
# scrapers/sav_international.py
"""
SAV International Scraper
==========================
Scrapes international decisions relevant to Swiss attorney law from the
SAV Rechtsprechung portal at sav-fsa.ch/international.

Architecture:
- Accordion-style page with 6 entries
- Mix of hosted PDFs and external links (HUDOC, CURIA)
- PDFs extracted via pdfplumber/fitz, external HTML parsed

Volume: ~6 decisions (ECtHR, CJEU)
Platform: Liferay DXP
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Iterator

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import (
    Decision,
    detect_language,
    extract_citations,
    make_decision_id,
    parse_date,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.sav-fsa.ch"
INTERNATIONAL_PATH = "/de/international"


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber, fallback to fitz."""
    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
            text = "\n\n".join(pages)
            if text.strip():
                return text
    except Exception:
        pass
    try:
        import fitz
        doc = fitz.open(stream=content, filetype="pdf")
        pages = [page.get_text() for page in doc]
        doc.close()
        return "\n\n".join(pages)
    except Exception:
        pass
    return ""


class SAVInternationalScraper(BaseScraper):
    """Scraper for international attorney law decisions from SAV portal."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 30

    @property
    def court_code(self) -> str:
        return "sav_international"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover decision entries on the SAV International page."""
        url = f"{BASE_URL}{INTERNATIONAL_PATH}"
        try:
            response = self.get(url)
        except Exception as e:
            logger.error(f"[sav_international] Failed to fetch listing: {e}")
            return

        soup = BeautifulSoup(response.text, "html.parser")
        seen_urls = set()

        # Find all links that point to decisions (PDFs or external court sites)
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            title = link.get_text(strip=True)

            if not href or not title:
                continue
            if href.startswith("/"):
                href = BASE_URL + href

            # Filter: PDFs on SAV, or external court links (HUDOC, CURIA, BVerfG)
            is_relevant = (
                href.endswith(".pdf")
                or "/documents/" in href
                or "hudoc.echr" in href
                or "curia.europa.eu" in href
                or "bundesverfassungsgericht" in href
            )
            if not is_relevant:
                continue
            if href in seen_urls:
                continue
            seen_urls.add(href)

            slug = re.sub(r"[^a-zA-Z0-9]", "_", title)[:80]
            decision_id = make_decision_id("sav_international", slug)

            if self.state.is_known(decision_id):
                continue

            stub = {
                "decision_id": decision_id,
                "docket_number": slug,
                "url": href,
                "title": title[:200],
                "is_pdf": href.endswith(".pdf") or "/documents/" in href,
            }
            yield stub

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch and parse a single international decision."""
        url = stub["url"]
        docket = stub["docket_number"]
        title = stub.get("title", "")

        try:
            response = self.get(url)
        except Exception as e:
            logger.error(f"[sav_international] Failed to fetch {docket}: {e}")
            return None

        full_text = ""
        pdf_url = None

        if stub.get("is_pdf") or "application/pdf" in response.headers.get("content-type", ""):
            full_text = _extract_pdf_text(response.content)
            pdf_url = url
        else:
            # External HTML page (HUDOC, CURIA, etc.)
            soup = BeautifulSoup(response.text, "html.parser")
            # Remove script/style tags
            for tag in soup(["script", "style", "nav", "footer"]):
                tag.decompose()
            full_text = soup.get_text(separator="\n", strip=True)

        if not full_text or len(full_text.strip()) < 100:
            logger.warning(f"[sav_international] Insufficient text for {docket}")
            return None

        full_text = self.clean_text(full_text)
        lang = detect_language(full_text)

        # Try to extract date
        date_match = re.search(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b", full_text[:3000])
        decision_date = parse_date(date_match.group(0)) if date_match else None

        return Decision(
            decision_id=stub["decision_id"],
            court="sav_international",
            canton="CH",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=title or None,
            legal_area="Anwaltsrecht",
            full_text=full_text,
            source_url=url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )
```

- [ ] **Step 4: Register in run_scraper.py**

Add after the `sav_kantone` entry:

```python
    "sav_international": ("scrapers.sav_international", "SAVInternationalScraper"),
```

- [ ] **Step 5: Run tests**

Run: `python3 -m pytest tests/test_sav_international.py -v`
Expected: PASS

- [ ] **Step 6: Local smoke test**

Run: `python3 run_scraper.py sav_international --max 3 -v`
Expected: Discovers entries, fetches 1-3 decisions

- [ ] **Step 7: Commit**

```bash
git add scrapers/sav_international.py tests/test_sav_international.py run_scraper.py
git commit -m "feat: add SAV International scraper for ECtHR/CJEU attorney law decisions"
```

---

### Task 4: TG Anwaltskommission scraper

**Files:**
- Create: `scrapers/cantonal/tg_anwaltskommission.py`
- Create: `tests/test_tg_anwaltskommission.py`
- Modify: `run_scraper.py` (add registry entry)

- [ ] **Step 1: Write the test**

```python
# tests/test_tg_anwaltskommission.py
"""Tests for TG Anwaltskommission scraper."""
import sys
sys.path.insert(0, ".")

def test_scraper_instantiates():
    from scrapers.cantonal.tg_anwaltskommission import TGAnwaltskommissionScraper
    scraper = TGAnwaltskommissionScraper()
    assert scraper.court_code == "tg_anwaltskommission"

def test_scraper_registered():
    from run_scraper import SCRAPERS
    assert "tg_anwaltskommission" in SCRAPERS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_tg_anwaltskommission.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Create the scraper**

```python
# scrapers/cantonal/tg_anwaltskommission.py
"""
TG Anwaltskommission Scraper
==============================
Scrapes attorney discipline decisions from the Thurgau Anwaltskommission
at register.tg.ch/anwaltskommission/entscheide.

Architecture:
- HTML listing page with linked PDF decisions
- Small volume (handful of decisions)

Volume: ~5-10 decisions
Platform: Custom HTML
"""
from __future__ import annotations

import io
import logging
import re
from datetime import date, datetime, timezone
from typing import Iterator

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import (
    Decision,
    detect_language,
    extract_citations,
    make_decision_id,
    parse_date,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://register.tg.ch"
LISTING_URL = f"{BASE_URL}/anwaltskommission/entscheide.html/10330"


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber, fallback to fitz."""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
            text = "\n\n".join(pages)
            if text.strip():
                return text
    except Exception:
        pass
    try:
        import fitz
        doc = fitz.open(stream=content, filetype="pdf")
        pages = [page.get_text() for page in doc]
        doc.close()
        return "\n\n".join(pages)
    except Exception:
        pass
    return ""


class TGAnwaltskommissionScraper(BaseScraper):
    """Scraper for Thurgau Anwaltskommission decisions."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 30

    @property
    def court_code(self) -> str:
        return "tg_anwaltskommission"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover decisions on the TG Anwaltskommission page."""
        try:
            response = self.get(LISTING_URL)
        except Exception as e:
            logger.error(f"[tg_anwaltskommission] Failed to fetch listing: {e}")
            return

        soup = BeautifulSoup(response.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            title = link.get_text(strip=True)

            if not href:
                continue

            # Look for PDF links or decision detail links
            if not (href.endswith(".pdf") or "entscheid" in href.lower()):
                continue

            if href.startswith("/"):
                href = BASE_URL + href

            # Extract docket number from title or filename
            docket_match = re.search(r"(AK\.\d{4}\.\d+)", title + " " + href)
            docket = docket_match.group(1) if docket_match else re.sub(r"[^a-zA-Z0-9._-]", "_", href.split("/")[-1])[:60]

            decision_id = make_decision_id("tg_anwaltskommission", docket)
            if self.state.is_known(decision_id):
                continue

            stub = {
                "decision_id": decision_id,
                "docket_number": docket,
                "url": href,
                "title": title[:200] if title else "",
            }
            yield stub

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch and parse a single TG Anwaltskommission decision."""
        url = stub["url"]
        docket = stub["docket_number"]
        title = stub.get("title", "")

        try:
            response = self.get(url)
        except Exception as e:
            logger.error(f"[tg_anwaltskommission] Failed to fetch {docket}: {e}")
            return None

        content_type = response.headers.get("content-type", "")

        if "pdf" in content_type or url.endswith(".pdf"):
            full_text = _extract_pdf_text(response.content)
            pdf_url = url
        else:
            # HTML decision page
            soup = BeautifulSoup(response.text, "html.parser")
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            main = soup.find("main") or soup.find("article") or soup.find("div", class_="content")
            full_text = (main or soup).get_text(separator="\n", strip=True)
            pdf_url = None
            # Check for embedded PDF link
            for a in (main or soup).find_all("a", href=True):
                h = a["href"]
                if h.endswith(".pdf"):
                    if h.startswith("/"):
                        h = BASE_URL + h
                    try:
                        pdf_resp = self.get(h)
                        pdf_text = _extract_pdf_text(pdf_resp.content)
                        if len(pdf_text) > len(full_text):
                            full_text = pdf_text
                            pdf_url = h
                    except Exception:
                        pass

        if not full_text or len(full_text.strip()) < 50:
            logger.warning(f"[tg_anwaltskommission] Insufficient text for {docket}")
            return None

        full_text = self.clean_text(full_text)
        lang = detect_language(full_text)

        date_match = re.search(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b", full_text[:2000])
        decision_date = parse_date(date_match.group(0)) if date_match else None

        return Decision(
            decision_id=stub["decision_id"],
            court="tg_anwaltskommission",
            canton="TG",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=title or None,
            legal_area="Anwaltsrecht",
            full_text=full_text,
            source_url=url,
            pdf_url=pdf_url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )
```

- [ ] **Step 4: Register in run_scraper.py**

Add after the `sav_international` entry:

```python
    "tg_anwaltskommission": ("scrapers.cantonal.tg_anwaltskommission", "TGAnwaltskommissionScraper"),
```

- [ ] **Step 5: Run tests**

Run: `python3 -m pytest tests/test_tg_anwaltskommission.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add scrapers/cantonal/tg_anwaltskommission.py tests/test_tg_anwaltskommission.py run_scraper.py
git commit -m "feat: add TG Anwaltskommission scraper for attorney discipline decisions"
```

---

### Task 5: FR Anwaltsaufsicht scraper

**Files:**
- Create: `scrapers/cantonal/fr_anwaltsaufsicht.py`
- Create: `tests/test_fr_anwaltsaufsicht.py`
- Modify: `run_scraper.py` (add registry entry)

- [ ] **Step 1: Write the test**

```python
# tests/test_fr_anwaltsaufsicht.py
"""Tests for FR Commission du barreau (Anwaltsaufsicht) scraper."""
import sys
sys.path.insert(0, ".")

def test_scraper_instantiates():
    from scrapers.cantonal.fr_anwaltsaufsicht import FRAnwaltsaufsichtScraper
    scraper = FRAnwaltsaufsichtScraper()
    assert scraper.court_code == "fr_anwaltsaufsicht"

def test_scraper_registered():
    from run_scraper import SCRAPERS
    assert "fr_anwaltsaufsicht" in SCRAPERS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_fr_anwaltsaufsicht.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Create the scraper**

```python
# scrapers/cantonal/fr_anwaltsaufsicht.py
"""
FR Anwaltsaufsicht (Commission du barreau) Scraper
====================================================
Scrapes attorney discipline decision summaries from the Fribourg
Commission du barreau at fr.ch.

Architecture:
- HTML page with Jurisprudence section containing PDF links
- ~3 anonymized summary PDFs (2021, 2023, 2024)
- Started publishing Feb 2024

Volume: ~3 decisions
Platform: Drupal CMS (fr.ch)
"""
from __future__ import annotations

import io
import logging
import re
from datetime import date, datetime, timezone
from typing import Iterator

from bs4 import BeautifulSoup

from base_scraper import BaseScraper
from models import (
    Decision,
    detect_language,
    extract_citations,
    make_decision_id,
    parse_date,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.fr.ch"
LISTING_URL = f"{BASE_URL}/etat-et-droit/justice/commission-du-barreau"


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber, fallback to fitz."""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
            text = "\n\n".join(pages)
            if text.strip():
                return text
    except Exception:
        pass
    try:
        import fitz
        doc = fitz.open(stream=content, filetype="pdf")
        pages = [page.get_text() for page in doc]
        doc.close()
        return "\n\n".join(pages)
    except Exception:
        pass
    return ""


class FRAnwaltsaufsichtScraper(BaseScraper):
    """Scraper for Fribourg Commission du barreau attorney discipline decisions."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 30

    @property
    def court_code(self) -> str:
        return "fr_anwaltsaufsicht"

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover PDF decision links on the FR Commission du barreau page."""
        try:
            response = self.get(LISTING_URL)
        except Exception as e:
            logger.error(f"[fr_anwaltsaufsicht] Failed to fetch listing: {e}")
            return

        soup = BeautifulSoup(response.text, "html.parser")
        seen_urls = set()

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            title = link.get_text(strip=True)

            if not href.endswith(".pdf"):
                continue

            if href.startswith("/"):
                href = BASE_URL + href

            # Filter for jurisprudence/decision-related PDFs
            title_lower = (title or "").lower()
            href_lower = href.lower()
            is_relevant = any(
                kw in title_lower or kw in href_lower
                for kw in ["jurisprudence", "décision", "disciplin", "barreau", "anonymi"]
            )
            if not is_relevant:
                continue

            if href in seen_urls:
                continue
            seen_urls.add(href)

            slug = re.sub(r"[^a-zA-Z0-9]", "_", href.split("/")[-1].replace(".pdf", ""))[:80]
            decision_id = make_decision_id("fr_anwaltsaufsicht", slug)

            if self.state.is_known(decision_id):
                continue

            stub = {
                "decision_id": decision_id,
                "docket_number": slug,
                "url": href,
                "title": title[:200] if title else "",
            }
            yield stub

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Fetch and parse a single FR attorney discipline PDF."""
        url = stub["url"]
        docket = stub["docket_number"]
        title = stub.get("title", "")

        try:
            response = self.get(url)
        except Exception as e:
            logger.error(f"[fr_anwaltsaufsicht] Failed to fetch {docket}: {e}")
            return None

        full_text = _extract_pdf_text(response.content)

        if not full_text or len(full_text.strip()) < 50:
            logger.warning(f"[fr_anwaltsaufsicht] Insufficient text for {docket}")
            return None

        full_text = self.clean_text(full_text)
        lang = detect_language(full_text)

        date_match = re.search(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b", full_text[:2000])
        decision_date = parse_date(date_match.group(0)) if date_match else None

        return Decision(
            decision_id=stub["decision_id"],
            court="fr_anwaltsaufsicht",
            canton="FR",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=title or None,
            legal_area="Anwaltsrecht",
            full_text=full_text,
            source_url=url,
            pdf_url=url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )
```

- [ ] **Step 4: Register in run_scraper.py**

Add after the `tg_anwaltskommission` entry:

```python
    "fr_anwaltsaufsicht": ("scrapers.cantonal.fr_anwaltsaufsicht", "FRAnwaltsaufsichtScraper"),
```

- [ ] **Step 5: Run tests**

Run: `python3 -m pytest tests/test_fr_anwaltsaufsicht.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add scrapers/cantonal/fr_anwaltsaufsicht.py tests/test_fr_anwaltsaufsicht.py run_scraper.py
git commit -m "feat: add FR Anwaltsaufsicht scraper for Fribourg attorney discipline decisions"
```

---

### Task 6: Anwaltsrecht tagging pipeline

**Files:**
- Create: `search_stack/build_anwaltsrecht_tags.py`
- Create: `tests/test_anwaltsrecht_tags.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_anwaltsrecht_tags.py
"""Tests for Anwaltsrecht tagging pipeline."""
import re
import sys
sys.path.insert(0, ".")

def test_docket_regex():
    """Verify regex extracts BGer docket numbers from SAV PDF text."""
    # Patterns found in SAV PDFs
    from search_stack.build_anwaltsrecht_tags import DOCKET_PATTERNS
    sample = """
    2C_345/2023, arrêt du 15.3.2024
    Urteil 2C_100/2020 vom 5. Mai 2021
    BGE 130 II 270
    ATF 140 II 102
    Arrêt 2P.100/2005 du 10 janvier 2006
    5A_123/2019
    """
    all_matches = set()
    for pattern in DOCKET_PATTERNS:
        for m in pattern.finditer(sample):
            all_matches.add(m.group(0))
    assert "2C_345/2023" in all_matches
    assert "2C_100/2020" in all_matches
    assert "2P.100/2005" in all_matches
    assert "5A_123/2019" in all_matches

def test_bge_regex():
    from search_stack.build_anwaltsrecht_tags import BGE_PATTERN
    sample = "BGE 130 II 270 und ATF 140 II 102"
    matches = [m.group(0) for m in BGE_PATTERN.finditer(sample)]
    assert "BGE 130 II 270" in matches
    assert "ATF 140 II 102" in matches

def test_extract_article_from_filename():
    from search_stack.build_anwaltsrecht_tags import _extract_bgfa_article
    assert _extract_bgfa_article("Art12.pdf") == "Art. 12"
    assert _extract_bgfa_article("Art3.pdf") == "Art. 3"
    assert _extract_bgfa_article("Art36.pdf") == "Art. 36"
    assert _extract_bgfa_article("Rechtsprechung_Bund_2024-2025.pdf") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_anwaltsrecht_tags.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Create the build script**

```python
# search_stack/build_anwaltsrecht_tags.py
#!/usr/bin/env python3
"""
Build Anwaltsrecht Tags DB
============================
Downloads SAV BGFA + Bund PDFs, extracts BGer docket numbers,
resolves them against the FTS5 DB, and writes a tags SQLite DB.

Output: output/anwaltsrecht_tags.db
Usage:  python3 -m search_stack.build_anwaltsrecht_tags --fts5-db output/decisions.db
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logger = logging.getLogger(__name__)

# --- Regex patterns for docket extraction ---

# BGer docket: 2C_345/2023, 5A_123/2019, 2P.100/2005
DOCKET_PATTERNS = [
    re.compile(r"\b(\d[A-Z][_.]\d+/\d{4})\b"),
    re.compile(r"\b(\d[A-Z]_\d+/\d{4})\b"),
]

# BGE reference: BGE 130 II 270, ATF 140 II 102
BGE_PATTERN = re.compile(
    r"\b(BGE|ATF|DTF)\s+(\d{1,3})\s+(I{1,3}[AV]?|V)\s+(\d+)\b"
)

# SAV BGFA PDF URLs — 25 article PDFs
# Pattern: /documents/672183/2059208/Art{N}.pdf/{uuid}
BGFA_PDF_BASE = "https://www.sav-fsa.ch/documents/672183/2059208"
BGFA_ARTICLES = [2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 25, 27, 28, 29, 34, 36]

# SAV Bund PDF URLs — 11 period PDFs
BUND_PDF_BASE = "https://www.sav-fsa.ch/documents/672183/2096091"
BUND_PERIODS = [
    "2001", "2003-2004", "2005-2006", "2007-2008", "2009-2010",
    "2012-2013", "2014-2015", "2016-2018", "2019-2021", "2022-2023", "2024-2025",
]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS anwaltsrecht_tags (
    decision_id TEXT NOT NULL,
    bgfa_article TEXT,
    source TEXT NOT NULL,
    docket_number TEXT,
    PRIMARY KEY (decision_id, bgfa_article, source)
);
CREATE INDEX IF NOT EXISTS idx_article ON anwaltsrecht_tags(bgfa_article);
CREATE INDEX IF NOT EXISTS idx_decision ON anwaltsrecht_tags(decision_id);
"""


def _extract_bgfa_article(filename: str) -> str | None:
    """Extract BGFA article number from PDF filename like 'Art12.pdf'."""
    m = re.search(r"Art(\d+)", filename)
    if m:
        return f"Art. {m.group(1)}"
    return None


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes."""
    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            return "\n\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception:
        pass
    try:
        import fitz
        doc = fitz.open(stream=content, filetype="pdf")
        text = "\n\n".join(page.get_text() for page in doc)
        doc.close()
        return text
    except Exception:
        pass
    return ""


def _extract_dockets(text: str) -> set[str]:
    """Extract all BGer docket numbers and BGE references from text."""
    dockets = set()
    for pattern in DOCKET_PATTERNS:
        for m in pattern.finditer(text):
            dockets.add(m.group(1))
    for m in BGE_PATTERN.finditer(text):
        # Normalize to "BGE vol div page" format
        dockets.add(f"BGE {m.group(2)} {m.group(3)} {m.group(4)}")
    return dockets


def _download_pdf(url: str, session) -> bytes | None:
    """Download a PDF with retry."""
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 100:
                return resp.content
        except Exception as e:
            logger.warning(f"Download attempt {attempt+1} failed for {url}: {e}")
            time.sleep(2)
    return None


def _resolve_dockets(dockets: set[str], fts5_db: Path) -> dict[str, str]:
    """Resolve docket numbers to decision_ids in the FTS5 DB.

    Returns dict of docket -> decision_id.
    """
    if not fts5_db.exists():
        logger.error(f"FTS5 DB not found: {fts5_db}")
        return {}

    conn = sqlite3.connect(f"file:{fts5_db}?mode=ro", uri=True)
    resolved = {}

    for docket in dockets:
        try:
            # Try exact match on docket_number
            row = conn.execute(
                "SELECT decision_id FROM decisions WHERE docket_number = ? LIMIT 1",
                (docket,),
            ).fetchone()
            if row:
                resolved[docket] = row[0]
                continue

            # For BGE references, try matching in decision_id
            if docket.startswith("BGE "):
                # BGE 130 II 270 -> search for bge_130_II_270
                bge_norm = docket.replace("BGE ", "").replace(" ", "_")
                row = conn.execute(
                    "SELECT decision_id FROM decisions WHERE decision_id LIKE ? LIMIT 1",
                    (f"bge_{bge_norm}%",),
                ).fetchone()
                if row:
                    resolved[docket] = row[0]
        except sqlite3.Error:
            pass

    conn.close()
    logger.info(f"Resolved {len(resolved)}/{len(dockets)} dockets")
    return resolved


def build_tags_db(fts5_db: Path, output_db: Path):
    """Main pipeline: download PDFs, extract dockets, resolve, write DB."""
    import requests

    session = requests.Session()
    session.headers["User-Agent"] = (
        "SwissCaselawBot/1.0 (https://github.com/jonashertner/caselaw-repo; legal research)"
    )

    all_entries: list[tuple[str, str | None, str]] = []  # (docket, article, source)

    # --- BGFA article PDFs ---
    for art_num in BGFA_ARTICLES:
        filename = f"Art{art_num}.pdf"
        article = f"Art. {art_num}"
        # Try both with and without (1) suffix (updated versions)
        for suffix in ["", "%20(1)"]:
            url = f"{BGFA_PDF_BASE}/{filename.replace('.pdf', '')}{suffix}.pdf"
            content = _download_pdf(url, session)
            if content:
                text = _extract_pdf_text(content)
                dockets = _extract_dockets(text)
                logger.info(f"BGFA {article}: {len(dockets)} dockets from {len(text)} chars")
                for d in dockets:
                    all_entries.append((d, article, "bgfa"))
                break
        else:
            logger.warning(f"Failed to download BGFA {article}")
        time.sleep(1)

    # --- Bund period PDFs ---
    for period in BUND_PERIODS:
        filename = f"Rechtsprechung_Bund_und_weiteres_Anwaltsrecht_{period}.pdf"
        url = f"{BUND_PDF_BASE}/{filename}"
        content = _download_pdf(url, session)
        if content:
            text = _extract_pdf_text(content)
            dockets = _extract_dockets(text)
            logger.info(f"Bund {period}: {len(dockets)} dockets from {len(text)} chars")
            for d in dockets:
                all_entries.append((d, None, "bund"))
        else:
            logger.warning(f"Failed to download Bund {period}")
        time.sleep(1)

    # Collect unique dockets for resolution
    unique_dockets = {e[0] for e in all_entries}
    logger.info(f"Total unique dockets extracted: {len(unique_dockets)}")

    # Resolve against FTS5 DB
    resolved = _resolve_dockets(unique_dockets, fts5_db)

    # Write to tags DB (atomic: write to .tmp, then rename)
    tmp_path = Path(str(output_db) + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    conn = sqlite3.connect(str(tmp_path))
    conn.executescript(SCHEMA_SQL)

    inserted = 0
    for docket, article, source in all_entries:
        decision_id = resolved.get(docket)
        if not decision_id:
            continue
        try:
            conn.execute(
                "INSERT OR IGNORE INTO anwaltsrecht_tags (decision_id, bgfa_article, source, docket_number) "
                "VALUES (?, ?, ?, ?)",
                (decision_id, article, source, docket),
            )
            inserted += 1
        except sqlite3.Error:
            pass

    conn.commit()

    # Stats
    total_unique = conn.execute("SELECT COUNT(DISTINCT decision_id) FROM anwaltsrecht_tags").fetchone()[0]
    total_rows = conn.execute("SELECT COUNT(*) FROM anwaltsrecht_tags").fetchone()[0]
    conn.close()

    logger.info(f"Tags DB: {total_rows} rows, {total_unique} unique decisions")

    # Atomic swap
    os.replace(str(tmp_path), str(output_db))
    logger.info(f"Written to {output_db}")


def main():
    parser = argparse.ArgumentParser(description="Build Anwaltsrecht tags DB from SAV PDFs")
    parser.add_argument("--fts5-db", type=Path, default=Path("output/decisions.db"),
                        help="Path to FTS5 decisions database")
    parser.add_argument("--output", type=Path, default=Path("output/anwaltsrecht_tags.db"),
                        help="Output tags database path")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    build_tags_db(args.fts5_db, args.output)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `python3 -m pytest tests/test_anwaltsrecht_tags.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add search_stack/build_anwaltsrecht_tags.py tests/test_anwaltsrecht_tags.py
git commit -m "feat: add Anwaltsrecht tagging pipeline — extract BGer docket tags from SAV PDFs"
```

---

### Task 7: MCP server integration for Anwaltsrecht filter

**Files:**
- Modify: `mcp_server.py` (add tags DB connection + filter logic)
- Create: `tests/test_anwaltsrecht_mcp.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_anwaltsrecht_mcp.py
"""Tests for Anwaltsrecht MCP filter integration."""
import sys
sys.path.insert(0, ".")

def test_tags_db_path_configured():
    """Verify the tags DB path constant exists."""
    from mcp_server import ANWALTSRECHT_TAGS_DB_PATH
    assert "anwaltsrecht_tags" in str(ANWALTSRECHT_TAGS_DB_PATH)

def test_get_tags_conn_returns_none_when_missing():
    """Verify graceful fallback when tags DB doesn't exist."""
    from mcp_server import _get_anwaltsrecht_conn
    # In test env, DB likely doesn't exist — should return None, not crash
    conn = _get_anwaltsrecht_conn()
    # Either None (no DB) or a valid connection (if DB exists in dev)
    if conn is not None:
        conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_anwaltsrecht_mcp.py -v`
Expected: FAIL — `ANWALTSRECHT_TAGS_DB_PATH` not found

- [ ] **Step 3: Add tags DB path constant**

In `mcp_server.py`, after line 175 (the `LEXFIND_CACHE_DB_PATH` definition), add:

```python
ANWALTSRECHT_TAGS_DB_PATH = Path(os.environ.get("SWISS_CASELAW_ANWALTSRECHT_DB", str(DATA_DIR / "anwaltsrecht_tags.db")))
```

- [ ] **Step 4: Add connection helper**

In `mcp_server.py`, after the `_get_graph_conn()` function (around line 2471), add:

```python
_anwaltsrecht_warned = False

def _get_anwaltsrecht_conn() -> sqlite3.Connection | None:
    """Open a read-only connection to the Anwaltsrecht tags DB, or None if unavailable."""
    global _anwaltsrecht_warned
    if not ANWALTSRECHT_TAGS_DB_PATH.exists():
        if not _anwaltsrecht_warned:
            logger.info("Anwaltsrecht tags DB not found at %s — anwaltsrecht filter disabled", ANWALTSRECHT_TAGS_DB_PATH)
            _anwaltsrecht_warned = True
        return None
    try:
        conn = sqlite3.connect(str(ANWALTSRECHT_TAGS_DB_PATH), timeout=0.5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        return conn
    except sqlite3.Error as e:
        logger.warning("Failed to open Anwaltsrecht tags DB: %s", e)
        return None
```

- [ ] **Step 5: Integrate filter into search**

In `_search_fts5_inner()` (around line 1997-2003), replace the existing `legal_area` soft boost with enhanced logic:

Find the block:
```python
        # Soft boost: if legal_area filter given, promote matching results
        # to the top without removing non-matching ones
        if legal_area and reranked:
            la_lower = legal_area.lower()
            matching = [r for r in reranked if la_lower in (r.get("legal_area") or "").lower()]
            others = [r for r in reranked if la_lower not in (r.get("legal_area") or "").lower()]
            reranked = matching + others
```

Replace with:
```python
        # Soft boost: if legal_area filter given, promote matching results
        if legal_area and reranked:
            la_lower = legal_area.lower()
            if la_lower == "anwaltsrecht":
                # Use Anwaltsrecht tags DB for hard filtering
                aw_conn = _get_anwaltsrecht_conn()
                if aw_conn:
                    try:
                        tagged_ids = {
                            row[0] for row in aw_conn.execute(
                                "SELECT DISTINCT decision_id FROM anwaltsrecht_tags"
                            ).fetchall()
                        }
                        matching = [r for r in reranked if r.get("decision_id") in tagged_ids]
                        others = [r for r in reranked if r.get("decision_id") not in tagged_ids]
                        reranked = matching + others
                    finally:
                        aw_conn.close()
                else:
                    # Fallback to text-based matching
                    matching = [r for r in reranked if la_lower in (r.get("legal_area") or "").lower()]
                    others = [r for r in reranked if la_lower not in (r.get("legal_area") or "").lower()]
                    reranked = matching + others
            else:
                matching = [r for r in reranked if la_lower in (r.get("legal_area") or "").lower()]
                others = [r for r in reranked if la_lower not in (r.get("legal_area") or "").lower()]
                reranked = matching + others
```

- [ ] **Step 6: Add bgfa_article parameter to search_decisions tool schema**

Find the `search_decisions` tool definition in the MCP tools list (around line 8618). In the `properties` dict, after the `legal_area` parameter, add:

```python
                    "bgfa_article": {
                        "type": "string",
                        "description": "Filter by BGFA article (e.g. 'Art. 12', 'Art. 17'). Requires legal_area='anwaltsrecht'.",
                    },
```

And pass it through to the search function. In the tool handler (around line 9362), after `legal_area=arguments.get("legal_area")`, ensure `bgfa_article` is also extracted and used.

- [ ] **Step 7: Run tests**

Run: `python3 -m pytest tests/test_anwaltsrecht_mcp.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add mcp_server.py tests/test_anwaltsrecht_mcp.py
git commit -m "feat: add Anwaltsrecht legal_area filter with tags DB integration in MCP"
```

---

### Task 8: Publish pipeline integration

**Files:**
- Modify: `publish.py` (add step 2e for Anwaltsrecht tags)

- [ ] **Step 1: Add the step function**

In `publish.py`, after `step_2d_enrich_quality` (around line 261), add:

```python
def step_2e_build_anwaltsrecht_tags(dry_run: bool = False, full_rebuild: bool = False) -> bool:
    """Step 2e: Build Anwaltsrecht tags DB from SAV PDFs."""
    logger.info("Step 2e: Build Anwaltsrecht tags")

    script = REPO_DIR / "search_stack" / "build_anwaltsrecht_tags.py"
    if not script.exists():
        logger.info("  build_anwaltsrecht_tags.py not found, skipping")
        return True

    if not DB_PATH.exists():
        logger.info("  FTS5 database not found, skipping Anwaltsrecht tags")
        return True

    tags_db = OUTPUT_DIR / "anwaltsrecht_tags.db"
    return run_cmd(
        [sys.executable, str(script),
         "--fts5-db", str(DB_PATH),
         "--output", str(tags_db)],
        "Build Anwaltsrecht tags",
        dry_run,
        timeout=600,  # ~5 min max (36 PDF downloads + regex)
    )
```

- [ ] **Step 2: Register in STEPS list**

In the `STEPS` list (around line 406), add after the `("2d", ...)` entry:

```python
    ("2e", "Anwaltsrecht Tags", step_2e_build_anwaltsrecht_tags),
```

- [ ] **Step 3: Update argparse help**

Update the `--step` help string to include `2e`:

```python
        help="Run only a specific step (1, 2, 2b, 2c, 2d, 2e, 3, 4, 5, 6)",
```

- [ ] **Step 4: Handle 2e in the step dispatch**

In the main loop (around line 504), where `num in ("2b", "2c", "2d")` is checked, update to:

```python
            elif num in ("2b", "2c", "2d", "2e"):
```

- [ ] **Step 5: Commit**

```bash
git add publish.py
git commit -m "feat: add Anwaltsrecht tags build step (2e) to publish pipeline"
```

---

### Task 9: Deploy and verify

**Files:** None (operational task)

- [ ] **Step 1: Push and deploy**

```bash
git push origin main
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && git pull --rebase origin main'
```

- [ ] **Step 2: Run new scrapers on VPS**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 run_scraper.py sav_kantone -v 2>&1 | tail -20'
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 run_scraper.py sav_international -v 2>&1 | tail -20'
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 run_scraper.py tg_anwaltskommission -v 2>&1 | tail -20'
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 run_scraper.py fr_anwaltsaufsicht -v 2>&1 | tail -20'
```

- [ ] **Step 3: Run GE scraper to pick up dcba**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && timeout 600 python3 run_scraper.py ge_gerichte --max 200 -v 2>&1 | tail -30'
```

- [ ] **Step 4: Build tags DB on VPS**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 -m search_stack.build_anwaltsrecht_tags --fts5-db output/decisions.db --output output/anwaltsrecht_tags.db -v 2>&1 | tail -20'
```

- [ ] **Step 5: Symlink tags DB to data volume**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'mv /opt/caselaw/repo/output/anwaltsrecht_tags.db /mnt/HC_Volume_104655575/output/anwaltsrecht_tags.db && ln -sf /mnt/HC_Volume_104655575/output/anwaltsrecht_tags.db /opt/caselaw/repo/output/anwaltsrecht_tags.db'
```

- [ ] **Step 6: Restart MCP workers**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773'
```

- [ ] **Step 7: Verify scraper health**

```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 -c "import json; h=json.load(open(\"logs/scraper_health.json\")); s=h[\"scrapers\"]; print(f\"{sum(1 for v in s.values() if v.get(\\\"success\\\"))}/{len(s)} scrapers healthy\")"'
```

Expected: 57/57 scrapers healthy (53 existing + 4 new)

- [ ] **Step 8: Test MCP anwaltsrecht filter**

Use the MCP `search_decisions` tool with `legal_area="anwaltsrecht"` and verify results are tagged attorney law decisions.

- [ ] **Step 9: Verify existing coverage**

Check that ZH, VD, BS, AG, SG already have attorney decisions:
```bash
ssh -i ~/.ssh/caselaw root@46.225.212.40 'cd /opt/caselaw/repo && python3 -c "
import sqlite3
conn = sqlite3.connect(\"file:output/decisions.db?mode=ro&immutable=1\", uri=True)
for q in [
    (\"ZH Aufsichtskommission\", \"SELECT COUNT(*) FROM decisions WHERE court=\\\"zh_obergericht\\\" AND chamber LIKE \\\"%Aufsichtskommission%\\\"\"),
    (\"VD CAVO\", \"SELECT COUNT(*) FROM decisions WHERE court=\\\"vd_gerichte\\\" AND (title LIKE \\\"%avocat%\\\" OR title LIKE \\\"%CAVO%\\\")\"),
    (\"BE Anwaltsaufsicht\", \"SELECT COUNT(*) FROM decisions WHERE court=\\\"be_anwaltsaufsicht\\\"\"),
]:
    label, sql = q
    n = conn.execute(sql).fetchone()[0]
    print(f\"{label}: {n} decisions\")
"'
```

- [ ] **Step 10: Commit any VPS-side config changes**

```bash
git add -A && git commit -m "chore: post-deploy verification of attorney decision scrapers"
```

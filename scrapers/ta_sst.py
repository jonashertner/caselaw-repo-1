"""
Swiss Sports Tribunal (Schweizer Sportgericht) Scraper
=======================================================

Scrapes decisions from sportstribunal.ch via entscheidsuche.ch metadata.

Architecture:
- Entscheidsuche provides metadata + PDF URLs for ta_sst decisions
- PDFs are hosted on sportstribunal.ch/customer/files/
- This scraper reads the entscheidsuche stubs, downloads each PDF,
  extracts text, and yields proper Decision objects

Coverage: ~50 decisions (small tribunal, doping/ethics cases)
Rate limiting: 2 seconds
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

from base_scraper import BaseScraper
from models import Decision, detect_language, make_decision_id, parse_date

logger = logging.getLogger(__name__)

# Default location of entscheidsuche stubs
_DEFAULT_DECISIONS_DIR = os.environ.get(
    "SWISS_CASELAW_DIR",
    str(Path(__file__).resolve().parent.parent / "output" / "decisions"),
)


def _extract_pdf_text(data: bytes) -> str:
    """Extract text from PDF bytes using fitz (PyMuPDF) with pdfplumber fallback."""
    try:
        import fitz
        doc = fitz.open(stream=data, filetype="pdf")
        return "\n\n".join(p.get_text() for p in doc)
    except ImportError:
        pass
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n\n".join(p.extract_text() or "" for p in pdf.pages)
    except ImportError:
        pass
    return ""


class TaSSTScraper(BaseScraper):
    """Swiss Sports Tribunal scraper — enriches entscheidsuche stubs with PDF text."""

    BASE_DELAY = 2.0

    @property
    def court_code(self) -> str:
        return "ta_sst"

    def _load_stubs(self) -> list[dict]:
        """Load entscheidsuche stub records."""
        stub_file = Path(_DEFAULT_DECISIONS_DIR) / "es_ta_sst.jsonl"
        if not stub_file.exists():
            logger.warning(f"No stub file at {stub_file}")
            return []
        stubs = []
        with open(stub_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        stubs.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return stubs

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Yield stubs for decisions not yet scraped."""
        stubs = self._load_stubs()
        logger.info(f"[ta_sst] {len(stubs)} stubs found")

        for stub in stubs:
            docket = stub.get("docket_number", "")
            if not docket:
                continue

            decision_id = make_decision_id("ta_sst", docket)
            if self.state.is_known(decision_id):
                continue

            # Date filter
            if since_date and stub.get("decision_date"):
                d = parse_date(stub["decision_date"])
                if d and d < since_date:
                    continue

            pdf_url = stub.get("pdf_url") or stub.get("source_url", "")
            if not pdf_url or not pdf_url.lower().endswith(".pdf"):
                logger.debug(f"No PDF URL for {docket}, skipping")
                continue

            yield {
                "docket_number": docket,
                "decision_date": stub.get("decision_date", ""),
                "url": pdf_url,
                "source_url": stub.get("source_url", pdf_url),
                "title": stub.get("title"),
            }

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download PDF and extract text."""
        docket = stub["docket_number"]
        pdf_url = stub["url"]

        self.rate_limit()

        try:
            resp = self.session.get(pdf_url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"[ta_sst] Failed to download PDF for {docket}: {e}")
            return None

        full_text = _extract_pdf_text(resp.content)
        if not full_text or len(full_text) < 100:
            logger.warning(f"[ta_sst] PDF text too short for {docket}: {len(full_text)} chars")
            return None

        decision_date = parse_date(stub.get("decision_date", ""))
        lang = detect_language(full_text)

        return Decision(
            decision_id=make_decision_id("ta_sst", docket),
            court="ta_sst",
            canton="CH",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=stub.get("title"),
            full_text=full_text,
            source_url=stub.get("source_url", pdf_url),
            pdf_url=pdf_url,
            decision_type="Entscheid",
            scraped_at=datetime.now(timezone.utc),
        )

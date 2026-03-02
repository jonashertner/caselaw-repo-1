"""
Swiss Sports Tribunal (Schweizer Sportgericht) Scraper
=======================================================

Scrapes decisions from sportstribunal.ch via entscheidsuche.ch metadata.

Architecture:
- Entscheidsuche provides metadata + PDF URLs for ta_sst decisions
- PDFs are hosted on sportstribunal.ch/customer/files/
- This scraper reads the existing es_ta_sst.jsonl stubs, downloads each PDF,
  extracts text, and yields proper Decision objects

Coverage: ~50 decisions (small tribunal, doping/ethics cases)
Rate limiting: 2 seconds
"""

from __future__ import annotations

import io
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

from base_scraper import BaseScraper
from models import Decision, detect_language, make_decision_id, parse_date

logger = logging.getLogger(__name__)


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

    COURT = "ta_sst"
    CANTON = "CH"
    BASE_DELAY = 2.0

    def scrape(self) -> Iterator[Decision]:
        # Find the entscheidsuche stub file
        output_dir = Path(self.output_dir)
        stub_file = output_dir / "es_ta_sst.jsonl"
        if not stub_file.exists():
            logger.warning(f"No stub file found at {stub_file}")
            return

        with open(stub_file) as f:
            stubs = [json.loads(line) for line in f if line.strip()]

        logger.info(f"Found {len(stubs)} ta_sst stubs to enrich")

        for stub in stubs:
            pdf_url = stub.get("pdf_url") or stub.get("source_url")
            docket = stub.get("docket_number", "")
            decision_id = make_decision_id(self.COURT, docket)

            if self.state.is_known(decision_id):
                continue

            if not pdf_url or not pdf_url.endswith(".pdf"):
                logger.warning(f"No PDF URL for {docket}, skipping")
                continue

            self.rate_limit()

            try:
                resp = self.session.get(pdf_url, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to download PDF for {docket}: {e}")
                continue

            full_text = _extract_pdf_text(resp.content)
            if not full_text or len(full_text) < 100:
                logger.warning(f"PDF text too short for {docket}: {len(full_text)} chars")
                continue

            decision_date = None
            if stub.get("decision_date"):
                decision_date = parse_date(stub["decision_date"])

            lang = detect_language(full_text)

            yield Decision(
                decision_id=decision_id,
                court=self.COURT,
                canton=self.CANTON,
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

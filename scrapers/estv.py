"""
ESTV Kreisschreiben Scraper
===========================

Scrapes administrative guidance ("Kreisschreiben", "Rundschreiben") published by
the Swiss Federal Tax Administration (Eidgenössische Steuerverwaltung, ESTV) at
estv.admin.ch.

Kreisschreiben are the binding administrative practice of the ESTV on federal
tax law (DBG, VStG, StG, MWSTG). They are addressed to cantonal tax authorities
and trustees ("Treuhänder") and are the day-to-day reference for Swiss fiduciary
practice — material that is otherwise scattered across the ESTV site as PDFs and
not available as an open, machine-readable corpus.

Architecture (mirrors scrapers/edoeb.py — federal authority, PDF documents):
- Each tax type has a static HTML listing page of <a> items, every item wrapping
  a PDF link of the form  https://www.estv.admin.ch/dam/.../<file>.pdf
- The filename encodes the type/year/number, e.g. `dbst-ks-2025-1-032a-dvs-de.pdf`
- No JSON/API endpoint exists; we parse the listing HTML, then extract text from
  each PDF with PyMuPDF (fitz), falling back to pdfplumber.

Coverage: Kreisschreiben for direkte Bundessteuer (DBST), Verrechnungssteuer
(VST) and Stempelabgaben (STA), in all three official languages (DE/FR/IT).
MWST and Merkblätter/Rundschreiben live elsewhere — see KNOWN GAPS below.
Rate limiting: 2.0 seconds (default).
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

BASE_URL = "https://www.estv.admin.ch"

# Each tax type publishes its Kreisschreiben on one listing page per language
# (DE/FR/IT). Every page mixes Kreisschreiben with the occasional
# Merkblatt/Rundschreiben — decision_type is derived per document (see _doc_type),
# not hardcoded.
#
# KNOWN GAPS (deliberate, follow-up PRs):
#   * MWST-Praxispublikationen (MWST-Infos / MWST-Branchen-Infos) — the single
#     most-used material for Treuhänder, but published as web-based HTML behind
#     gate.estv.admin.ch, not /dam/ PDFs. Needs a separate scraper module.
#   * DBST Merkblätter/Rundschreiben/Wegleitungen on sub-pages off the
#     publications hub (the hub page itself exposes no direct PDFs).
_TAX_TYPES = [
    {
        "tax_type": "DBST",
        "legal_area": "Direkte Bundessteuer",
        "pages": {
            "de": "/de/kreisschreiben-direkten-bundessteuer",
            "fr": "/fr/circulaires-impot-federal-direct",
            "it": "/it/circolari-imposta-federale-diretta",
        },
    },
    {
        "tax_type": "VST",
        "legal_area": "Verrechnungssteuer",
        "pages": {
            "de": "/de/kreisschreiben-verrechnungssteuer",
            "fr": "/fr/circulaires-impot-anticipe",
            "it": "/it/circolari-imposta-preventiva",
        },
    },
    {
        "tax_type": "STA",
        "legal_area": "Stempelabgaben",
        "pages": {
            "de": "/de/kreisschreiben-stempelabgaben",
            "fr": "/fr/circulaires-droits-de-timbre",
            "it": "/it/circolari-tasse-di-bollo",
        },
    },
]

SOURCES = [
    {
        "url": BASE_URL + slug,
        "tax_type": t["tax_type"],
        "legal_area": t["legal_area"],
        "lang": lang,
    }
    for t in _TAX_TYPES
    for lang, slug in t["pages"].items()
]

# Filenames look like: dbst-ks-2025-1-032a-dvs-de.pdf  /  vst-ks-2024-3-050-de.pdf
# Layout: <type>-ks-<internal-year>-<internal-seq>-<REAL-KS-NUMBER>-<lang flags>.pdf
# The REAL Kreisschreiben number is the 5th segment ("032a"/"050"), NOT the seq.
_FILENAME_NUM_RE = re.compile(r"-ks-\d{4}-\d+-(\d+[a-z]?)", re.IGNORECASE)
_TITLE_NUM_RE = re.compile(r"Nr\.\s*(\d+)\s*([a-z])?\b", re.IGNORECASE)
_DATE_RE = re.compile(r"(\d{1,2}\.\d{1,2}\.\d{4})")
# Long-form date as printed in the PDF body, e.g. "12. März 2025" / "1er janvier 2024".
_LONG_DATE_RE = re.compile(
    r"\d{1,2}\.?\s*(?:er)?\s*"
    r"(?:Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember"
    r"|janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre"
    r"|gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)"
    r"\s+\d{4}",
    re.IGNORECASE,
)


def _date_from_text(text: str):
    """Fallback decision date parsed from the PDF body (first plausible date in
    the opening section, where ESTV prints the issue date). Returns a date or None."""
    head = text[:2000]
    for rx in (_LONG_DATE_RE, _DATE_RE):
        for m in rx.finditer(head):
            d = parse_date(m.group(0))
            if d:
                return d
    return None


def _ks_number(title: str, href: str) -> str | None:
    """Extract the real Kreisschreiben number (e.g. '50a'), not the internal seq.

    Prefers the human title ("Kreisschreiben Nr. 50a"); falls back to the 5th
    filename segment ("...-050a-..." -> "50a"). Returns None if neither matches.
    """
    m = _TITLE_NUM_RE.search(title)
    if m:
        return m.group(1) + (m.group(2) or "").lower()
    m = _FILENAME_NUM_RE.search(href)
    if m:
        raw = m.group(1)  # e.g. "050a"
        digits = re.match(r"0*(\d+)", raw).group(1)
        suffix = re.search(r"[a-z]$", raw, re.IGNORECASE)
        return digits + (suffix.group(0).lower() if suffix else "")
    return None


def _part_marker(title: str) -> str | None:
    """Parse an annex/FAQ marker from the title for a readable docket label.

    'Kreisschreiben Nr. 45 Anhang I-1: ...' -> 'Anhang I-1'
    'Kreisschreiben Nr. 45: Fragen und Antworten' -> 'FAQ'
    """
    m = re.search(r"Anhang\s+[\dIVXLC]+(?:-\d+)?", title, re.IGNORECASE)
    if m:
        return re.sub(r"\s+", " ", m.group(0)).strip()
    if re.search(r"Fragen und Antworten|FAQ", title, re.IGNORECASE):
        return "FAQ"
    return None


def _date_near(anchor) -> str:
    """Return the first DD.MM.YYYY found on the anchor or its nearest ancestors.

    Climbing from the anchor outward yields the tightest enclosing container that
    carries a date, which avoids picking an unrelated date from a large parent.
    """
    node = anchor
    for _ in range(4):
        if node is None:
            break
        m = _DATE_RE.search(node.get_text(" ", strip=True))
        if m:
            return m.group(1)
        node = node.parent
    return ""


def _doc_type(title: str) -> str:
    """Classify the ESTV publication type from its title (drives the MCP
    `decision_type` filter). Defaults to Kreisschreiben."""
    t = title.lower()
    if "rundschreiben" in t:
        return "Rundschreiben"
    if "merkblatt" in t:
        return "Merkblatt"
    if "wegleitung" in t:
        return "Wegleitung"
    return "Kreisschreiben"


def _extract_pdf_text(data: bytes) -> str:
    """Extract text from PDF bytes via fitz (PyMuPDF), pdfplumber fallback.

    Mirrors scrapers/edoeb.py:_extract_pdf_text. OCR fallback is intentionally
    omitted — ESTV Kreisschreiben are born-digital PDFs, never scanned.
    """
    text = ""
    try:
        import fitz

        doc = fitz.open(stream=data, filetype="pdf")
        text = "\n\n".join(p.get_text() for p in doc)
    except ImportError:
        pass

    if not text.strip():
        try:
            import io

            import pdfplumber

            with pdfplumber.open(io.BytesIO(data)) as pdf:
                text = "\n\n".join(p.extract_text() or "" for p in pdf.pages)
        except ImportError:
            pass

    return text


class ESTVScraper(BaseScraper):
    """Scraper for ESTV Kreisschreiben (federal tax administrative practice)."""

    REQUEST_DELAY = 2.0
    TIMEOUT = 60  # some Kreisschreiben PDFs are several MB

    @property
    def court_code(self) -> str:
        return "estv"

    # ---- discovery -------------------------------------------------------

    def _make_docket(self, title: str, tax_type: str, num: str | None) -> str:
        """Build a human-readable docket label, e.g. 'KS DBST Nr. 45 (Anhang I-1)'.

        Note: a docket is NOT unique — one Kreisschreiben ships as several PDFs
        (main text + Anhänge + FAQ) that share a number. Uniqueness is carried by
        decision_id (derived from the unique filename stem), not by the docket.
        """
        base = f"KS {tax_type} Nr. {num}" if num else f"KS {tax_type}"
        part = _part_marker(title)
        return f"{base} ({part})" if part else base

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Discover Kreisschreiben across all SOURCES listing pages."""
        seen_stems: set[str] = set()  # the same PDF is often linked twice per page
        for source in SOURCES:
            try:
                response = self.get(source["url"])
            except Exception as e:  # noqa: BLE001 — log and move to next source
                logger.error("[estv] Failed to fetch listing %s: %s", source["url"], e)
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            # Every Kreisschreiben is an <a> wrapping a /dam/ PDF link.
            anchors = soup.select('a[href*="/dam/"][href$=".pdf"]')
            logger.info(
                "[estv] %s/%s: %d PDF links",
                source["tax_type"],
                source["lang"],
                len(anchors),
            )

            for a in anchors:
                href = a.get("href", "")
                if not href:
                    continue
                if not href.startswith("http"):
                    href = BASE_URL + href

                title = a.get_text(separator=" ", strip=True)
                num = _ks_number(title, href)
                docket = self._make_docket(title, source["tax_type"], num)

                # Uniqueness lives on the filename stem (unique per PDF), NOT the
                # docket — one Kreisschreiben has many PDFs sharing a number.
                stem = href.rsplit("/", 1)[-1].rsplit(".", 1)[0]
                if stem in seen_stems:
                    continue
                seen_stems.add(stem)
                if self.state.is_known(make_decision_id("estv", stem)):
                    continue

                # Date sits in the item's metadata line ("PDF, 1.2 MB, 12.03.2025").
                # The DBST list carries it inside the <a>; VST/STA put it in an
                # ancestor — so climb to the nearest container that holds a date.
                date_text = _date_near(a)

                if since_date and date_text:
                    parsed = parse_date(date_text)
                    if parsed and parsed < since_date:
                        continue

                yield {
                    "docket_number": docket,
                    "stem": stem,
                    "decision_date": date_text,
                    "url": href,
                    "title": title,
                    "tax_type": source["tax_type"],
                    "legal_area": source["legal_area"],
                    "decision_type": _doc_type(title),
                    "lang": source["lang"],
                }

    # ---- fetch -----------------------------------------------------------

    def fetch_decision(self, stub: dict) -> Decision | None:
        """Download one Kreisschreiben PDF and build a Decision."""
        url = stub["url"]
        docket = stub["docket_number"]

        try:
            response = self.get(url)
        except Exception as e:  # noqa: BLE001 — logged, not raised (per contract)
            logger.error("[estv] Failed to fetch %s (%s): %s", docket, url, e)
            return None

        full_text = _extract_pdf_text(response.content)
        if not full_text.strip():
            logger.warning("[estv] Empty/unreadable PDF for %s at %s", docket, url)
            return None

        full_text = self.clean_text(full_text)
        # The source page's language is authoritative; detect_language is only a
        # fallback (short, tabular Kreisschreiben PDFs are easy to misdetect).
        lang = stub.get("lang") or detect_language(full_text)

        # Prefer the listing date; fall back to the issue date printed in the PDF
        # so year-based filtering (date_from/date_to) works for every document.
        decision_date = parse_date(stub.get("decision_date", "")) or _date_from_text(
            full_text
        )

        # No reliable headnote in an ESTV PDF (the first lines are the EFD/ESTV
        # letterhead). Leave regeste empty rather than polluting the heavily
        # BM25-weighted regeste field with boilerplate; the title carries the gist.

        decision = Decision(
            decision_id=make_decision_id("estv", stub["stem"]),
            court="estv",
            canton="CH",
            docket_number=docket,
            decision_date=decision_date,
            language=lang,
            title=stub.get("title") or docket,
            legal_area=stub.get("legal_area"),
            decision_type=stub.get("decision_type", "Kreisschreiben"),
            full_text=full_text,
            source_url=url,
            pdf_url=url,
            cited_decisions=extract_citations(full_text),
            scraped_at=datetime.now(timezone.utc),
        )
        return decision


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape ESTV Kreisschreiben")
    parser.add_argument("--since", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--max", type=int, default=5, help="Max documents")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    since = date.fromisoformat(args.since) if args.since else None
    scraper = ESTVScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  {(d.title or '')[:60]}")
    print(f"\nScraped {len(decisions)} ESTV Kreisschreiben")

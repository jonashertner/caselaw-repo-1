"""
Militärkassationsgericht (MKG) Scraper
======================================

Scrapes MKGE / ATMC / STMC decisions of the Swiss Military Court of Cassation
from the Federal Department of Defence's online archive.

Source: https://www.oa.admin.ch/de/urteile-militarkassationsgericht
Coverage: Bd. 13–16 (~66 individual decisions, 2008–2024).
Older volumes (Bd. 1–12, 1915–2007) are kept on alexandria.ch behind a
JS-rendered Primo viewer (record 12329504630001791) and are not yet ingested
— see TODO at the bottom of this file.

PDFs on oa.admin.ch are direct: each decision is one PDF, naming follows several
historical conventions:
  - Modern (Bd. 15+): "MKGE 16 Nr. 1.pdf"
  - Old, full meta:  "mkge13nr.36nr.843vom08.02.2013.pdf"  (Band, Nr., MKG-#, date)
  - Old, short:      "mkge13nr18.pdf" or "mkge13nr.1.pdf"
  - Old, concat:     "mkge1331.pdf" or "mkge1304d.pdf"     (Band+Nr, opt. lang)

Bundled PDFs (whole-Band volumes), trilingual side-by-side editions, registers,
and Regesten lists are skipped — they duplicate or summarise the individual
decisions, and would inflate the corpus with non-decision artefacts.

Each decision PDF carries a trilingual Regeste (DE / FR / IT) at the top,
followed by Sachverhalt + Erwägungen in the original language. Decision date
and parties are usually printed at the very end in the form
  "(936, 16. März 2024, B. gegen Militärappellationsgericht 2)".

Court is federal (canton=CH), legal area = Militärstrafrecht.
"""

from __future__ import annotations

import io
import logging
import re
from datetime import date, datetime, timezone
from typing import Iterator
from urllib.parse import unquote, urljoin

from base_scraper import BaseScraper
from models import (
    Decision,
    detect_language,
    extract_citations,
    make_decision_id,
)

logger = logging.getLogger(__name__)

INDEX_URL = "https://www.oa.admin.ch/de/urteile-militarkassationsgericht"
BASE_URL = "https://www.oa.admin.ch"

MAX_PDF_SIZE = 30 * 1024 * 1024  # 30 MB — older bundled PDFs can be larger

# Filename patterns (case-insensitive). Order matters: most-specific first.
_PAT_MODERN = re.compile(r"^MKGE\s+(\d+)\s+Nr\.?\s*(\d+)\.pdf$", re.I)
_PAT_OLD_FULL = re.compile(
    r"^mkge(\d+)nr\.?(\d+)nr\.?(\d+)vom(\d{1,2})\.(\d{1,2})\.(\d{4})\.pdf$", re.I
)
_PAT_OLD_NR = re.compile(r"^mkge(\d+)nr\.?(\d+)\.pdf$", re.I)
_PAT_OLD_CONCAT = re.compile(r"^mkge(\d{2})(\d{1,2})[a-z]?\.pdf$", re.I)

# Skip: indexes, registers, trilingual side-by-side editions, whole-band bundles.
_SKIP_PATTERNS = [
    re.compile(r"regest", re.I),
    re.compile(r"sach.*gesetzes", re.I),
    re.compile(r"atm.*stmc|atmc.*stmc|mkge.*atm|mkge.*stmc", re.I),
    re.compile(r"^entscheide\s+mkg\s+band", re.I),
    re.compile(r"^mkge-entscheide-\d+-\d+\.pdf$", re.I),
]

# German / French / Italian month names → 1..12
_MONTHS = {
    "januar": 1, "februar": 2, "märz": 3, "april": 4, "mai": 5, "juni": 6,
    "juli": 7, "august": 8, "september": 9, "oktober": 10, "november": 11, "dezember": 12,
    "janvier": 1, "février": 2, "mars": 3, "avril": 4, "juin": 6,
    "juillet": 7, "août": 8, "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5, "giugno": 6,
    "luglio": 7, "agosto": 8, "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
}


def _extract_pdf_text(data: bytes) -> str:
    """PDF text extraction via fitz (PyMuPDF) with pdfplumber fallback."""
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


def _parse_filename(filename: str) -> dict | None:
    """Parse a MKG PDF filename into {band, nr, mkg_no, date}.

    Returns None for filenames that should be skipped (registers, bundles, etc.)
    or that don't match any known pattern.
    """
    name = unquote(filename)

    for pat in _SKIP_PATTERNS:
        if pat.search(name):
            return None

    m = _PAT_MODERN.match(name)
    if m:
        return {"band": int(m.group(1)), "nr": int(m.group(2)),
                "mkg_no": None, "date": None}

    m = _PAT_OLD_FULL.match(name)
    if m:
        try:
            d = date(int(m.group(6)), int(m.group(5)), int(m.group(4)))
        except ValueError:
            d = None
        return {"band": int(m.group(1)), "nr": int(m.group(2)),
                "mkg_no": int(m.group(3)), "date": d}

    m = _PAT_OLD_NR.match(name)
    if m:
        return {"band": int(m.group(1)), "nr": int(m.group(2)),
                "mkg_no": None, "date": None}

    m = _PAT_OLD_CONCAT.match(name)
    if m:
        # Heuristic: only assume concat form when the leading two digits are a
        # plausible band number (Bd. 13+ are the digital ones on this portal).
        band = int(m.group(1))
        if band >= 13:
            return {"band": band, "nr": int(m.group(2)),
                    "mkg_no": None, "date": None}

    return None


def _extract_trailing_meta(text: str) -> dict:
    """Pull (mkg_no, decision_date, parties) from the closing parenthesis line.

    Swiss MKG PDFs end with e.g.:
      "(936, 16. März 2024, B. gegen Militärappellationsgericht 2)"
      "(843, 8. Februar 2013, G. P. gegen Präsident MG 7)"

    Returns {} if no match.
    """
    # Search the last 2 KB only — the trailer is always near EOF.
    tail = text[-2000:]
    # Variants seen across volumes:
    #   "(936, 16. März 2024, B. gegen ...)"            modern DE
    #   "(MKG 944, arrêt du 22 novembre 2024, ...)"     modern FR
    #   "(TMC 943, 22 novembre 2024, accusé contre ...)"
    #   "(N° 813 du 10 décembre 2009, Auditeur c. ...)" Bd 13 FR
    #   "(Nr. 781, 30. März 2006, S. A. c MAG 2)"       Bd 13 DE
    # Number prefixes: MKG / TMC / TMCa / ATMC / STMC / Nr. / N° / no.
    # Date intro:      "" / "du" / "rendu le" / "vom" / "del" / "arrêt du"
    # Separator before date: comma OR "du"
    m = re.search(
        r"\(\s*"
        r"(?:MKG|TMC|TMCa|ATMC|STMC|N(?:r\.?|°)|no\.?)?\s*"
        r"(\d{2,4}(?:\s*/\s*\d{2,4})*)\s*"  # leading or grouped case numbers
        r"(?:,\s*(?:arr[eê]t\s+(?:du|rendu\s+le)\s+|urteil\s+vom\s+|sentenza\s+del\s+|del\s+)?"
        r"|\s+(?:du|del|vom)\s+)"
        r"(\d{1,2})\.?\s*([A-Za-zÄÖÜäöüéèàùç]+)\.?\s*(\d{4})\s*,\s*"
        r"([^()]{2,200})\)",
        tail,
        re.I,
    )
    if not m:
        return {}
    # Case number may be a single int or "849/850/851" — keep first as primary.
    raw_no = m.group(1).split("/")[0].strip()
    try:
        mkg_no = int(raw_no)
    except ValueError:
        mkg_no = None
    day = int(m.group(2))
    month_name = m.group(3).lower().rstrip(".")
    year = int(m.group(4))
    parties = m.group(5).strip()
    month = _MONTHS.get(month_name)
    if not month:
        return {"mkg_no": mkg_no, "parties": parties}
    try:
        d = date(year, month, day)
    except ValueError:
        return {"mkg_no": mkg_no, "parties": parties}
    return {"mkg_no": mkg_no, "decision_date": d, "parties": parties}


_BODY_MARKERS = (
    "Das Militärkassationsgericht hat festgestellt",
    "Das Militärkassationsgericht hat erwogen",
    "Le Tribunal militaire de cassation",
    "Il Tribunale militare di cassazione",
)


def _split_trilingual_regeste(text: str) -> dict:
    """Split the leading Regeste into DE / FR / IT chunks.

    MKG decisions open with a headnote that is either trilingual (Art. ... MStP →
    Art. ... PPM (FR) → Art. ... PPM (IT)) or monolingual in the case language.
    We:
      1. Find the end of the Regeste block (start of body) using known markers.
      2. Within that block, detect FR ("PPM") and IT ("cpv.") headers.
      3. Assign each segment to the right language slot. If no segments are
         found, place the whole block under the case language.
    """
    head = text[:10000]

    # Find body start to bound the regeste search
    body_pos = len(head)
    for m in _BODY_MARKERS:
        i = head.find(m)
        if 0 <= i < body_pos:
            body_pos = i
    regeste = head[:body_pos].strip()
    if len(regeste) < 80:
        return {"abstract_de": None, "abstract_fr": None, "abstract_it": None}

    fr_match = re.search(r"\bArt\.\s+\d+[^\n]{0,80}\bPPM\b", regeste)
    it_match = None
    if fr_match:
        it_match = re.search(
            r"\bArt\.\s+\d+\s+cpv\.\s+\d+[^\n]{0,80}\bPPM\b",
            regeste[fr_match.start() + 50:],
        )

    out: dict = {"abstract_de": None, "abstract_fr": None, "abstract_it": None}

    if fr_match and it_match:
        it_start_abs = fr_match.start() + 50 + it_match.start()
        out["abstract_de"] = regeste[: fr_match.start()].strip()[:2000] or None
        out["abstract_fr"] = regeste[fr_match.start(): it_start_abs].strip()[:2000] or None
        out["abstract_it"] = regeste[it_start_abs:].strip()[:2000] or None
    elif fr_match:
        out["abstract_de"] = regeste[: fr_match.start()].strip()[:2000] or None
        out["abstract_fr"] = regeste[fr_match.start():].strip()[:2000] or None
    else:
        # Monolingual Regeste — caller's language detection will route it.
        out["_monolingual"] = regeste[:2000]

    return out


class MilitaerkassationsgerichtScraper(BaseScraper):
    """Scraper for the Swiss Military Court of Cassation (MKG)."""

    REQUEST_DELAY = 1.0
    TIMEOUT = 90

    @property
    def court_code(self) -> str:
        return "mkg"

    def _fetch_index(self) -> list[tuple[str, str]]:
        """Return [(filename, absolute_url), ...] of every PDF on the index page."""
        response = self.get(INDEX_URL)
        html = response.text
        urls = re.findall(r'href="([^"]+\.pdf)"', html, re.I)
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for u in urls:
            if u.startswith("/"):
                u = urljoin(BASE_URL, u)
            if u in seen:
                continue
            seen.add(u)
            name = unquote(u.rsplit("/", 1)[-1])
            out.append((name, u))
        logger.info(f"[mkg] Index returned {len(out)} PDF links")
        return out

    def discover_new(self, since_date=None) -> Iterator[dict]:
        """Iterate individual MKGE decision PDFs."""
        # Track band/nr to detect duplicates across naming conventions
        # (the index may list the same decision under multiple historical
        # filenames — we keep only the first).
        seen_keys: set[tuple[int, int]] = set()
        n_kept = 0
        n_skipped = 0

        for filename, url in self._fetch_index():
            meta = _parse_filename(filename)
            if not meta:
                n_skipped += 1
                continue

            key = (meta["band"], meta["nr"])
            if key in seen_keys:
                logger.debug(f"[mkg] Duplicate band+nr {key} — skipping {filename}")
                continue
            seen_keys.add(key)

            decision_id = make_decision_id("mkg", f"MKGE_{meta['band']}_Nr_{meta['nr']}")

            if self.state.is_known(decision_id):
                continue

            if since_date and meta["date"] and meta["date"] < since_date:
                continue

            n_kept += 1
            yield {
                "decision_id": decision_id,
                "docket_number": f"MKGE {meta['band']} Nr. {meta['nr']}",
                "decision_date": meta["date"],
                "url": url,
                "title": filename,
                "_filename_meta": meta,
            }

        logger.info(f"[mkg] discover_new: {n_kept} candidates, {n_skipped} non-decision PDFs skipped")

    def fetch_decision(self, stub: dict) -> Decision | None:
        url = stub["url"]
        docket = stub["docket_number"]
        decision_id = stub["decision_id"]
        meta = stub.get("_filename_meta", {})

        try:
            response = self.get(url)
            pdf_data = response.content
        except Exception as e:
            logger.error(f"[mkg] Failed to download {docket}: {e}")
            return None

        if len(pdf_data) > MAX_PDF_SIZE:
            logger.warning(f"[mkg] PDF too large ({len(pdf_data)} bytes): {docket}")
            return None

        full_text = _extract_pdf_text(pdf_data)
        if not full_text or len(full_text.strip()) < 100:
            logger.warning(f"[mkg] No usable text from {docket} ({len(pdf_data)} bytes)")
            return None

        full_text = self.clean_text(full_text)

        # Decision date: prefer in-PDF trailer, fall back to filename meta.
        trailer = _extract_trailing_meta(full_text)
        d_date = trailer.get("decision_date") or meta.get("date")
        mkg_no = trailer.get("mkg_no") or meta.get("mkg_no")
        parties = trailer.get("parties")

        # Regeste split (best-effort; non-fatal if it fails).
        try:
            abs_blocks = _split_trilingual_regeste(full_text)
        except Exception:
            abs_blocks = {}

        # Language: prefer the parties phrasing in the trailer ("gegen" / "contre"
        # / "contro"), since detect_language tripps on the trilingual Regeste at
        # the top of every PDF. Fall back to detection on the tail.
        lang = None
        parties_lower = (parties or "").lower()
        if " gegen " in parties_lower:
            lang = "de"
        elif " contre " in parties_lower or re.search(r"\bc\.?\s+[A-ZÉÈ]", parties or ""):
            lang = "fr"
        elif " contro " in parties_lower:
            lang = "it"
        if not lang:
            tail_text = full_text[-4000:] if len(full_text) > 6000 else full_text
            lang = detect_language(tail_text) or "de"

        # Monolingual Regeste? Route to the slot matching detected language.
        mono = abs_blocks.pop("_monolingual", None) if isinstance(abs_blocks, dict) else None
        if mono and not (abs_blocks.get("abstract_de") or abs_blocks.get("abstract_fr") or abs_blocks.get("abstract_it")):
            slot = f"abstract_{lang}" if lang in ("de", "fr", "it") else "abstract_de"
            abs_blocks[slot] = mono

        title = docket
        if parties:
            title = f"{docket} — {parties[:120]}"

        citations = extract_citations(full_text)

        decision = Decision(
            decision_id=decision_id,
            court="mkg",
            canton="CH",
            docket_number=docket,
            decision_date=d_date,
            language=lang,
            title=title,
            legal_area="Militärstrafrecht",
            abstract_de=abs_blocks.get("abstract_de"),
            abstract_fr=abs_blocks.get("abstract_fr"),
            abstract_it=abs_blocks.get("abstract_it"),
            full_text=full_text,
            source_url=url,
            pdf_url=url,
            collection=f"MKGE {meta.get('band')} Nr. {meta.get('nr')}" if meta else None,
            external_id=f"MKG-{mkg_no}" if mkg_no else None,
            cited_decisions=citations,
            scraped_at=datetime.now(timezone.utc),
        )
        return decision


# TODO (Phase 2): Older volumes (Bd. 1–12, 1915–2007) are catalogued at
# https://www.alexandria.ch/discovery/delivery/41BIG_INST:ALEX/12329504630001791
# but the asset is delivered through a JS-rendered Primo viewer that does not
# expose direct PDF URLs over standard HTTP. Recovering them requires either
# (a) a Playwright-driven scrape that resolves the viewer's representation
# manifest, or (b) a direct request to BBL/Alexandria for the source PDFs.


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Militärkassationsgericht")
    parser.add_argument("--since", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--max", type=int, default=5, help="Max decisions")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    since = date.fromisoformat(args.since) if args.since else None
    scraper = MilitaerkassationsgerichtScraper()
    decisions = scraper.run(since_date=since, max_decisions=args.max)
    scraper.mark_run_complete(decisions)
    for d in decisions:
        print(f"  {d.decision_id}  {d.decision_date}  {d.language}  {d.title[:80]}")
    print(f"\nScraped {len(decisions)} MKG decisions")

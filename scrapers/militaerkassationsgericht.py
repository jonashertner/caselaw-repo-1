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

Whole-Band bundle PDFs (e.g. "Entscheide MKG Band 14 (2014-2021).pdf",
"MKGE-Entscheide-13-42.pdf") are downloaded once, split into per-decision
slices using header + trailer markers, and yielded as separate stubs.
Trilingual side-by-side editions, registers, and Regesten lists are skipped —
they duplicate or summarise the individual decisions.

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

# Alexandria.ch (Swiss federal libraries / BBL) hosts the digitised volumes
# Bd. 1–13 of the MKGE serial as scanned, OCR'd PDFs. The "representationInfo"
# JSON endpoint returns presigned S3 download URLs for each Band PDF.
ALEX_REPINFO_URL = (
    "https://www.alexandria.ch/primaws/rest/priv/delivery/representationInfo"
    "?inst=41BIG_INST&lang=de&mmsId=&pid=12329504630001791"
)

MAX_PDF_SIZE = 60 * 1024 * 1024  # 60 MB — Alexandria scans (Bd. 4) reach 41 MB

# Filename patterns (case-insensitive). Order matters: most-specific first.
_PAT_MODERN = re.compile(r"^MKGE\s+(\d+)\s+Nr\.?\s*(\d+)\.pdf$", re.I)
_PAT_OLD_FULL = re.compile(
    r"^mkge(\d+)nr\.?(\d+)nr\.?(\d+)vom(\d{1,2})\.(\d{1,2})\.(\d{4})\.pdf$", re.I
)
_PAT_OLD_NR = re.compile(r"^mkge(\d+)nr\.?(\d+)\.pdf$", re.I)
_PAT_OLD_CONCAT = re.compile(r"^mkge(\d{2})(\d{1,2})[a-z]?\.pdf$", re.I)
# "MKGE-Entscheide-13-42.pdf" — single Bd 13 decision, not a bundle.
_PAT_OLD_DASHED = re.compile(r"^MKGE-Entscheide-(\d+)-(\d+)\.pdf$", re.I)

# Skip: indexes, registers, trilingual side-by-side editions.
# (Bundle PDFs that aggregate a whole Band of decisions are NOT skipped — they
# are handled separately by _split_band_bundle.)
_SKIP_PATTERNS = [
    re.compile(r"regest", re.I),
    re.compile(r"sach.*gesetzes", re.I),
    re.compile(r"atm.*stmc|atmc.*stmc|mkge.*atm|mkge.*stmc", re.I),
]

# Whole-Band bundle PDFs: each holds N individual decisions to be split.
# (NB: "MKGE-Entscheide-<band>-<nr>.pdf" looks bundle-shaped but is in fact
#  a single decision — see _PAT_OLD_DASHED below.)
_PAT_BUNDLE_BD14 = re.compile(r"^Entscheide\s+MKG\s+Band\s+(\d+)", re.I)

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
    pages = _extract_pdf_pages(data)
    return "\n\n".join(pages)


def _extract_pdf_pages(data: bytes) -> list[str]:
    """Per-page PDF text extraction; preserves page boundaries for splitter."""
    try:
        import fitz
        doc = fitz.open(stream=data, filetype="pdf")
        return [p.get_text() for p in doc]
    except ImportError:
        pass
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return [p.extract_text() or "" for p in pdf.pages]
    except ImportError:
        pass
    return []


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

    m = _PAT_OLD_DASHED.match(name)
    if m:
        return {"band": int(m.group(1)), "nr": int(m.group(2)),
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
    # For long bundle slices (multiple pages), trailers are still near the end
    # but we need a wider window than for single-PDF decisions.
    tail = text[-4000:]
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
        # Case-number group: one number (allows decimal like 886.1), with
        # optional siblings joined by '/', 'et' or 'und' (e.g. "924.1 und 924.2",
        # "810/811", "886.1 et 886.2").
        r"(\d{2,4}(?:\.\d+)?(?:\s*(?:/|et|und)\s*\d{2,4}(?:\.\d+)?)*)\s*"
        r"(?:,\s*(?:arr[eê]t\s+(?:du|rendu\s+le)\s+|urteil\s+vom\s+|sentenza\s+del\s+|del\s+)?"
        r"|\s+(?:du|del|vom)\s+)"
        r"(\d{1,2})\.?\s*([A-Za-zÄÖÜäöüéèàùç]+)\.?\s*(\d{4})\s*,\s*"
        r"([^()]{2,200})\)",
        tail,
        re.I,
    )
    if not m:
        # Older volumes (Bd. 1–11) often write the trailer without a case number:
        #   "(24. März 1988, Aud. und H. e. MAG 2A)"
        # Try a number-less variant to recover dates from those slices.
        m = re.search(
            r"\(\s*(\d{1,2})\.?\s*([A-Za-zÄÖÜäöüéèàùç]+)\.?\s*(\d{4})\s*,\s*"
            r"([^()]{2,200})\)",
            tail,
            re.I,
        )
        if not m:
            return {}
        day, month_name, year, parties = m.group(1), m.group(2), m.group(3), m.group(4)
        try:
            d = date(int(year), _MONTHS.get(month_name.lower().rstrip("."), 0), int(day))
        except (ValueError, TypeError):
            return {"parties": parties.strip()}
        return {"mkg_no": None, "decision_date": d, "parties": parties.strip()}

    # Case number may be a single int, "849/850/851", "886.1 et 886.2", etc.
    # Keep the first integer part as the primary MKG-#.
    raw_no = re.split(r"[\s/etundEUNDA]+", m.group(1).strip())[0]
    raw_no = raw_no.split(".")[0]  # drop decimal suffix (886.1 → 886)
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


_TRAILER_PAT = re.compile(
    r"\(\s*"
    r"(?:MKG|TMC|TMCa|ATMC|STMC|N(?:r\.?|°)|no\.?)?\s*"
    r"(\d{2,4}(?:\.\d+)?(?:\s*(?:/|et|und)\s*\d{2,4}(?:\.\d+)?)*)\s*"
    r"(?:,\s*(?:arr[eê]t\s+(?:du|rendu\s+le)\s+|urteil\s+vom\s+|sentenza\s+del\s+|del\s+)?"
    r"|\s+(?:du|del|vom)\s+)"
    r"(\d{1,2})\.?\s*([A-Za-zÄÖÜäöüéèàùç]+)\.?\s*(\d{4})\s*,\s*"
    r"([^()]{2,200})\)",
    re.I,
)

# Decision-number header in page running text. Both "Nr. N" (modern Bd. 12+)
# and "No. N" (Bd. 1–11) are accepted. Anchored to start of line OR after a
# leading newline so we don't catch in-text references like "MKGE 11 Nr. 28".
_NR_HEADER_PAT = re.compile(r"(?:^|\n)\s*(?:Nr|No)\.?\s*(\d{1,3})\b")


def _page_header_nr(page_text: str) -> int | None:
    """Return the decision Nr declared by this page's running header.

    Both header styles seen in MKGE volumes:
      • Modern (Bd. 12+):  "Nr. N\\n<page>"      at top of page
      • Old (Bd. 1–11):   "No. N\\n<page>"  or  "<page>\\nNo. N"
    To avoid being fooled by in-text citations like "MKGE 11 Nr. 28" we look
    only inside the first few non-empty lines (= page header zone) plus the
    last few (page footer for old volumes that put the header at the bottom).
    """
    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
    if not lines:
        return None
    head = "\n".join(lines[:6])
    foot = "\n".join(lines[-6:])
    candidates: list[int] = []
    for region in (head, foot):
        # OCR artefacts: "l" / "I" frequently substitute for "1" in old prints.
        # Normalise the digit cluster after "Nr/No" so e.g. "No. l" → 1,
        # "No. ll" → 11. We require the resulting token to start with [1-9].
        for m in re.finditer(r"\b(?:Nr|No)\.?\s*([0-9lIO]{1,3})\b", region):
            tok = (m.group(1)
                   .replace("l", "1").replace("I", "1").replace("O", "0"))
            if tok and tok[0] in "123456789":
                try:
                    n = int(tok)
                except ValueError:
                    continue
                if 1 <= n <= 200:
                    candidates.append(n)
    if not candidates:
        return None
    # Lowest candidate is usually the running Nr (footnotes/citations bring
    # in higher refs); when ambiguous, pick the most frequent.
    counts: dict[int, int] = {}
    for n in candidates:
        counts[n] = counts.get(n, 0) + 1
    return max(counts.items(), key=lambda kv: (kv[1], -kv[0]))[0]


def _split_band_bundle_pages(pages: list[str], band: int) -> list[dict]:
    """Group consecutive pages by their dominant decision Nr.

    Each page of a MKGE volume carries a running header "Nr. N" (or "No. N"
    for Bd. 1–11). We assign each page to its dominant Nr and concatenate
    consecutive pages with the same Nr into one decision slice. This is
    robust both to old volumes (no trailers) and to modern ones (trailer at
    end of last page lands cleanly in the slice for that decision).

    The Nr sequence must be monotonically non-decreasing within a Band; any
    page whose dominant Nr would *decrease* relative to the running max is
    treated as still belonging to the current Nr (e.g. an in-text reference
    overwhelmed the actual header). Stops when "Entscheidungsliste" /
    similar back-matter markers appear.
    """
    end_marker_re = re.compile(
        r"Entscheidungsliste|Liste des arr[eê]ts\s+selon|Lista delle sentenze|"
        r"Sach-\s*und\s*Gesetzes|Abk[üu]rzungen\s+\u2013",
        re.I,
    )

    groups: list[tuple[int, list[str]]] = []  # [(nr, [pages])]
    current_nr: int | None = None
    current_pages: list[str] = []
    body_started = False  # Flips True the first time we see Nr=1 or two
                          # consecutive pages agreeing on the same Nr.
    pending_nr: int | None = None  # candidate Nr awaiting confirmation

    for page_text in pages:
        if end_marker_re.search(page_text) and current_nr is not None:
            break
        nr = _page_header_nr(page_text)

        # Treat "Nr. 1" as an explicit body-start trigger — front-matter pages
        # may carry stray numerals that fool the heuristic, so we reset state
        # whenever the canonical first-decision header appears.
        if nr == 1 and not body_started:
            body_started = True
            current_nr = 1
            current_pages = [page_text]
            pending_nr = None
            continue

        if not body_started:
            # Require two consecutive pages with the same Nr before locking in
            # — this filters TOC / Vorwort numerals.
            if nr is None:
                pending_nr = None
                continue
            if pending_nr == nr:
                body_started = True
                current_nr = nr
                current_pages = [page_text]
                pending_nr = None
            else:
                pending_nr = nr
            continue

        # Body started.
        if nr is None:
            current_pages.append(page_text)
            continue
        if nr == current_nr:
            current_pages.append(page_text)
        elif nr > current_nr:
            groups.append((current_nr, current_pages))
            current_nr = nr
            current_pages = [page_text]
        else:
            # nr < current_nr — keep with current decision (likely an OCR
            # mis-read or in-text reference that beat the actual header).
            current_pages.append(page_text)

    if current_nr is not None and current_pages:
        groups.append((current_nr, current_pages))

    out: list[dict] = []
    for nr, pgs in groups:
        text = "\n".join(pgs).strip()
        if len(text) < 200:
            continue
        out.append({"nr": nr, "text": text, "band": band})
    return out


def _split_band_bundle(text: str, band: int) -> list[dict]:
    """Backward-compatible wrapper that splits a single concatenated text.

    Re-creates page boundaries from the form-feed character ('\\f') if
    present, otherwise falls back to one-page-per-text. New code should
    prefer _split_band_bundle_pages, which keeps the original page array.
    """
    if "\f" in text:
        pages = text.split("\f")
    else:
        pages = [text]
    return _split_band_bundle_pages(pages, band)


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
        """Iterate individual MKGE decision PDFs.

        Whole-Band bundle PDFs are detected, downloaded once, split into
        per-decision slices, and yielded as separate stubs that already carry
        the pre-extracted text (so fetch_decision skips re-download).
        """
        seen_keys: set[tuple[int, int]] = set()
        n_kept = 0
        n_skipped = 0
        n_bundle_decisions = 0

        # First pass: collect bundle URLs and individual URLs separately so we
        # can prefer individual PDFs (better quality, single-PDF metadata) over
        # the bundle slice for the same decision.
        all_links = list(self._fetch_index())
        individual_links: list[tuple[str, str]] = []
        bundle_links: list[tuple[str, str, int]] = []  # (filename, url, band)

        for filename, url in all_links:
            bd14_match = _PAT_BUNDLE_BD14.match(filename)
            if bd14_match:
                bundle_links.append((filename, url, int(bd14_match.group(1))))
            else:
                individual_links.append((filename, url))

        # Pass 1: individual decision PDFs (preferred source).
        for filename, url in individual_links:
            meta = _parse_filename(filename)
            if not meta:
                n_skipped += 1
                continue

            key = (meta["band"], meta["nr"])
            if key in seen_keys:
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

        # Pass 2: bundle PDFs (currently only Bd. 14 on oa.admin.ch).
        for filename, url, band in bundle_links:
            yield from self._yield_bundle_slices(filename, url, band, seen_keys)

        # Pass 3: Alexandria.ch — Bd. 1–12 historical volumes plus Bd. 13.x
        # subdivisions. The Bd. 13 sub-PDFs are skipped because oa.admin.ch
        # already serves cleaner per-decision PDFs for that Band.
        try:
            alex_files = self._fetch_alexandria_files()
        except Exception as e:
            logger.warning(f"[mkg] Alexandria index fetch failed: {e}")
            alex_files = []

        for af in alex_files:
            label = af["label"]
            if "Register" in label:
                continue
            band_match = re.match(r"^(\d+)\.", label)
            if not band_match:
                continue
            band = int(band_match.group(1))
            sub_match = re.match(r"^\d+\.(\d+)", label)
            if sub_match and band == 13:
                # Bd. 13.1/13.2/13.3 — already covered by oa.admin.ch.
                continue
            yield from self._yield_bundle_slices(
                f"alexandria:{label}", af["downloadUrl"], band, seen_keys
            )

        logger.info(
            f"[mkg] discover_new: {n_kept} individual + bundle slices, "
            f"{n_skipped} non-decision PDFs skipped"
        )

    def _fetch_alexandria_files(self) -> list[dict]:
        """Hit Alexandria's representationInfo API; return file descriptor list."""
        response = self.get(ALEX_REPINFO_URL)
        data = response.json()
        files = data.get("data", {}).get("files", [])
        logger.info(f"[mkg] Alexandria representationInfo: {len(files)} files")
        return files

    def _yield_bundle_slices(
        self, filename: str, url: str, band: int, seen_keys: set,
    ) -> Iterator[dict]:
        """Download a bundle PDF, split into slices, yield stubs for new ones."""
        try:
            logger.info(f"[mkg] Downloading bundle: {filename}")
            pdf_data = self.get(url).content
        except Exception as e:
            logger.error(f"[mkg] Bundle download failed {filename}: {e}")
            return
        if len(pdf_data) > MAX_PDF_SIZE:
            logger.warning(f"[mkg] Bundle too large ({len(pdf_data)} bytes): {filename}")
            return
        pages = _extract_pdf_pages(pdf_data)
        if not pages:
            logger.warning(f"[mkg] Bundle text extraction empty: {filename}")
            return
        slices = _split_band_bundle_pages(pages, band)
        logger.info(f"[mkg] Bundle {filename}: {len(slices)} decision slices")
        for sl in slices:
            key = (band, sl["nr"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            decision_id = make_decision_id("mkg", f"MKGE_{band}_Nr_{sl['nr']}")
            if self.state.is_known(decision_id):
                continue
            yield {
                "decision_id": decision_id,
                "docket_number": f"MKGE {band} Nr. {sl['nr']}",
                "decision_date": None,
                "url": url,
                "title": f"{filename} (slice Nr. {sl['nr']})",
                "_filename_meta": {
                    "band": band, "nr": sl["nr"], "mkg_no": None, "date": None,
                },
                "_text_slice": sl["text"],
                "_bundle_filename": filename,
            }

    def fetch_decision(self, stub: dict) -> Decision | None:
        url = stub["url"]
        docket = stub["docket_number"]
        decision_id = stub["decision_id"]
        meta = stub.get("_filename_meta", {})

        # Bundle slice path: text already extracted in discover_new.
        text_slice = stub.get("_text_slice")
        if text_slice:
            full_text = self.clean_text(text_slice)
        else:
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


# Bd. 1–11 (1915–1996) are scanned/OCR'd PDFs from Alexandria. The OCR
# quality varies; a few volumes (notably Bd. 11, where decision Nrs are
# given as "1.", "2." inside the body rather than as page-running headers)
# yield coarser splits — those slices may bundle several adjacent decisions.


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

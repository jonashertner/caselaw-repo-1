#!/usr/bin/env python3
"""
entscheidsuche_ingest.py — Convert entscheidsuche.ch bulk download to our JSONL
================================================================================

Reads JSON metadata + HTML/PDF content from entscheidsuche.ch/docs/ bulk download,
converts to our Decision JSONL format, and deduplicates against existing scrapers.

Usage:
    python3 entscheidsuche_ingest.py --input /opt/caselaw/entscheidsuche \
                                      --output /opt/caselaw/repo/output/decisions \
                                      --existing /opt/caselaw/repo/output/decisions \
                                      [--spider GE_Gerichte] \
                                      [--dry-run] [-v]

Architecture:
    1. Scan spider directories for .json files
    2. Parse JSON metadata (Signatur, Spider, Datum, Num, etc.)
    3. If HTML companion exists → extract text with bs4
    4. If PDF only → extract text with pdftotext (if available)
    5. Map Spider → our court/canton codes
    6. Deduplicate by docket_number against existing JSONL files
    7. Output one JSONL per spider-mapped court
"""

import argparse
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ============================================================================
# Spider → Court/Canton mapping
# ============================================================================
# Each spider maps to: (our_court_code, canton, court_display_name, level)
# level: "federal", "cantonal", "regulatory"

SPIDER_MAP = {
    # ── Federal ──
    "CH_BGE":       ("bge",    "CH", "Bundesgericht (BGE)",              "federal"),
    "CH_BGer":      ("bger",   "CH", "Bundesgericht",                    "federal"),
    "CH_BVGer":     ("bvger",  "CH", "Bundesverwaltungsgericht",         "federal"),
    "CH_BSTG":      ("bstger", "CH", "Bundesstrafgericht",               "federal"),
    "CH_BPatG":     ("bpatger","CH", "Bundespatentgericht",              "federal"),
    "CH_VB":        ("ch_vb",  "CH", "Eidg. Verwaltungsbehörden",        "federal"),
    "CH_Bundesrat": ("ch_bundesrat","CH","Bundesrat",                    "federal"),
    "CH_EDOEB":     ("edoeb",  "CH", "EDÖB",                             "regulatory"),
    "CH_WEKO":      ("weko",   "CH", "WEKO",                             "regulatory"),

    # ── Zürich ──
    "ZH_Obergericht":   ("zh_gerichte",                "ZH", "ZH Obergericht",              "cantonal"),
    "ZH_Sozialversicherungsgericht": ("zh_sozialversicherungsgericht","ZH", "ZH Sozialversicherungsgericht","cantonal"),
    "ZH_Verwaltungsgericht":        ("zh_verwaltungsgericht",      "ZH", "ZH Verwaltungsgericht",        "cantonal"),
    "ZH_Baurekurs":     ("zh_baurekursgericht",        "ZH", "ZH Baurekursgericht",          "cantonal"),
    "ZH_Steuerrekurs":  ("zh_steuerrekursgericht",     "ZH", "ZH Steuerrekursgericht",       "cantonal"),

    # ── Aargau ──
    "AG_Gerichte":      ("ag_gerichte",  "AG", "AG Gerichte",   "cantonal"),
    "AG_Baugesetzgebung": ("ag_baugesetzgebung", "AG", "AG Baugesetzgebung", "cantonal"),
    "AG_Weitere":       ("ag_weitere",   "AG", "AG Weitere",    "cantonal"),

    # ── Basel-Stadt ──
    "BS_Omni":          ("bs_gerichte",  "BS", "BS Gerichte",   "cantonal"),

    # ── Zug ──
    "ZG_Verwaltungsgericht": ("zg_verwaltungsgericht", "ZG", "ZG Verwaltungsgericht", "cantonal"),
    "ZG_Obergericht":        ("zg_obergericht",        "ZG", "ZG Obergericht",        "cantonal"),

    # ── Genève ──
    "GE_Gerichte":      ("ge_gerichte",  "GE", "GE Gerichte",   "cantonal"),

    # ── Vaud ──
    "VD_FindInfo":      ("vd_findinfo",  "VD", "VD FindInfo",   "cantonal"),
    "VD_Omni":          ("vd_omni",      "VD", "VD Gerichte",   "cantonal"),

    # ── Ticino ──
    "TI_Gerichte":      ("ti_gerichte",  "TI", "TI Gerichte",   "cantonal"),

    # ── Luzern ──
    "LU_Gerichte":      ("lu_gerichte",  "LU", "LU Gerichte",   "cantonal"),

    # ── St. Gallen ──
    "SG_Gerichte":      ("sg_gerichte",  "SG", "SG Gerichte",   "cantonal"),
    "SG_Publikationen": ("sg_publikationen", "SG", "SG Publikationen", "cantonal"),

    # ── Solothurn ──
    "SO_Omni":          ("so_gerichte",  "SO", "SO Gerichte",   "cantonal"),

    # ── Baselland ──
    "BL_Gerichte":      ("bl_gerichte",  "BL", "BL Gerichte",   "cantonal"),

    # ── Graubünden ──
    "GR_Gerichte":      ("gr_gerichte",  "GR", "GR Gerichte",   "cantonal"),

    # ── Fribourg ──
    "FR_Gerichte":      ("fr_gerichte",  "FR", "FR Gerichte",   "cantonal"),

    # ── Neuchâtel ──
    "NE_Omni":          ("ne_gerichte",  "NE", "NE Gerichte",   "cantonal"),

    # ── Bern ──
    "BE_Verwaltungsgericht": ("be_verwaltungsgericht", "BE", "BE Verwaltungsgericht", "cantonal"),
    "BE_ZivilStraf":    ("be_zivilstraf","BE", "BE Zivil/Straf", "cantonal"),
    "BE_Steuerrekurs":  ("be_steuerrekurs","BE", "BE Steuerrekursk.", "cantonal"),
    "BE_BVD":           ("be_bvd",       "BE", "BE BVD",           "cantonal"),
    "BE_Anwaltsaufsicht": ("be_anwaltsaufsicht", "BE", "BE Anwaltsaufsicht", "cantonal"),
    "BE_Weitere":       ("be_weitere",   "BE", "BE Weitere",       "cantonal"),

    # ── Schwyz ──
    "SZ_Gerichte":      ("sz_gerichte",  "SZ", "SZ Gerichte",   "cantonal"),
    "SZ_Verwaltungsgericht": ("sz_verwaltungsgericht", "SZ", "SZ Verwaltungsgericht", "cantonal"),

    # ── Thurgau ──
    "TG_OG":            ("tg_obergericht","TG", "TG Obergericht","cantonal"),

    # ── Appenzell AR ──
    "AR_Gerichte":      ("ar_gerichte",  "AR", "AR Gerichte",   "cantonal"),

    # ── Valais ──
    "VS_Gerichte":      ("vs_gerichte",  "VS", "VS Gerichte",   "cantonal"),

    # ── Uri ──
    "UR_Gerichte":      ("ur_gerichte",  "UR", "UR Gerichte",   "cantonal"),

    # ── Obwalden ──
    "OW_Gerichte":      ("ow_gerichte",  "OW", "OW Gerichte",   "cantonal"),

    # ── Nidwalden ──
    "NW_Gerichte":      ("nw_gerichte",  "NW", "NW Gerichte",   "cantonal"),

    # ── Schaffhausen ──
    "SH_OG":            ("sh_obergericht","SH", "SH Obergericht","cantonal"),

    # ── Jura ──
    "JU_Gerichte":      ("ju_gerichte",  "JU", "JU Gerichte",   "cantonal"),

    # ── Glarus ──
    "GL_Omni":          ("gl_gerichte",  "GL", "GL Gerichte",   "cantonal"),

    # ── Appenzell AI ──
    "AI_Aktuell":       ("ai_gerichte",  "AI", "AI Gerichte",   "cantonal"),
    "AI_Bericht":       ("ai_gerichte",  "AI", "AI Gerichte",   "cantonal"),

    # ── TA (Tagesanzeiger Strafsentencing) ──
    "TA_SST":           ("ta_sst",       "CH", "TA Strafsentencing", "other"),
}


# ============================================================================
# HTML text extraction
# ============================================================================

def extract_text_from_html(html_path: Path) -> str:
    """Extract clean text from HTML file using BeautifulSoup."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        # Fallback: regex-based stripping
        return _extract_text_regex(html_path)

    with open(html_path, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")

    # Remove script, style, nav, header, footer
    for tag in soup.find_all(["script", "style", "nav", "header", "footer", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator="\n", strip=True)

    # Clean up excessive whitespace
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    return "\n".join(lines)


def _extract_text_regex(html_path: Path) -> str:
    """Fallback HTML→text using regex."""
    with open(html_path, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()
    # Remove tags
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.S)
    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.S)
    text = re.sub(r"<[^>]+>", "\n", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&#\d+;", "", text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    return "\n".join(lines)


def extract_text_from_pdf(pdf_path: Path) -> Optional[str]:
    """Extract text from PDF using pdftotext."""
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None


# ============================================================================
# JSON metadata parsing
# ============================================================================

def parse_entscheidsuche_json(json_path: Path) -> Optional[dict]:
    """
    Parse an entscheidsuche.ch JSON metadata file.

    Known fields:
      - Signatur: unique ID (e.g. "GE_CAPJ_001")
      - Spider: source spider name
      - Sprache: language code (de/fr/it) — sometimes absent
      - Datum: decision date (YYYY-MM-DD)
      - HTML: {Datei, URL, Checksum} — if HTML version exists
      - PDF: {Datei, URL, Checksum} — if PDF version exists
      - Scrapedate: when entscheidsuche scraped it
      - Num: [docket number(s)]
      - Kopfzeile: [{Sprachen, Text}] — header text
      - Meta: [{Sprachen, Text}] — meta information
      - Abstract: [{Sprachen, Text}] — summary/regeste
      - Checksum: overall checksum
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning(f"Failed to parse {json_path}: {e}")
        return None

    if not isinstance(data, dict):
        return None

    return data


def detect_language(text: str) -> str:
    """Simple language detection based on common words."""
    if not text:
        return "de"
    text_lower = text[:2000].lower()

    fr_words = ["le ", "la ", "les ", "des ", "du ", "un ", "une ", "est ", "sont ", "dans ", "pour ", "que ", "qui "]
    it_words = ["il ", "la ", "le ", "dei ", "del ", "un ", "una ", "che ", "per ", "con ", "sono ", "nella "]
    de_words = ["der ", "die ", "das ", "den ", "dem ", "des ", "ein ", "eine ", "ist ", "sind ", "und ", "für "]

    fr_score = sum(text_lower.count(w) for w in fr_words)
    it_score = sum(text_lower.count(w) for w in it_words)
    de_score = sum(text_lower.count(w) for w in de_words)

    if fr_score > de_score and fr_score > it_score:
        return "fr"
    if it_score > de_score and it_score > fr_score:
        return "it"
    return "de"


# ============================================================================
# Decision builder
# ============================================================================

def build_decision(
    meta: dict,
    spider: str,
    full_text: str,
    html_url: Optional[str] = None,
    pdf_url: Optional[str] = None,
) -> Optional[dict]:
    """
    Convert entscheidsuche metadata + text into our Decision JSONL format.
    """
    mapping = SPIDER_MAP.get(spider)
    if not mapping:
        logger.debug(f"Unknown spider: {spider}")
        return None

    court_code, canton, court_name, level = mapping
    signatur = meta.get("Signatur", "")
    datum = meta.get("Datum", "")

    # Docket number(s)
    nums = meta.get("Num", [])
    docket = nums[0] if nums else signatur
    docket_2 = nums[1] if len(nums) > 1 else None

    # Language
    sprache = meta.get("Sprache", "")
    if sprache in ("de", "fr", "it"):
        language = sprache
    else:
        # Detect from Kopfzeile/Abstract language tags or full text
        kopf_langs = set()
        for item in meta.get("Kopfzeile", []) + meta.get("Abstract", []):
            if isinstance(item, dict):
                kopf_langs.update(item.get("Sprachen", []))
        if len(kopf_langs) == 1:
            language = list(kopf_langs)[0]
        elif full_text:
            language = detect_language(full_text)
        else:
            language = "de"

    # Regeste from Abstract
    regeste_parts = []
    for item in meta.get("Abstract", []):
        if isinstance(item, dict) and item.get("Text"):
            regeste_parts.append(item["Text"])
    regeste = "\n".join(regeste_parts) if regeste_parts else None

    # Kopfzeile (header) — useful metadata
    kopf_parts = []
    for item in meta.get("Kopfzeile", []):
        if isinstance(item, dict) and item.get("Text"):
            kopf_parts.append(item["Text"])
    kopfzeile = "\n".join(kopf_parts) if kopf_parts else None

    # Meta text
    meta_parts = []
    for item in meta.get("Meta", []):
        if isinstance(item, dict) and item.get("Text"):
            meta_parts.append(item["Text"])
    meta_text = "\n".join(meta_parts) if meta_parts else None

    # Source URL — prefer HTML URL, then PDF URL
    source_url = html_url or pdf_url
    if not source_url:
        html_obj = meta.get("HTML", {})
        pdf_obj = meta.get("PDF", {})
        source_url = html_obj.get("URL") or pdf_obj.get("URL") or ""

    # PDF URL from metadata
    if not pdf_url:
        pdf_obj = meta.get("PDF", {})
        pdf_url = pdf_obj.get("URL")

    # Build content: combine kopfzeile + regeste + full_text
    content_parts = []
    if kopfzeile:
        content_parts.append(kopfzeile)
    if regeste and regeste not in (full_text or ""):
        content_parts.append(regeste)
    if full_text:
        content_parts.append(full_text)
    elif meta_text:
        content_parts.append(meta_text)

    content = "\n\n".join(content_parts) if content_parts else "(metadata only)"

    # Decision ID: court_code + normalized docket
    docket_norm = re.sub(r"[^a-zA-Z0-9_.-]", "_", docket)
    decision_id = f"{court_code}_{docket_norm}"

    # Content hash for dedup
    content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()

    # Parse date
    try:
        decision_date = datum if re.match(r"^\d{4}-\d{2}-\d{2}$", datum) else None
    except Exception:
        decision_date = None

    return {
        "decision_id": decision_id,
        "court": court_code,
        "canton": canton,
        "chamber": None,  # Not reliably in entscheidsuche JSON
        "docket_number": docket,
        "docket_number_2": docket_2,
        "decision_date": decision_date,
        "publication_date": None,
        "language": language,
        "regeste": regeste,
        "full_text": content,
        "decision_type": None,
        "source_url": source_url,
        "pdf_url": pdf_url,
        "cited_decisions": [],
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        # Extra fields for provenance
        "source": "entscheidsuche",
        "entscheidsuche_signatur": signatur,
        "entscheidsuche_spider": spider,
        "content_hash": content_hash,
    }


# ============================================================================
# Deduplication
# ============================================================================

def load_existing_ids(existing_dir: Path) -> set:
    """Load all decision_ids from existing JSONL files."""
    known = set()
    if not existing_dir.exists():
        return known

    for jsonl_file in existing_dir.glob("*.jsonl"):
        try:
            with open(jsonl_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        did = obj.get("decision_id", "")
                        if did:
                            known.add(did)
                        # Also index by docket for cross-source dedup
                        docket = obj.get("docket_number", "")
                        court = obj.get("court", "")
                        if docket and court:
                            known.add(f"{court}::{docket}")
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.warning(f"Error reading {jsonl_file}: {e}")

    return known


# ============================================================================
# Main ingestion
# ============================================================================

def ingest_spider(
    spider: str,
    input_dir: Path,
    output_dir: Path,
    known_ids: set,
    dry_run: bool = False,
) -> tuple[int, int, int]:
    """
    Ingest all decisions from a single spider directory.
    Returns (processed, new, skipped).
    """
    spider_dir = input_dir / spider
    if not spider_dir.exists():
        logger.warning(f"Spider directory not found: {spider_dir}")
        return 0, 0, 0

    mapping = SPIDER_MAP.get(spider)
    if not mapping:
        logger.warning(f"No mapping for spider: {spider}")
        return 0, 0, 0

    court_code = mapping[0]
    output_file = output_dir / f"es_{court_code}.jsonl"  # es_ prefix = entscheidsuche source

    json_files = sorted(spider_dir.glob("*.json"))
    if not json_files:
        logger.info(f"{spider}: no JSON files found")
        return 0, 0, 0

    processed = 0
    new_count = 0
    skipped = 0
    errors = 0

    output_handle = None
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
        output_handle = open(output_file, "a", encoding="utf-8")

    try:
        for json_path in json_files:
            processed += 1

            # Parse JSON metadata
            meta = parse_entscheidsuche_json(json_path)
            if not meta:
                errors += 1
                continue

            # Check for existing — by decision_id or court::docket
            signatur = meta.get("Signatur", "")
            nums = meta.get("Num", [])
            docket = nums[0] if nums else signatur
            docket_norm = re.sub(r"[^a-zA-Z0-9_.-]", "_", docket)
            decision_id = f"{court_code}_{docket_norm}"

            if decision_id in known_ids or f"{court_code}::{docket}" in known_ids:
                skipped += 1
                continue

            # Find companion HTML file
            full_text = ""
            html_path = json_path.with_suffix(".html")

            # Sometimes HTML filename differs from JSON filename
            if not html_path.exists():
                # Try finding by Signatur
                html_obj = meta.get("HTML", {})
                html_datei = html_obj.get("Datei", "")
                if html_datei:
                    alt_path = input_dir / html_datei
                    if alt_path.exists():
                        html_path = alt_path

            if html_path.exists():
                full_text = extract_text_from_html(html_path)
            else:
                # Try PDF
                pdf_path = json_path.with_suffix(".pdf")
                if not pdf_path.exists():
                    pdf_obj = meta.get("PDF", {})
                    pdf_datei = pdf_obj.get("Datei", "")
                    if pdf_datei:
                        alt_pdf = input_dir / pdf_datei
                        if alt_pdf.exists():
                            pdf_path = alt_pdf

                if pdf_path.exists():
                    full_text = extract_text_from_pdf(pdf_path) or ""

            # Build decision
            decision = build_decision(meta, spider, full_text)
            if not decision:
                errors += 1
                continue

            # Write
            if not dry_run and output_handle:
                output_handle.write(json.dumps(decision, ensure_ascii=False) + "\n")

            known_ids.add(decision_id)
            known_ids.add(f"{court_code}::{docket}")
            new_count += 1

            if processed % 1000 == 0:
                logger.info(f"{spider}: {processed} processed, {new_count} new, {skipped} skipped")

    finally:
        if output_handle:
            output_handle.close()

    logger.info(
        f"{spider}: done — {processed} processed, {new_count} new, "
        f"{skipped} skipped, {errors} errors → {output_file.name}"
    )
    return processed, new_count, skipped


def main():
    parser = argparse.ArgumentParser(description="Entscheidsuche.ch → JSONL converter")
    parser.add_argument("--input", default="/opt/caselaw/entscheidsuche",
                        help="Input directory with spider subdirectories")
    parser.add_argument("--output", default="/opt/caselaw/repo/output/decisions",
                        help="Output directory for JSONL files")
    parser.add_argument("--existing", default="/opt/caselaw/repo/output/decisions",
                        help="Directory with existing JSONL for dedup")
    parser.add_argument("--spider", help="Process single spider only")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    existing_dir = Path(args.existing)

    if not input_dir.exists():
        logger.error(f"Input directory not found: {input_dir}")
        sys.exit(1)

    # Load existing IDs for dedup
    logger.info("Loading existing decision IDs for deduplication...")
    known_ids = load_existing_ids(existing_dir)
    logger.info(f"Loaded {len(known_ids)} existing IDs")

    # Determine spiders to process
    if args.spider:
        spiders = [args.spider]
    else:
        # Auto-discover from input directory
        spiders = sorted([
            d.name for d in input_dir.iterdir()
            if d.is_dir() and d.name in SPIDER_MAP
        ])

    if not spiders:
        logger.error("No spider directories found or recognized")
        sys.exit(1)

    logger.info(f"Processing {len(spiders)} spiders: {', '.join(spiders)}")

    # Process each spider
    grand_total = 0
    grand_new = 0
    grand_skipped = 0

    for spider in spiders:
        processed, new, skipped = ingest_spider(
            spider, input_dir, output_dir, known_ids, dry_run=args.dry_run
        )
        grand_total += processed
        grand_new += new
        grand_skipped += skipped

    # Summary
    print()
    print("=" * 60)
    print("Entscheidsuche Ingest Complete")
    print("=" * 60)
    print(f"  Spiders processed: {len(spiders)}")
    print(f"  Total JSON files:  {grand_total:,}")
    print(f"  New decisions:     {grand_new:,}")
    print(f"  Skipped (dedup):   {grand_skipped:,}")
    print(f"  Output directory:  {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()

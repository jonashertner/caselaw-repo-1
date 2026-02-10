#!/usr/bin/env python3
"""
WEKO (Wettbewerbskommission) scraper - Swiss Competition Commission decisions.
Source: https://www.weko.admin.ch/weko/de/home/praxis/publizierte-entscheide.html

Single page with ~115 published decisions as PDF downloads.
Structure: div.mod-download > p > a (href=PDF, title=metadata)
Title format: "Case_name: Decision_type vom Date"

Crash-safe: JSONL append, skip already-downloaded PDFs.
"""

import json
import os
import re
import sys
import time
import hashlib
import logging
import requests
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# --- Configuration ---
HOST = "https://www.weko.admin.ch"
URL = "/weko/de/home/praxis/publizierte-entscheide.html"
BASE_DIR = Path("/opt/caselaw/data/weko")
DECISIONS_DIR = BASE_DIR / "decisions"
PDF_DIR = BASE_DIR / "pdfs"
JSONL_FILE = BASE_DIR / "weko_decisions.jsonl"
LOG_FILE = BASE_DIR / "weko_scraper.log"
DELAY = 1.0  # polite delay between PDF downloads

# --- Setup ---
DECISIONS_DIR.mkdir(parents=True, exist_ok=True)
PDF_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("weko")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; CaseLaw-Research/1.0; academic project)",
    "Accept-Language": "de-CH,de;q=0.9",
})


def load_existing_urls() -> set:
    """Load already-scraped PDF URLs from JSONL for crash-safety."""
    existing = set()
    if JSONL_FILE.exists():
        with open(JSONL_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if rec.get("pdf_url"):
                        existing.add(rec["pdf_url"])
                except json.JSONDecodeError:
                    continue
    return existing


def parse_title(title: str) -> dict:
    """
    Parse WEKO title format into structured metadata.
    
    Patterns:
      "Case_name: Decision_type vom Date"
      "Case_name: Decision_type vom Date (französisch)"
      "Case_name vom Date"
      Just a date or bare title
    """
    meta = {}
    
    # Extract language hint
    lang_match = re.search(r"\((französisch|italienisch|englisch)\)", title)
    if lang_match:
        lang_map = {"französisch": "fr", "italienisch": "it", "englisch": "en"}
        meta["language"] = lang_map.get(lang_match.group(1), "de")
        title_clean = title[:lang_match.start()].strip()
    else:
        meta["language"] = "de"
        title_clean = title.strip()
    
    # Try "Name: Type vom Date"
    colon_split = title_clean.split(": ", 1)
    if len(colon_split) > 1:
        meta["case_name"] = colon_split[0].strip()
        remainder = colon_split[1]
        vom_split = remainder.rsplit(" vom ", 1)
        if len(vom_split) > 1:
            meta["decision_type"] = vom_split[0].strip()
            meta["date_raw"] = vom_split[1].strip()
        else:
            # Try " du " for French decisions
            du_split = remainder.rsplit(" du ", 1)
            if len(du_split) > 1:
                meta["decision_type"] = du_split[0].strip()
                meta["date_raw"] = du_split[1].strip()
            else:
                meta["decision_type"] = remainder
    else:
        # No colon — try "Name vom Date"
        vom_split = title_clean.rsplit(" vom ", 1)
        if len(vom_split) > 1:
            meta["case_name"] = vom_split[0].strip()
            meta["date_raw"] = vom_split[1].strip()
        else:
            du_split = title_clean.rsplit(" du ", 1)
            if len(du_split) > 1:
                meta["case_name"] = du_split[0].strip()
                meta["date_raw"] = du_split[1].strip()
            else:
                meta["case_name"] = title_clean
    
    # Normalize date
    if "date_raw" in meta:
        meta["date_iso"] = normalize_date(meta["date_raw"])
    
    return meta


def normalize_date(date_str: str) -> str:
    """Try to parse various German/French date formats to ISO."""
    date_str = date_str.strip().rstrip(".")
    
    # German months
    month_map = {
        "januar": "01", "februar": "02", "märz": "03", "april": "04",
        "mai": "05", "juni": "06", "juli": "07", "august": "08",
        "september": "09", "oktober": "10", "november": "11", "dezember": "12",
        # French months
        "janvier": "01", "février": "02", "mars": "03", "avril": "04",
        "juin": "06", "juillet": "07", "août": "08",
        "octobre": "10", "novembre": "11", "décembre": "12",
        # Italian months  
        "gennaio": "01", "febbraio": "02", "marzo": "03", "aprile": "04",
        "maggio": "05", "giugno": "06", "luglio": "07", "agosto": "08",
        "settembre": "09", "ottobre": "10", "dicembre": "12",
    }
    
    # Try "DD. Month YYYY" or "DD Month YYYY"
    m = re.match(r"(\d{1,2})\.?\s+(\w+)\s+(\d{4})", date_str)
    if m:
        day = m.group(1).zfill(2)
        month_name = m.group(2).lower()
        year = m.group(3)
        month = month_map.get(month_name)
        if month:
            return f"{year}-{month}-{day}"
    
    # Try DD.MM.YYYY
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    
    # Try "D. Month YYYY" with period after day
    m = re.match(r"(\d{1,2})\.\s*(\w+)\s+(\d{4})", date_str)
    if m:
        day = m.group(1).zfill(2)
        month_name = m.group(2).lower()
        year = m.group(3)
        month = month_map.get(month_name)
        if month:
            return f"{year}-{month}-{day}"
    
    return ""


def download_pdf(url: str, filepath: Path) -> bool:
    """Download a PDF file. Returns True on success."""
    try:
        resp = SESSION.get(url, timeout=60, stream=True)
        resp.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        log.error(f"PDF download failed: {url} -> {e}")
        return False


def extract_text_from_pdf(filepath: Path) -> str:
    """Extract text from PDF using pdftotext (poppler) or pymupdf."""
    # Try pdftotext first (faster, better quality)
    try:
        import subprocess
        result = subprocess.run(
            ["pdftotext", "-layout", str(filepath), "-"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    
    # Fallback to pymupdf
    try:
        import fitz  # pymupdf
        doc = fitz.open(str(filepath))
        text = ""
        for page in doc:
            text += page.get_text() + "\n"
        doc.close()
        return text.strip()
    except ImportError:
        log.warning("Neither pdftotext nor pymupdf available for text extraction")
        return ""
    except Exception as e:
        log.warning(f"PDF text extraction failed for {filepath}: {e}")
        return ""


def scrape_weko():
    """Main scraper: fetch page, parse entries, download PDFs, extract text."""
    log.info("=" * 60)
    log.info("WEKO Scraper starting")
    log.info("=" * 60)
    
    # Load existing for crash-safety
    existing_urls = load_existing_urls()
    log.info(f"Already scraped: {len(existing_urls)} decisions")
    
    # Fetch the single listing page
    log.info(f"Fetching listing page: {HOST}{URL}")
    try:
        resp = SESSION.get(HOST + URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Failed to fetch listing page: {e}")
        return
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Parse all download entries
    download_divs = soup.find_all("div", class_="mod-download")
    log.info(f"Found {len(download_divs)} download entries")
    
    new_count = 0
    skip_count = 0
    error_count = 0
    
    for i, div in enumerate(download_divs):
        a_tag = div.find("a", href=re.compile(r"\.pdf", re.I))
        if not a_tag:
            log.warning(f"Entry {i+1}: no PDF link found")
            continue
        
        href = a_tag.get("href", "")
        title = a_tag.get("title", "").strip()
        link_text = a_tag.get_text(strip=True)
        
        # Build full URL
        if href.startswith("/"):
            pdf_url = HOST + href
        elif href.startswith("http"):
            pdf_url = href
        else:
            pdf_url = HOST + "/" + href
        
        # Skip if already scraped
        if pdf_url in existing_urls:
            skip_count += 1
            continue
        
        log.info(f"[{i+1}/{len(download_divs)}] Processing: {title[:80]}")
        
        # Parse metadata from title
        meta = parse_title(title)
        
        # Extract file size and publication date from link text
        # Format: "Title(PDF, 23 MB, 17.12.2025)"
        size_match = re.search(r"\(PDF,\s*([^,]+),\s*(\d{2}\.\d{2}\.\d{4})\)", link_text)
        if size_match:
            meta["file_size"] = size_match.group(1).strip()
            meta["publication_date"] = normalize_date(size_match.group(2))
        
        # Generate safe filename from URL hash
        url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:12]
        safe_name = re.sub(r'[^\w\-.]', '_', (meta.get("case_name", "") or "unknown")[:60])
        pdf_filename = f"weko_{safe_name}_{url_hash}.pdf"
        pdf_path = PDF_DIR / pdf_filename
        
        # Download PDF
        if not pdf_path.exists():
            success = download_pdf(pdf_url, pdf_path)
            if not success:
                error_count += 1
                continue
            time.sleep(DELAY)
        
        # Extract text
        full_text = extract_text_from_pdf(pdf_path)
        
        # Build record
        record = {
            "source": "WEKO",
            "court": "Wettbewerbskommission",
            "jurisdiction": "CH",
            "pdf_url": pdf_url,
            "page_url": HOST + URL,
            "title_raw": title,
            "case_name": meta.get("case_name", ""),
            "decision_type": meta.get("decision_type", ""),
            "date": meta.get("date_iso", ""),
            "date_raw": meta.get("date_raw", ""),
            "language": meta.get("language", "de"),
            "file_size": meta.get("file_size", ""),
            "publication_date": meta.get("publication_date", ""),
            "pdf_file": pdf_filename,
            "text": full_text[:500000] if full_text else "",  # cap at 500K chars
            "text_length": len(full_text),
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
        
        # Append to JSONL (crash-safe)
        with open(JSONL_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        
        new_count += 1
        log.info(f"  -> Saved: {meta.get('case_name', 'unknown')} | "
                 f"Date: {meta.get('date_iso', '?')} | "
                 f"Text: {len(full_text)} chars")
    
    log.info("=" * 60)
    log.info(f"WEKO Scraper complete")
    log.info(f"  New: {new_count}")
    log.info(f"  Skipped (already scraped): {skip_count}")
    log.info(f"  Errors: {error_count}")
    log.info(f"  Total in JSONL: {len(existing_urls) + new_count}")
    log.info("=" * 60)


if __name__ == "__main__":
    scrape_weko()
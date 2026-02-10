#!/usr/bin/env python3
"""
FINMA (Finanzmarktaufsicht) scraper - Swiss Financial Market Supervisory Authority.

Five collections:
1. Kasuistik (406 enforcement case reports) - API + detail page scraping
2. Court decisions (414 BVGer/BGer rulings on FINMA enforcement) - API + detail pages
3. Circulars/Rundschreiben (37 current) - API with direct PDF links
4. Rulings/Bulletins (48 selected rulings) - static page PDFs
5. Circular archive (276 archived circulars) - year pages with PDFs

API endpoint: POST /de/api/search/getresult with ds={GUID}
Crash-safe: JSONL append, skip already-scraped items.
"""

import json
import os
import re
import sys
import time
import hashlib
import logging
import subprocess
import requests
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# --- Configuration ---
HOST = "https://www.finma.ch"
API = "/de/api/search/getresult"
BASE_DIR = Path("/opt/caselaw/data/finma")
PDF_DIR = BASE_DIR / "pdfs"
JSONL_FILE = BASE_DIR / "finma_decisions.jsonl"
LOG_FILE = BASE_DIR / "finma_scraper.log"
DELAY = 0.8  # polite delay

SOURCES = {
    "kasuistik": "{2FBD0DFE-112F-4176-BE8D-07C2D0BE0903}",
    "court_decisions": "{4C699740-8893-4B35-B7D9-152A2702ABCD}",
    "circulars": "{3009DAA1-E9A3-4CF1-B0F0-8059B9A37AFA}",
}

RULINGS_URL = "/de/dokumentation/enforcementberichterstattung/ausgewaehlte-verfuegungen/"

ARCHIVE_YEARS = ["2008", "2009", "2010", "2011", "2012", "2013",
                 "2015", "2016", "2017", "2018", "2019", "2020"]
ARCHIVE_BASE = "/de/dokumentation/archiv/rundschreiben/archiv-{}/"
ARCHIVE_EXTRA = "/de/dokumentation/archiv/rundschreiben/weitere-dokumente/"

# --- Setup ---
PDF_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("finma")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; CaseLaw-Research/1.0; academic project)",
    "Accept-Language": "de-CH,de;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
})


def load_existing_ids() -> set:
    """Load already-scraped record IDs from JSONL."""
    existing = set()
    if JSONL_FILE.exists():
        with open(JSONL_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    rid = rec.get("record_id", "")
                    if rid:
                        existing.add(rid)
                except json.JSONDecodeError:
                    continue
    return existing


def save_record(record: dict):
    """Append a record to JSONL."""
    with open(JSONL_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def download_pdf(url: str, filepath: Path) -> bool:
    """Download a PDF file."""
    try:
        resp = SESSION.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        log.error(f"PDF download failed: {url} -> {e}")
        return False


def extract_text_from_pdf(filepath: Path) -> str:
    """Extract text using pdftotext."""
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(filepath), "-"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    try:
        import fitz
        doc = fitz.open(str(filepath))
        text = "".join(page.get_text() + "\n" for page in doc)
        doc.close()
        return text.strip()
    except:
        return ""


def make_pdf_filename(collection: str, title: str, url: str) -> str:
    """Generate safe PDF filename."""
    url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
    safe = re.sub(r'[^\w\-.]', '_', title[:50])
    return f"finma_{collection}_{safe}_{url_hash}.pdf"


def make_record_id(collection: str, identifier: str) -> str:
    """Generate unique record ID."""
    return f"finma_{collection}_{hashlib.md5(identifier.encode()).hexdigest()[:12]}"


# ============================================================
# 1. KASUISTIK - enforcement case reports
# ============================================================
def scrape_kasuistik(existing: set) -> int:
    """Scrape 406 kasuistik detail pages."""
    log.info("--- KASUISTIK: Fetching API listing ---")
    
    r = SESSION.post(HOST + API, data={"ds": SOURCES["kasuistik"]}, timeout=60)
    r.raise_for_status()
    data = r.json()
    items = data.get("Items", [])
    log.info(f"Kasuistik: {len(items)} items from API")
    
    new_count = 0
    for i, item in enumerate(items):
        link = item.get("Link", "")
        title = item.get("Title", "")
        record_id = make_record_id("kasuistik", link or title)
        
        if record_id in existing:
            continue
        
        log.info(f"[Kasuistik {i+1}/{len(items)}] {title}")
        
        # Scrape detail page
        detail = {"partei": "", "bereich": "", "thema": "", "zusammenfassung": "", "massnahmen": ""}
        if link:
            try:
                r2 = SESSION.get(HOST + link, timeout=30)
                if r2.status_code == 200:
                    soup = BeautifulSoup(r2.text, "html.parser")
                    # Parse table rows
                    for row in soup.find_all("tr"):
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            key = cells[0].get_text(strip=True).lower()
                            val = cells[1].get_text(strip=True)
                            if "partei" in key:
                                detail["partei"] = val
                            elif "bereich" in key:
                                detail["bereich"] = val
                            elif "thema" in key:
                                detail["thema"] = val
                            elif "zusammenfassung" in key:
                                detail["zusammenfassung"] = val
                            elif "massnahmen" in key or "mesures" in key:
                                detail["massnahmen"] = val
                time.sleep(DELAY)
            except Exception as e:
                log.warning(f"Detail page failed: {link} -> {e}")
        
        record = {
            "source": "FINMA",
            "collection": "kasuistik",
            "record_id": record_id,
            "court": "FINMA",
            "jurisdiction": "CH",
            "title": title,
            "page_url": HOST + link if link else "",
            "date": item.get("Date", ""),
            "facet": item.get("FacetColumn", ""),
            "partei": detail["partei"],
            "bereich": detail["bereich"],
            "thema": detail["thema"],
            "zusammenfassung": detail["zusammenfassung"],
            "massnahmen": detail["massnahmen"],
            "language": "de",
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
        
        save_record(record)
        new_count += 1
    
    return new_count


# ============================================================
# 2. COURT DECISIONS - BVGer/BGer on FINMA enforcement
# ============================================================
def scrape_court_decisions(existing: set) -> int:
    """Scrape 414 court decision detail pages."""
    log.info("--- COURT DECISIONS: Fetching API listing ---")
    
    r = SESSION.post(HOST + API, data={"ds": SOURCES["court_decisions"]}, timeout=60)
    r.raise_for_status()
    data = r.json()
    items = data.get("Items", [])
    log.info(f"Court decisions: {len(items)} items from API")
    
    new_count = 0
    for i, item in enumerate(items):
        link = item.get("Link", "")
        title = item.get("Title", "")
        record_id = make_record_id("court", link or title)
        
        if record_id in existing:
            continue
        
        log.info(f"[Court {i+1}/{len(items)}] {title}")
        
        # Scrape detail page
        detail = {
            "gericht": "", "status": "", "beschwerde_partei": "",
            "beschwerde_finma": "", "datum": "", "bereich": "",
            "thema": "", "zusammenfassung": "",
        }
        if link:
            try:
                r2 = SESSION.get(HOST + link, timeout=30)
                if r2.status_code == 200:
                    soup = BeautifulSoup(r2.text, "html.parser")
                    for row in soup.find_all("tr"):
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            key = cells[0].get_text(strip=True).lower()
                            val = cells[1].get_text(strip=True)
                            if "gericht" in key or "tribunal" in key:
                                detail["gericht"] = val
                            elif "status" in key:
                                detail["status"] = val
                            elif "beschwerde partei" in key or "recours partie" in key:
                                detail["beschwerde_partei"] = val
                            elif "beschwerde finma" in key or "recours finma" in key:
                                detail["beschwerde_finma"] = val
                            elif "datum" in key or "date" in key:
                                detail["datum"] = val
                            elif "bereich" in key or "domaine" in key:
                                detail["bereich"] = val
                            elif "thema" in key or "thème" in key:
                                detail["thema"] = val
                            elif "zusammenfassung" in key or "résumé" in key:
                                detail["zusammenfassung"] = val
                time.sleep(DELAY)
            except Exception as e:
                log.warning(f"Detail page failed: {link} -> {e}")
        
        record = {
            "source": "FINMA",
            "collection": "court_decisions",
            "record_id": record_id,
            "court": detail["gericht"] or "FINMA-related",
            "jurisdiction": "CH",
            "title": title,
            "page_url": HOST + link if link else "",
            "date": detail["datum"] or item.get("Date", ""),
            "facet": item.get("FacetColumn", ""),
            "gericht": detail["gericht"],
            "status": detail["status"],
            "beschwerde_partei": detail["beschwerde_partei"],
            "beschwerde_finma": detail["beschwerde_finma"],
            "bereich": detail["bereich"],
            "thema": detail["thema"],
            "zusammenfassung": detail["zusammenfassung"],
            "language": "de",
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
        
        save_record(record)
        new_count += 1
    
    return new_count


# ============================================================
# 3. CIRCULARS (current) - direct PDF links from API
# ============================================================
def scrape_circulars(existing: set) -> int:
    """Scrape 37 current circulars with PDFs."""
    log.info("--- CIRCULARS: Fetching API listing ---")
    
    r = SESSION.post(HOST + API, data={"ds": SOURCES["circulars"]}, timeout=60)
    r.raise_for_status()
    data = r.json()
    items = data.get("Items", [])
    log.info(f"Circulars: {len(items)} items from API")
    
    new_count = 0
    for i, item in enumerate(items):
        title = item.get("Title", "unknown")
        item_id = item.get("Id", "") or item.get("WatchlistUrl", "") or title
        record_id = make_record_id("circular", item_id)
        
        if record_id in existing:
            continue
        
        log.info(f"[Circular {i+1}/{len(items)}] {title}")
        
        # Get DE PDF URL from OtherLanguageLinks
        pdf_url = ""
        lang_links = item.get("OtherLanguageLinks", [])
        for ll in lang_links:
            if ll.get("Name") == "DE":
                pdf_url = ll.get("Url", "")
                break
        # Fallback: use Link field
        if not pdf_url:
            pdf_url = item.get("Link", "")
        
        if not pdf_url:
            log.warning(f"  No PDF URL for circular: {title}")
            continue
        
        full_url = HOST + pdf_url if pdf_url.startswith("/") else pdf_url
        
        # Download PDF
        pdf_filename = make_pdf_filename("circular", title, full_url)
        pdf_path = PDF_DIR / pdf_filename
        text = ""
        
        if not pdf_path.exists():
            if download_pdf(full_url, pdf_path):
                text = extract_text_from_pdf(pdf_path)
                time.sleep(DELAY)
            else:
                pdf_filename = ""
        else:
            text = extract_text_from_pdf(pdf_path)
        
        record = {
            "source": "FINMA",
            "collection": "circular",
            "record_id": record_id,
            "court": "FINMA",
            "jurisdiction": "CH",
            "title": title,
            "description": item.get("Description", ""),
            "pdf_url": full_url,
            "pdf_file": pdf_filename,
            "date": item.get("Date", ""),
            "file_size_mb": item.get("Size", ""),
            "language": "de",
            "text": text[:500000] if text else "",
            "text_length": len(text),
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
        
        save_record(record)
        new_count += 1
    
    return new_count


# ============================================================
# 4. RULINGS/BULLETINS - static page PDFs
# ============================================================
def scrape_rulings(existing: set) -> int:
    """Scrape ~48 selected ruling PDFs from bulletin page."""
    log.info("--- RULINGS: Fetching static page ---")
    
    r = SESSION.get(HOST + RULINGS_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    
    # Collect unique DE PDFs
    seen_hrefs = set()
    pdfs = []
    for a in soup.find_all("a", href=re.compile(r"\.pdf", re.I)):
        href = a.get("href", "")
        title = a.get("title", "") or a.get_text(strip=True)
        # Skip non-DE and portrait
        if href.startswith(("/fr/", "/it/", "/en/")):
            continue
        if "finma-ein-portrae" in href:
            continue
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)
        pdfs.append((href, title))
    
    log.info(f"Rulings: {len(pdfs)} unique DE PDFs")
    
    new_count = 0
    for i, (href, title) in enumerate(pdfs):
        full_url = HOST + href if href.startswith("/") else href
        record_id = make_record_id("ruling", href)
        
        if record_id in existing:
            continue
        
        log.info(f"[Ruling {i+1}/{len(pdfs)}] {title[:60]}")
        
        pdf_filename = make_pdf_filename("ruling", title, full_url)
        pdf_path = PDF_DIR / pdf_filename
        text = ""
        
        if not pdf_path.exists():
            if download_pdf(full_url, pdf_path):
                text = extract_text_from_pdf(pdf_path)
                time.sleep(DELAY)
            else:
                pdf_filename = ""
        else:
            text = extract_text_from_pdf(pdf_path)
        
        record = {
            "source": "FINMA",
            "collection": "ruling",
            "record_id": record_id,
            "court": "FINMA",
            "jurisdiction": "CH",
            "title": title,
            "pdf_url": full_url,
            "page_url": HOST + RULINGS_URL,
            "pdf_file": pdf_filename,
            "language": "de",
            "text": text[:500000] if text else "",
            "text_length": len(text),
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
        
        save_record(record)
        new_count += 1
    
    return new_count


# ============================================================
# 5. CIRCULAR ARCHIVE - year pages
# ============================================================
def scrape_circular_archive(existing: set) -> int:
    """Scrape ~276 archived circular PDFs from year pages."""
    log.info("--- CIRCULAR ARCHIVE: Scraping year pages ---")
    
    all_pdfs = []  # (url, title, year)
    
    # Year pages
    for year in ARCHIVE_YEARS:
        url = ARCHIVE_BASE.format(year)
        try:
            r = SESSION.get(HOST + url, timeout=30)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            seen = set()
            for a in soup.find_all("a", href=re.compile(r"\.pdf", re.I)):
                href = a.get("href", "")
                title = a.get("title", "") or a.get_text(strip=True)
                if href.startswith(("/fr/", "/it/", "/en/")):
                    continue
                if "finma-ein-portrae" in href:
                    continue
                if href in seen:
                    continue
                seen.add(href)
                all_pdfs.append((href, title, year))
            log.info(f"  Archive {year}: {len(seen)} PDFs")
        except Exception as e:
            log.warning(f"  Archive {year} failed: {e}")
    
    # Weitere Dokumente
    try:
        r = SESSION.get(HOST + ARCHIVE_EXTRA, timeout=30)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            seen = set()
            for a in soup.find_all("a", href=re.compile(r"\.pdf", re.I)):
                href = a.get("href", "")
                title = a.get("title", "") or a.get_text(strip=True)
                if href.startswith(("/fr/", "/it/", "/en/")):
                    continue
                if "finma-ein-portrae" in href:
                    continue
                if href in seen:
                    continue
                seen.add(href)
                all_pdfs.append((href, title, "misc"))
            log.info(f"  Weitere Dokumente: {len(seen)} PDFs")
    except Exception as e:
        log.warning(f"  Weitere Dokumente failed: {e}")
    
    log.info(f"Circular archive total: {len(all_pdfs)} PDFs")
    
    new_count = 0
    for i, (href, title, year) in enumerate(all_pdfs):
        full_url = HOST + href if href.startswith("/") else href
        record_id = make_record_id("archive_circular", href)
        
        if record_id in existing:
            continue
        
        log.info(f"[Archive {i+1}/{len(all_pdfs)}] {title[:60]}")
        
        pdf_filename = make_pdf_filename(f"archive_{year}", title, full_url)
        pdf_path = PDF_DIR / pdf_filename
        text = ""
        
        if not pdf_path.exists():
            if download_pdf(full_url, pdf_path):
                text = extract_text_from_pdf(pdf_path)
                time.sleep(DELAY)
            else:
                pdf_filename = ""
        else:
            text = extract_text_from_pdf(pdf_path)
        
        record = {
            "source": "FINMA",
            "collection": "archive_circular",
            "record_id": record_id,
            "court": "FINMA",
            "jurisdiction": "CH",
            "title": title,
            "pdf_url": full_url,
            "pdf_file": pdf_filename,
            "archive_year": year,
            "language": "de",
            "text": text[:500000] if text else "",
            "text_length": len(text),
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
        
        save_record(record)
        new_count += 1
    
    return new_count


# ============================================================
# MAIN
# ============================================================
def main():
    log.info("=" * 60)
    log.info("FINMA Scraper starting")
    log.info("=" * 60)
    
    existing = load_existing_ids()
    log.info(f"Already scraped: {len(existing)} records")
    
    totals = {}
    
    # 1. Kasuistik (detail pages - slowest, ~400 requests)
    totals["kasuistik"] = scrape_kasuistik(existing)
    existing = load_existing_ids()  # refresh
    
    # 2. Court decisions (detail pages - ~414 requests)
    totals["court_decisions"] = scrape_court_decisions(existing)
    existing = load_existing_ids()
    
    # 3. Circulars - current (37 PDFs)
    totals["circulars"] = scrape_circulars(existing)
    existing = load_existing_ids()
    
    # 4. Rulings/Bulletins (48 PDFs)
    totals["rulings"] = scrape_rulings(existing)
    existing = load_existing_ids()
    
    # 5. Circular archive (276 PDFs)
    totals["archive"] = scrape_circular_archive(existing)
    
    log.info("=" * 60)
    log.info("FINMA Scraper complete")
    for k, v in totals.items():
        log.info(f"  {k}: {v} new records")
    log.info(f"  Total new: {sum(totals.values())}")
    log.info(f"  Total in JSONL: {len(load_existing_ids())}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
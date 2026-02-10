#!/usr/bin/env python3
"""Deep probe FINMA - tables in scripts, circular subpages, accordion content."""
import requests
from bs4 import BeautifulSoup
import re
import json

HOST = "https://www.finma.ch"
s = requests.Session()
s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; Research/1.0)", "Accept-Language": "de-CH,de;q=0.9"})

# ============================================================
# 1. KASUISTIK - extract table from script
# ============================================================
print("=" * 70)
print("1. KASUISTIK TABLE EXTRACTION")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/enforcementberichterstattung/kasuistik/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# Find script containing table HTML
for sc in soup.find_all("script"):
    txt = sc.string or ""
    if "e-table" in txt and "Entscheid" in txt:
        # Parse the embedded HTML table
        table_soup = BeautifulSoup(txt, "html.parser")
        table = table_soup.find("table")
        if table:
            rows = table.find_all("tr")
            print(f"Table rows: {len(rows)} (inc header)")
            # Show header
            headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]
            print(f"Headers: {headers}")
            # Show first 5 data rows
            for row in rows[1:6]:
                cells = [td.get_text(strip=True)[:60] for td in row.find_all("td")]
                print(f"  {cells}")
            # Show last 3
            print("  ...")
            for row in rows[-3:]:
                cells = [td.get_text(strip=True)[:60] for td in row.find_all("td")]
                print(f"  {cells}")
        break

# ============================================================
# 2. COURT DECISIONS - extract table from script
# ============================================================
print(f"\n{'=' * 70}")
print("2. COURT DECISIONS TABLE EXTRACTION")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/enforcementberichterstattung/gerichtsentscheide/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

for sc in soup.find_all("script"):
    txt = sc.string or ""
    if "e-table" in txt and "Urteil" in txt:
        table_soup = BeautifulSoup(txt, "html.parser")
        table = table_soup.find("table")
        if table:
            rows = table.find_all("tr")
            print(f"Table rows: {len(rows)} (inc header)")
            headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]
            print(f"Headers: {headers}")
            for row in rows[1:6]:
                cells = [td.get_text(strip=True)[:60] for td in row.find_all("td")]
                print(f"  {cells}")
            print("  ...")
            for row in rows[-3:]:
                cells = [td.get_text(strip=True)[:60] for td in row.find_all("td")]
                print(f"  {cells}")
        break

# ============================================================
# 3. RULINGS - analyze accordion structure and PDF links
# ============================================================
print(f"\n{'=' * 70}")
print("3. RULINGS PAGE - ACCORDION ANALYSIS")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/enforcementberichterstattung/ausgewaehlte-verfuegungen/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# Find accordion sections
accordions = soup.find_all(class_=re.compile(r"accordion", re.I))
print(f"Accordion elements: {len(accordions)}")

for acc in accordions[:15]:
    # Get title
    title_el = acc.find(re.compile(r"h[2-4]")) or acc.find(class_=re.compile(r"title|header", re.I))
    title = title_el.get_text(strip=True) if title_el else "?"
    # Count PDFs inside
    pdfs = acc.find_all("a", href=re.compile(r"\.pdf", re.I))
    print(f"\n  Accordion: {title[:60]} | PDFs: {len(pdfs)}")
    for a in pdfs[:3]:
        print(f"    -> {a.get('title', a.get_text(strip=True))[:70]}")
        print(f"       {a.get('href', '')[:80]}")

# Also count PDFs NOT in accordions
all_pdfs = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
acc_pdfs = set()
for acc in accordions:
    for a in acc.find_all("a", href=re.compile(r"\.pdf", re.I)):
        acc_pdfs.add(a.get("href", ""))
non_acc = [a for a in all_pdfs if a.get("href", "") not in acc_pdfs]
print(f"\nTotal PDFs: {len(all_pdfs)}, In accordions: {len(acc_pdfs)}, Outside: {len(non_acc)}")

# ============================================================
# 4. CIRCULARS - check subpages
# ============================================================
print(f"\n{'=' * 70}")
print("4. CIRCULARS - SUBPAGE ANALYSIS")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/rundschreiben/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# Find all links to individual circulars
circular_links = set()
for a in soup.find_all("a", href=True):
    href = a.get("href", "")
    if "/rundschreiben/" in href and href != "/de/dokumentation/rundschreiben/":
        circular_links.add(href)

print(f"Circular sublinks: {len(circular_links)}")
for link in sorted(circular_links)[:20]:
    print(f"  {link[:100]}")

# Check archive page
print(f"\n--- Circulars Archive ---")
r2 = s.get(HOST + "/de/dokumentation/rundschreiben/archiv/", timeout=30)
if r2.status_code == 200:
    soup2 = BeautifulSoup(r2.text, "html.parser")
    pdfs2 = soup2.find_all("a", href=re.compile(r"\.pdf", re.I))
    print(f"Archive page: {r2.status_code}, PDFs: {len(pdfs2)}")
    circ_links2 = set()
    for a in soup2.find_all("a", href=True):
        href = a.get("href", "")
        if "/rundschreiben/" in href:
            circ_links2.add(href)
    print(f"Archive sublinks: {len(circ_links2)}")
    for link in sorted(circ_links2)[:15]:
        print(f"  {link[:100]}")
else:
    print(f"Archive: {r2.status_code}")

# ============================================================
# 5. Try Art 34 with different URL patterns
# ============================================================
print(f"\n{'=' * 70}")
print("5. ART 34 - TRYING ALTERNATE URLS")
print("=" * 70)

art34_tries = [
    "/de/durchsetzung/enforcementinstrumente/",
    "/de/durchsetzung/kasuistik-und-gerichtsentscheide/",
    "/de/durchsetzung/",
]
for url in art34_tries:
    r = s.get(HOST + url, timeout=15)
    print(f"  {url}: {r.status_code}")
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        pdfs = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
        h2s = [h.get_text(strip=True)[:40] for h in soup.find_all("h2")]
        print(f"    PDFs: {len(pdfs)}, H2s: {h2s[:5]}")

# ============================================================
# 6. Probe one circular detail page
# ============================================================
print(f"\n{'=' * 70}")
print("6. SAMPLE CIRCULAR DETAIL PAGE")
print("=" * 70)

# Try a known circular path pattern
sample_paths = [
    "/de/dokumentation/rundschreiben/2023-1/",
    "/de/dokumentation/rundschreiben/2025-2/",
    "/de/dokumentation/rundschreiben/2017-1/",
]
for path in sample_paths:
    r = s.get(HOST + path, timeout=15)
    print(f"\n  {path}: {r.status_code}")
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        pdfs = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
        title = soup.find("h1")
        print(f"    Title: {title.get_text(strip=True)[:80] if title else '?'}")
        print(f"    PDFs: {len(pdfs)}")
        for a in pdfs[:5]:
            print(f"      {a.get_text(strip=True)[:60]} -> {a.get('href', '')[:80]}")
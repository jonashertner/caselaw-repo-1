#!/usr/bin/env python3
"""Probe FINMA JS for API endpoints, get PDF inventories."""
import requests
from bs4 import BeautifulSoup
import re
import json

HOST = "https://www.finma.ch"
s = requests.Session()
s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; Research/1.0)", "Accept-Language": "de-CH,de;q=0.9"})

# ============================================================
# 1. Examine app.min.js for API patterns
# ============================================================
print("=" * 70)
print("1. JS BUNDLE ANALYSIS")
print("=" * 70)

r = s.get(HOST + "/Frontend/Finma/app.min.js?v=3.5.0", timeout=30)
print(f"JS bundle: {r.status_code}, {len(r.text)} chars")

js = r.text

# Search for API-related patterns
patterns = [
    (r'["\']([^"\']*api[^"\']*)["\']', "API URLs"),
    (r'["\']([^"\']*\.ashx[^"\']*)["\']', "ASHX handlers"),
    (r'["\']([^"\']*sitecore[^"\']*)["\']', "Sitecore URLs"),
    (r'["\']([^"\']*/-/[^"\']*)["\']', "Sitecore media/API"),
    (r'["\']([^"\']*search[^"\']*)["\']', "Search URLs"),
    (r'data-source[^}]*', "data-source usage"),
    (r'(tableData|tableItems|loadData|fetchData|getData)[^;]{0,100}', "Data loading"),
    (r'(\.ajax|\.get|\.post|fetch)\([^)]{0,200}\)', "AJAX calls"),
    (r'(kasuistik|enforcement|rundschreib)', "Domain terms"),
    (r'["\']([^"\']*json[^"\']*)["\']', "JSON endpoints"),
]

for pattern, desc in patterns:
    matches = re.findall(pattern, js, re.I)
    unique = list(set(matches))[:8]
    if unique:
        print(f"\n  {desc} ({len(matches)} matches):")
        for m in unique:
            print(f"    {str(m)[:150]}")

# Look for the table initialization code
table_patterns = [
    r'e-table[^}]{0,500}',
    r'table-sorting[^}]{0,500}',
    r'vertical-sorting[^}]{0,500}',
    r'data-source[^}]{0,300}',
]
for p in table_patterns:
    matches = re.findall(p, js, re.I)
    if matches:
        print(f"\n  Table pattern [{p[:30]}]:")
        for m in matches[:3]:
            print(f"    {m[:200]}")

# ============================================================
# 2. Try Sitecore API patterns with the known GUID
# ============================================================
print(f"\n{'=' * 70}")
print("2. SITECORE API ATTEMPTS")
print("=" * 70)

guid = "2FBD0DFE-112F-4176-BE8D-07C2D0BE0903"
sitecore_tries = [
    f"/-/item/v1/?sc_itemid={{{guid}}}",
    f"/sitecore/api/ssc/item/{{{guid}}}",
    f"/-/item/v1/{{{guid}}}",
    f"/api/sitecore/item/{guid}",
    f"/sitecore/api/layout/render/jss?item={{{guid}}}",
    f"/-/media/{guid}",
]

for url in sitecore_tries:
    try:
        r = s.get(HOST + url, timeout=10, headers={"Accept": "application/json"})
        ct = r.headers.get("content-type", "")
        print(f"  {url[:70]}: {r.status_code} | {ct[:30]} | {len(r.text)}")
        if r.status_code == 200 and len(r.text) > 10:
            print(f"    {r.text[:200]}")
    except Exception as e:
        print(f"  {url[:70]}: {e}")

# ============================================================
# 3. RULINGS - full PDF inventory (deduplicated)
# ============================================================
print(f"\n{'=' * 70}")
print("3. RULINGS - FULL PDF INVENTORY")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/enforcementberichterstattung/ausgewaehlte-verfuegungen/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# Collect ALL unique DE PDF links
all_pdfs = {}
for a in soup.find_all("a", href=re.compile(r"\.pdf", re.I)):
    href = a.get("href", "")
    title = a.get("title", "") or a.get_text(strip=True)
    # Only German versions, skip /fr/ /it/ /en/ paths
    if href.startswith("/fr/") or href.startswith("/it/") or href.startswith("/en/"):
        continue
    if "finma-ein-portrae" in href:  # Skip the portrait PDF (site-wide)
        continue
    if href not in all_pdfs:
        all_pdfs[href] = title

print(f"Unique DE PDFs (excl portrait): {len(all_pdfs)}")
for href, title in list(all_pdfs.items())[:20]:
    print(f"  {title[:60]} -> {href[:80]}")

# ============================================================
# 4. CIRCULARS - current circulars on main page
# ============================================================
print(f"\n{'=' * 70}")
print("4. CIRCULARS - MAIN PAGE STRUCTURE")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/rundschreiben/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# Look for the actual circular listings - they might be in a specific section
# Find the content area between nav elements
content = soup.find("div", class_="content-main") or soup.find("main") or soup

# Look for specific RS number patterns in text
rs_pattern = re.compile(r'(RS\s*\d{4}/\d+|\d{4}/\d{1,2}\s*FINMA-Rundschreiben)', re.I)
for text_node in content.stripped_strings:
    if rs_pattern.search(text_node):
        print(f"  RS mention: {text_node[:100]}")

# Try to find circular entries - look for structured content
# Check for any embedded table or list structure
for sc in soup.find_all("script"):
    txt = sc.string or ""
    if len(txt) > 500 and ("rundschreib" in txt.lower() or "e-table" in txt.lower()):
        print(f"\n  Script with circular content ({len(txt)} chars)")
        # Try to parse table
        if "<table" in txt:
            tsoup = BeautifulSoup(txt, "html.parser")
            table = tsoup.find("table")
            if table:
                rows = table.find_all("tr")
                print(f"  Table: {len(rows)} rows")
                for row in rows[:8]:
                    cells = [td.get_text(strip=True)[:40] for td in row.find_all(["td", "th"])]
                    links = [a.get("href", "")[:60] for a in row.find_all("a")]
                    print(f"    {cells} | {links}")

# Also check the appendices page
print(f"\n--- Anhänge page ---")
r2 = s.get(HOST + "/de/dokumentation/rundschreiben/anhaenge/", timeout=15)
if r2.status_code == 200:
    soup2 = BeautifulSoup(r2.text, "html.parser")
    pdfs2 = soup2.find_all("a", href=re.compile(r"\.pdf", re.I))
    print(f"  Anhänge: {len(pdfs2)} PDFs")

# ============================================================
# 5. Check if circulars have a Sitecore table too
# ============================================================
print(f"\n{'=' * 70}")
print("5. CIRCULARS - CHECKING FOR SITECORE TABLE")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/rundschreiben/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# Find ALL data-source elements
for el in soup.find_all(attrs={"data-source": True}):
    ds = el.get("data-source", "")
    cls = el.get("class", [])
    print(f"  data-source: {ds} | class: {cls}")

# Find all script tags and check for table templates
for i, sc in enumerate(soup.find_all("script")):
    txt = sc.string or ""
    if "{{=" in txt:
        print(f"\n  Script {i} has templates ({len(txt)} chars):")
        print(f"    {txt[:300]}")
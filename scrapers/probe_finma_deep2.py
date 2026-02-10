#!/usr/bin/env python3
"""Probe FINMA API endpoints and circular structure."""
import requests
from bs4 import BeautifulSoup
import re
import json

HOST = "https://www.finma.ch"
s = requests.Session()
s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; Research/1.0)", "Accept-Language": "de-CH,de;q=0.9"})

# ============================================================
# 1. Find API endpoint for Kasuistik table
# ============================================================
print("=" * 70)
print("1. KASUISTIK - FINDING DATA SOURCE")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/enforcementberichterstattung/kasuistik/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# Look for data-url, data-source, ng-*, or API URLs in all scripts
for sc in soup.find_all("script", src=True):
    src = sc.get("src", "")
    if "bundle" in src or "app" in src or "main" in src:
        print(f"  JS bundle: {src[:100]}")

for sc in soup.find_all("script"):
    txt = sc.string or ""
    # Look for API URLs or config
    for pattern in [r'(https?://[^\s"\']+api[^\s"\']*)', r'(https?://[^\s"\']+/de/suche[^\s"\']*)',
                    r'(/api/[^\s"\']+)', r'(data-url[^\n]+)', r'(searchUrl[^\n]+)',
                    r'(serviceUrl[^\n]+)', r'(apiUrl[^\n]+)', r'(\.json[^\s"\']*)',
                    r'(fetch\([^\)]+\))', r'(XMLHttpRequest)', r'(\$\.ajax)',
                    r'(kasuistik[^\s"\']*\.json)', r'(enforcement[^\s"\']*\.json)']: 
        matches = re.findall(pattern, txt, re.I)
        for m in matches[:3]:
            print(f"  Pattern [{pattern[:20]}]: {m[:120]}")
    
    # Check for the template binding and surrounding context
    if "{{=item" in txt:
        # Get 500 chars around the template
        idx = txt.find("{{=item")
        start = max(0, idx - 300)
        end = min(len(txt), idx + 300)
        context = txt[start:end].replace("\n", " ").replace("  ", " ")
        print(f"\n  Template context:\n    {context[:500]}")

# Check for data-source attributes
for el in soup.find_all(attrs={"data-source": True}):
    print(f"  data-source: {el.get('data-source', '')[:100]}")

for el in soup.find_all(attrs={"data-url": True}):
    print(f"  data-url: {el.get('data-url', '')[:100]}")
    # Also get nearby attributes
    print(f"    class: {el.get('class', '')}")
    print(f"    all attrs: {dict(el.attrs)}")

# ============================================================
# 2. Try common API patterns
# ============================================================
print(f"\n{'=' * 70}")
print("2. TRYING API ENDPOINTS")
print("=" * 70)

api_tries = [
    "/api/enforcement/kasuistik",
    "/de/suche?query=&category=kasuistik",
    "/de/suche?category=kasuistik&format=json",
    "/-/api/enforcement/kasuistik",
    "/api/v1/enforcement",
    "/de/dokumentation/enforcementberichterstattung/kasuistik/?format=json",
    "/de/suche",
]

for url in api_tries:
    try:
        r = s.get(HOST + url, timeout=10, headers={"Accept": "application/json"})
        ct = r.headers.get("content-type", "")
        print(f"  {url[:60]}: {r.status_code} | CT: {ct[:40]} | Len: {len(r.text)}")
        if r.status_code == 200 and "json" in ct:
            try:
                data = r.json()
                if isinstance(data, dict):
                    print(f"    Keys: {list(data.keys())[:10]}")
                elif isinstance(data, list):
                    print(f"    List of {len(data)} items")
                    if data:
                        print(f"    First: {json.dumps(data[0], ensure_ascii=False)[:200]}")
            except:
                print(f"    Body start: {r.text[:200]}")
        elif r.status_code == 200:
            print(f"    Body start: {r.text[:200]}")
    except Exception as e:
        print(f"  {url}: {e}")

# ============================================================
# 3. CIRCULARS - main page full structure
# ============================================================
print(f"\n{'=' * 70}")
print("3. CIRCULARS - FULL PAGE LINK STRUCTURE")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/rundschreiben/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# Get ALL links with text
main_content = soup.find("main") or soup.find("div", class_="content") or soup
all_links = main_content.find_all("a", href=True)
print(f"Total links in content: {len(all_links)}")

# Find links that look like circulars (RS numbers)
rs_links = []
for a in all_links:
    href = a.get("href", "")
    text = a.get_text(strip=True)
    if re.search(r"rundschreib|RS\s*\d|circular|media.*rundschreib", href + text, re.I):
        rs_links.append((href, text[:80]))
        
print(f"\nCircular-related links: {len(rs_links)}")
for href, text in rs_links[:25]:
    print(f"  {text[:50]:50s} -> {href[:80]}")

# Check for table or list structure with circular entries
tables = soup.find_all("table")
for t in tables:
    rows = t.find_all("tr")
    print(f"\nTable with {len(rows)} rows")
    for row in rows[:5]:
        cells = [td.get_text(strip=True)[:40] for td in row.find_all(["td", "th"])]
        print(f"  {cells}")

# Check for script-embedded tables (like kasuistik)
for sc in soup.find_all("script"):
    txt = sc.string or ""
    if "e-table" in txt or "rundschreib" in txt.lower()[:500]:
        print(f"\n  Script with table/circular content found, length: {len(txt)}")
        # Extract table if present
        if "<table" in txt:
            tsoup = BeautifulSoup(txt, "html.parser")
            table = tsoup.find("table")
            if table:
                rows = table.find_all("tr")
                print(f"  Embedded table: {len(rows)} rows")
                for row in rows[:5]:
                    cells = [td.get_text(strip=True)[:40] for td in row.find_all(["td", "th"])]
                    links = [a.get("href", "")[:60] for a in row.find_all("a")]
                    print(f"    {cells} | Links: {links}")

# ============================================================
# 4. CIRCULARS ARCHIVE - probe year pages
# ============================================================
print(f"\n{'=' * 70}")
print("4. CIRCULARS ARCHIVE - YEAR PAGES")
print("=" * 70)

for year in ["2008", "2013", "2017", "2020"]:
    url = f"/de/dokumentation/archiv/rundschreiben/archiv-{year}/"
    r = s.get(HOST + url, timeout=15)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        pdfs = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
        print(f"\n  Archive {year}: {len(pdfs)} PDFs")
        for a in pdfs[:5]:
            print(f"    {a.get_text(strip=True)[:60]} -> {a.get('href', '')[:80]}")
    else:
        print(f"  Archive {year}: {r.status_code}")

# ============================================================
# 5. ENFORCEMENT page (potential Art 34)
# ============================================================
print(f"\n{'=' * 70}")
print("5. ENFORCEMENT PAGE DETAIL")
print("=" * 70)

r = s.get(HOST + "/de/durchsetzung/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# All PDF links with context
pdfs = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
print(f"PDFs on /de/durchsetzung/: {len(pdfs)}")
for a in pdfs[:15]:
    title = a.get("title", "") or a.get_text(strip=True)
    href = a.get("href", "")
    print(f"  {title[:60]} -> {href[:80]}")

# Check for Art 34 table
for sc in soup.find_all("script"):
    txt = sc.string or ""
    if "e-table" in txt or "art" in txt.lower()[:300]:
        if "<table" in txt:
            tsoup = BeautifulSoup(txt, "html.parser")
            table = tsoup.find("table")
            if table:
                rows = table.find_all("tr")
                print(f"\n  Art 34 table: {len(rows)} rows")
                headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]
                print(f"  Headers: {headers}")
                for row in rows[1:4]:
                    cells = [td.get_text(strip=True)[:40] for td in row.find_all("td")]
                    print(f"    {cells}")
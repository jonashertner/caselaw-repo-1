#!/usr/bin/env python3
"""Get full FINMA API data: counts, pagination, detail page structure."""
import requests
import json

HOST = "https://www.finma.ch"
API = "/de/api/search/getresult"
s = requests.Session()
s.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; Research/1.0)",
    "Accept-Language": "de-CH,de;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
})

SOURCES = {
    "kasuistik": "{2FBD0DFE-112F-4176-BE8D-07C2D0BE0903}",
    "court": "{4C699740-8893-4B35-B7D9-152A2702ABCD}",
    "circulars": "{3009DAA1-E9A3-4CF1-B0F0-8059B9A37AFA}",
}

for name, guid in SOURCES.items():
    print(f"\n{'='*70}")
    print(f"=== {name.upper()}")
    print(f"{'='*70}")
    
    r = s.post(HOST + API, data={"ds": guid}, timeout=30)
    data = r.json()
    
    items = data.get("Items", [])
    print(f"Items: {len(items)}")
    print(f"Count: {data.get('Count')}")
    print(f"MaxResultCount: {data.get('MaxResultCount')}")
    print(f"ResultsPerPage: {data.get('ResultsPerPage')}")
    print(f"NextPageLink: {data.get('NextPageLink')}")
    print(f"Skip: {data.get('Skip')}")
    
    # Show first 3 items fully
    for i, item in enumerate(items[:3]):
        print(f"\n  Item {i+1}: {json.dumps(item, ensure_ascii=False)[:500]}")
    
    # Show last item
    if len(items) > 3:
        print(f"\n  Item {len(items)}: {json.dumps(items[-1], ensure_ascii=False)[:500]}")
    
    # For kasuistik and court: probe a detail page
    if name in ("kasuistik", "court") and items:
        link = items[0].get("Link", "")
        if link:
            print(f"\n  --- Detail page: {link} ---")
            from bs4 import BeautifulSoup
            import re
            r2 = s.get(HOST + link, timeout=15)
            print(f"  Status: {r2.status_code}, Length: {len(r2.text)}")
            if r2.status_code == 200:
                soup = BeautifulSoup(r2.text, "html.parser")
                # Title
                h1 = soup.find("h1")
                print(f"  H1: {h1.get_text(strip=True)[:100] if h1 else '?'}")
                # Content
                content = soup.find("div", class_="content-main") or soup.find("main")
                if content:
                    # Get text paragraphs
                    paras = content.find_all("p")
                    print(f"  Paragraphs: {len(paras)}")
                    for p in paras[:5]:
                        txt = p.get_text(strip=True)
                        if txt:
                            print(f"    {txt[:120]}")
                # PDFs on detail page
                pdfs = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
                print(f"  PDFs: {len(pdfs)}")
                for a in pdfs[:3]:
                    print(f"    {a.get_text(strip=True)[:60]} -> {a.get('href', '')[:80]}")
                # Check for tables with case info
                tables = soup.find_all("table")
                for t in tables[:2]:
                    rows = t.find_all("tr")
                    print(f"  Table: {len(rows)} rows")
                    for row in rows[:5]:
                        cells = [td.get_text(strip=True)[:40] for td in row.find_all(["td", "th"])]
                        print(f"    {cells}")

# Check pagination for kasuistik
print(f"\n{'='*70}")
print("PAGINATION TEST - KASUISTIK")
print(f"{'='*70}")
# Try with skip parameter
for skip in [0, 100, 200]:
    r = s.post(HOST + API, data={"ds": SOURCES["kasuistik"], "skip": skip}, timeout=30)
    data = r.json()
    items = data.get("Items", [])
    print(f"  skip={skip}: Items={len(items)}, Count={data.get('Count')}, Next={data.get('NextPageLink')}")
    if items:
        print(f"    First: {items[0].get('Title', '?')}, Last: {items[-1].get('Title', '?')}")
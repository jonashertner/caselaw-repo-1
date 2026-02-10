#!/usr/bin/env python3
"""Probe FINMA document pages for structure analysis."""
import requests
from bs4 import BeautifulSoup
import re
import json

HOST = "https://www.finma.ch"
s = requests.Session()
s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; Research/1.0)", "Accept-Language": "de-CH,de;q=0.9"})

PAGES = {
    "rulings": "/de/dokumentation/enforcementberichterstattung/ausgewaehlte-verfuegungen/",
    "circulars": "/de/dokumentation/rundschreiben/",
    "case_reports": "/de/dokumentation/enforcementberichterstattung/kasuistik/",
    "court_decisions": "/de/dokumentation/enforcementberichterstattung/gerichtsentscheide/",
    "art34": "/de/dokumentation/enforcementberichterstattung/veroeffentlichung-nach-art-34-finmag/",
    "guidance": "/de/dokumentation/wegleitungen/",
}

for name, path in PAGES.items():
    print(f"\n{'='*70}")
    print(f"=== {name.upper()}: {HOST}{path}")
    print(f"{'='*70}")
    try:
        r = s.get(HOST + path, timeout=30)
        print(f"Status: {r.status_code}, Length: {len(r.text)}")
        
        if r.status_code != 200:
            print(f"  ERROR: {r.status_code}")
            continue
        
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Check for Nuxt
        nuxt = soup.find("script", id="__NUXT_DATA__")
        if nuxt:
            print("  *** NUXT SPA detected")
        
        # Check for dynamic loading
        dynamic = soup.find_all("div", attrs={"data-url": True})
        if dynamic:
            print(f"  Dynamic divs: {len(dynamic)}")
            for d in dynamic[:3]:
                print(f"    data-url: {d.get('data-url', '')[:100]}")
        
        # PDF links
        pdfs = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
        print(f"  PDF links: {len(pdfs)}")
        for a in pdfs[:5]:
            print(f"    {a.get('title', a.get_text(strip=True))[:80]} -> {a.get('href', '')[:80]}")
        
        # Check for tables
        tables = soup.find_all("table")
        print(f"  Tables: {len(tables)}")
        
        # Check for list/article structures
        articles = soup.find_all("article")
        print(f"  Articles: {len(articles)}")
        
        # Check for accordion/collapsible
        accordions = soup.find_all(class_=re.compile(r"accordion|collaps|toggle", re.I))
        print(f"  Accordions: {len(accordions)}")
        
        # H2 headings for structure
        h2s = soup.find_all("h2")
        print(f"  H2s: {[h.get_text(strip=True)[:40] for h in h2s[:8]]}")
        
        # Links to subpages
        internal_links = soup.find_all("a", href=re.compile(r"^/de/"))
        unique_paths = set()
        for a in internal_links:
            href = a.get("href", "")
            if any(x in href for x in ["verfueg", "rundschreib", "kasuistik", "bulletin", "enforcement"]):
                unique_paths.add(href)
        if unique_paths:
            print(f"  Relevant sublinks ({len(unique_paths)}):")
            for p in sorted(unique_paths)[:10]:
                print(f"    {p[:100]}")
        
        # Look for JS data
        scripts = soup.find_all("script")
        for sc in scripts:
            txt = sc.string or ""
            if "json" in txt.lower()[:200] or "data" in txt.lower()[:200] or "fetch" in txt.lower()[:200]:
                print(f"  Script hint: {txt[:200]}")
                
    except Exception as e:
        print(f"  EXCEPTION: {e}")

# Also check the enforcement bulletin archive
print(f"\n{'='*70}")
print("=== ENFORCEMENT BULLETINS (historical)")
print(f"{'='*70}")
for i in range(1, 7):
    url = f"{HOST}/de/dokumentation/enforcementberichterstattung/enforcement-bericht-{i}/"
    try:
        r = s.get(url, timeout=15)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            pdfs = soup.find_all("a", href=re.compile(r"\.pdf", re.I))
            print(f"  Bulletin {i}: {r.status_code}, {len(pdfs)} PDFs")
        else:
            print(f"  Bulletin {i}: {r.status_code}")
    except:
        print(f"  Bulletin {i}: failed")
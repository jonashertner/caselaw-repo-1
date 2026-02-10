#!/usr/bin/env python3
"""Final FINMA probe - find apiUrl data attributes, circular structure."""
import requests
from bs4 import BeautifulSoup
import re

HOST = "https://www.finma.ch"
s = requests.Session()
s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; Research/1.0)", "Accept-Language": "de-CH,de;q=0.9"})

# ============================================================
# 1. Find ALL data-* attributes on Kasuistik page
# ============================================================
print("=" * 70)
print("1. KASUISTIK - ALL DATA ATTRIBUTES")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/enforcementberichterstattung/kasuistik/", timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# Find elements with any data- attribute containing 'api' or 'url' or 'source'
for el in soup.find_all(True):
    for attr, val in el.attrs.items():
        if isinstance(attr, str) and attr.startswith("data-") and isinstance(val, str):
            if any(k in attr.lower() for k in ["api", "url", "source", "endpoint", "href", "ajax", "load", "fetch"]):
                print(f"  {el.name}.{attr} = {val[:150]}")
            # Also check for GUIDs or URLs in any data attribute
            if re.search(r'[{/]', str(val)) and len(str(val)) > 5:
                if attr not in ("data-url", "data-source"):  # already seen
                    print(f"  {el.name}.{attr} = {val[:150]}")

# Find the specific div/section containing the template table
for sc in soup.find_all("script"):
    txt = sc.string or ""
    if "{{=item" in txt:
        # Get the parent container
        parent = sc.parent
        print(f"\n  Template script parent: <{parent.name} class='{parent.get('class', '')}'")
        # Check ALL attributes of parent and ancestors
        for ancestor in [parent] + list(parent.parents)[:5]:
            if ancestor.name:
                relevant = {k: v for k, v in ancestor.attrs.items() 
                           if isinstance(k, str) and k.startswith("data-")}
                if relevant:
                    print(f"    <{ancestor.name}> data attrs: {relevant}")

# Also dump raw HTML around data-source
html = r.text
idx = html.find("2FBD0DFE")
if idx > -1:
    context = html[max(0, idx-500):idx+200]
    print(f"\n  Context around GUID:\n{context}")

# ============================================================
# 2. Same for Court Decisions page
# ============================================================
print(f"\n{'=' * 70}")
print("2. COURT DECISIONS - DATA ATTRIBUTES")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/enforcementberichterstattung/gerichtsentscheide/", timeout=30)
html = r.text

# Find ALL data-source GUIDs
guids = re.findall(r'data-source="(\{[^"]+\})"', html)
print(f"  data-source GUIDs: {guids}")

# Find data-apiurl or similar
api_attrs = re.findall(r'data-(?:api[-_]?url|apiurl|api|endpoint|load)="([^"]+)"', html, re.I)
print(f"  API attributes: {api_attrs}")

# Find the table container with all its attributes
soup = BeautifulSoup(html, "html.parser")
for el in soup.find_all(attrs={"data-source": True}):
    # Get ALL attributes
    print(f"\n  Element with data-source:")
    print(f"    Tag: {el.name}")
    print(f"    All attrs: {dict(el.attrs)}")
    # Check children for more data attrs
    for child in el.find_all(True, recursive=True):
        data_attrs = {k: v for k, v in child.attrs.items() if str(k).startswith("data-")}
        if data_attrs:
            print(f"    Child <{child.name}>: {data_attrs}")

# ============================================================
# 3. Art 34 on durchsetzung page
# ============================================================
print(f"\n{'=' * 70}")
print("3. ART 34 / DURCHSETZUNG - DATA ATTRIBUTES")
print("=" * 70)

r = s.get(HOST + "/de/durchsetzung/", timeout=30)
html = r.text
soup = BeautifulSoup(html, "html.parser")

guids = re.findall(r'data-source="(\{[^"]+\})"', html)
print(f"  data-source GUIDs: {guids}")

for el in soup.find_all(attrs={"data-source": True}):
    print(f"\n  Element with data-source:")
    print(f"    Tag: {el.name}")
    all_attrs = dict(el.attrs)
    print(f"    All attrs: {all_attrs}")
    for child in el.find_all(True, recursive=True):
        data_attrs = {k: v for k, v in child.attrs.items() if str(k).startswith("data-")}
        if data_attrs and any(k != "data-sort" and k != "data-sort-value" and k != "data-title" for k in data_attrs):
            print(f"    Child <{child.name}>: {data_attrs}")

# ============================================================
# 4. CIRCULARS - main page, find the filter module
# ============================================================
print(f"\n{'=' * 70}")
print("4. CIRCULARS - FILTER MODULE ANALYSIS")
print("=" * 70)

r = s.get(HOST + "/de/dokumentation/rundschreiben/", timeout=30)
html = r.text
soup = BeautifulSoup(html, "html.parser")

# Find the mod-filter element
for el in soup.find_all(class_=re.compile(r"mod-filter")):
    print(f"  mod-filter element: <{el.name}>")
    print(f"  All attrs: {dict(el.attrs)}")
    # Get all children with data attrs
    for child in el.find_all(True, recursive=True):
        data_attrs = {k: v for k, v in child.attrs.items() if str(k).startswith("data-")}
        if data_attrs:
            print(f"    <{child.name}>: {data_attrs}")
    # Get first 2000 chars of inner HTML
    inner = el.decode_contents()[:2000]
    print(f"\n  Inner HTML (first 2000):\n{inner}")

# ============================================================
# 5. Circular archive - count all year pages
# ============================================================
print(f"\n{'=' * 70}")
print("5. CIRCULAR ARCHIVE - ALL YEARS INVENTORY")
print("=" * 70)

years = ["2008", "2009", "2010", "2011", "2012", "2013", "2015", "2016", "2017", "2018", "2019", "2020"]
total_pdfs = 0
for year in years:
    url = f"/de/dokumentation/archiv/rundschreiben/archiv-{year}/"
    try:
        r = s.get(HOST + url, timeout=15)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            # Count only DE PDFs
            de_pdfs = [a for a in soup.find_all("a", href=re.compile(r"\.pdf", re.I))
                      if not a.get("href", "").startswith(("/fr/", "/it/", "/en/"))
                      and "finma-ein-portrae" not in a.get("href", "")]
            # Deduplicate by href
            unique_hrefs = set(a.get("href", "") for a in de_pdfs)
            total_pdfs += len(unique_hrefs)
            print(f"  Archive {year}: {len(unique_hrefs)} unique DE PDFs")
        else:
            print(f"  Archive {year}: {r.status_code}")
    except Exception as e:
        print(f"  Archive {year}: {e}")

print(f"\n  Total archive DE PDFs: {total_pdfs}")

# Also check "weitere-dokumente"
r = s.get(HOST + "/de/dokumentation/archiv/rundschreiben/weitere-dokumente/", timeout=15)
if r.status_code == 200:
    soup = BeautifulSoup(r.text, "html.parser")
    pdfs = [a for a in soup.find_all("a", href=re.compile(r"\.pdf", re.I))
           if not a.get("href", "").startswith(("/fr/", "/it/", "/en/"))
           and "finma-ein-portrae" not in a.get("href", "")]
    print(f"  Weitere Dokumente: {len(pdfs)} DE PDFs")
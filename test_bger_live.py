#!/usr/bin/env python3
"""
LIVE DIAGNOSTIC TEST for BGer scraper.
Tests each component individually so we can see exactly what works.
"""

import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timedelta

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("test_bger")


# ============================================================
# TEST 1: PoW mining (no network needed)
# ============================================================

def test_pow_mining():
    """Test that PoW mining works and produces valid hashes."""
    print("\n" + "="*70)
    print("TEST 1: Proof-of-Work mining")
    print("="*70)
    
    difficulty = 16
    pow_data = hashlib.sha256(os.urandom(32)).hexdigest()
    
    t0 = time.time()
    nonce = 0
    while True:
        h = hashlib.sha256(f"{pow_data}{nonce}".encode()).digest()
        # Check leading zero bits
        if h[0] == 0 and h[1] == 0:  # 16 bits = 2 full zero bytes
            elapsed = time.time() - t0
            pow_hash = h.hex()
            print(f"  ✓ Mined in {elapsed:.3f}s, nonce={nonce}")
            print(f"    pow_data = {pow_data[:40]}...")
            print(f"    pow_hash = {pow_hash[:40]}...")
            print(f"    Verify: hash starts with {pow_hash[:4]} (should be 0000...)")
            
            cookies = {
                "powData": pow_data,
                "powDifficulty": str(difficulty),
                "powHash": pow_hash,
                "powNonce": str(nonce),
            }
            return cookies
        nonce += 1
        if nonce > 500000:
            print("  ✗ FAILED: exceeded 500k hashes")
            return None


# ============================================================
# TEST 2: RSS feed (no PoW needed)
# ============================================================

def test_rss_feed():
    """Test RSS feed access — should work without PoW."""
    print("\n" + "="*70)
    print("TEST 2: RSS feed (no PoW)")
    print("="*70)
    
    rss_url = "https://search.bger.ch/ext/eurospider/live/de/php/aza/rss/index_aza.php"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) Gecko/20100101 Firefox/141.0",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }
    
    try:
        resp = requests.get(rss_url, headers=headers, timeout=30)
        print(f"  Status: {resp.status_code}")
        print(f"  Content-Type: {resp.headers.get('Content-Type', '?')}")
        print(f"  Size: {len(resp.text)} chars")
        print(f"  Final URL: {resp.url}")
        
        if "pow.php" in resp.url:
            print("  ⚠ REDIRECTED to pow.php — RSS also requires PoW now!")
            return []
        
        if resp.status_code != 200:
            print(f"  ✗ Non-200 status")
            return []
        
        # Parse RSS
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            print(f"  ✗ XML parse error: {e}")
            print(f"  First 500 chars: {resp.text[:500]}")
            return []
        
        items = list(root.iter("item"))
        print(f"  Found {len(items)} RSS items")
        
        results = []
        for item in items[:5]:
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")
            
            # Extract docket
            docket_m = re.search(r"\b(\d{1,2}[A-Z]_\d+/\d{4})\b", title + " " + link)
            docket = docket_m.group(1) if docket_m else "?"
            
            print(f"    - {docket} | {pub_date[:20]} | {link[:80]}")
            results.append({"docket": docket, "url": link, "date": pub_date})
        
        if results:
            print(f"  ✓ RSS works, found {len(items)} decisions")
        else:
            print(f"  ⚠ RSS returned data but no parseable items")
        return results
    
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        return []


# ============================================================
# TEST 3: AZA initial page with PoW cookies
# ============================================================

def test_aza_initial(pow_cookies):
    """Test AZA initial page access with PoW cookies."""
    print("\n" + "="*70)
    print("TEST 3: AZA initial page (with PoW)")
    print("="*70)
    
    url = "https://www.bger.ch/ext/eurospider/live/de/php/aza/http/index.php"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) Gecko/20100101 Firefox/141.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de,en-US;q=0.7,en;q=0.3",
    }
    
    session = requests.Session()
    
    try:
        resp = session.get(url, headers=headers, cookies=pow_cookies, timeout=30, allow_redirects=True)
        print(f"  Status: {resp.status_code}")
        print(f"  Final URL: {resp.url}")
        print(f"  Size: {len(resp.text)} chars")
        print(f"  Session cookies: {dict(session.cookies)}")
        
        if "pow.php" in resp.url:
            print("  ⚠ REDIRECTED to pow.php — PoW cookies not accepted")
            print(f"  Response first 500 chars: {resp.text[:500]}")
            return session, False
        
        # Check if we got a proper search page
        soup = BeautifulSoup(resp.text, "html.parser")
        forms = soup.find_all("form")
        inputs = soup.find_all("input")
        print(f"  Forms found: {len(forms)}")
        print(f"  Input fields: {len(inputs)}")
        
        # Look for search form elements
        if soup.find("input", {"name": "query_words"}) or soup.find("input", {"name": "from_date"}):
            print("  ✓ Search form detected — session initialized")
            return session, True
        else:
            print("  ⚠ Got a page but search form not found")
            # Show page title/structure
            title = soup.find("title")
            print(f"  Page title: {title.get_text() if title else 'none'}")
            print(f"  First 300 chars: {resp.text[:300]}")
            return session, True  # Might still work for search
    
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        return session, False


# ============================================================
# TEST 4: AZA search — recent decisions
# ============================================================

def test_aza_search(session, pow_cookies):
    """Test AZA search with a recent date range."""
    print("\n" + "="*70)
    print("TEST 4: AZA search (last 4 days)")
    print("="*70)
    
    today = date.today()
    from_date = today - timedelta(days=4)
    von = from_date.strftime("%d.%m.%Y")
    bis = today.strftime("%d.%m.%Y")
    
    url = (
        "https://www.bger.ch/ext/eurospider/live/de/php/aza/http/index.php?"
        f"lang=de&type=simple_query&query_words=&top_subcollection_aza=all"
        f"&from_date={von}&to_date={bis}"
    )
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) Gecko/20100101 Firefox/141.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de,en-US;q=0.7,en;q=0.3",
    }
    
    try:
        resp = session.get(url, headers=headers, cookies=pow_cookies, timeout=30)
        print(f"  Status: {resp.status_code}")
        print(f"  Final URL: {resp.url}")
        print(f"  Size: {len(resp.text)} chars")
        
        if "pow.php" in resp.url:
            print("  ⚠ REDIRECTED to pow.php")
            return []
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Check hit count
        header = soup.select_one("div.content div.ranklist_header.center")
        if header:
            print(f"  Hit count header: '{header.get_text(strip=True)}'")
        else:
            # Try alternative selectors
            for sel in ["div.ranklist_header", ".treffer", ".resultcount", "h2", "h3"]:
                alt = soup.select_one(sel)
                if alt:
                    print(f"  Alt header ({sel}): '{alt.get_text(strip=True)[:100]}'")
        
        # Check for "no results"
        no_hit = soup.select_one("div.content div.ranklist_content.center")
        if no_hit:
            print(f"  No-results div: '{no_hit.get_text(strip=True)}'")
        
        # Parse results list
        ranklist = soup.select_one("div.ranklist_content ol")
        if not ranklist:
            ranklist = soup.find("ol")
        
        results = []
        if ranklist:
            items = ranklist.find_all("li", recursive=False)
            print(f"  Found {len(items)} result items in <ol>")
            
            for li in items[:5]:
                link = li.select_one("span > a") or li.find("a", href=True)
                if not link:
                    continue
                
                href = link.get("href", "")
                text = link.get_text(strip=True)
                
                # Parse: "DD.MM.YYYY DOCKET"
                decision_date = None
                docket = None
                if len(text) >= 10:
                    dm = re.match(r"(\d{2}\.\d{2}\.\d{4})\s*(.*)", text)
                    if dm:
                        decision_date = dm.group(1)
                        docket_text = dm.group(2).strip()
                        docket_m = re.search(r"(\d{1,2}[A-Z]_\d+/\d{4})", docket_text)
                        if docket_m:
                            docket = docket_m.group(1)
                
                # Metadata divs
                divs = li.select("div > div")
                chamber = divs[0].get_text(strip=True) if len(divs) >= 1 else ""
                legal_area = divs[1].get_text(strip=True) if len(divs) >= 2 else ""
                title = divs[2].get_text(strip=True) if len(divs) >= 3 else ""
                
                result = {
                    "docket": docket or text[:50],
                    "date": decision_date,
                    "url": href if href.startswith("http") else f"https://www.bger.ch{href}",
                    "chamber": chamber,
                    "legal_area": legal_area,
                    "title": title,
                }
                results.append(result)
                print(f"    - {docket or '?'} | {decision_date or '?'} | {chamber[:30]} | {title[:50]}")
        else:
            print("  ⚠ No <ol> found in results")
            # Debug: show page structure
            print(f"  Page structure (first 1000 chars of relevant divs):")
            for div in soup.find_all("div", class_=True)[:10]:
                classes = " ".join(div.get("class", []))
                txt = div.get_text(strip=True)[:80]
                print(f"    div.{classes}: {txt}")
        
        if results:
            print(f"  ✓ Search works, found {len(results)} decisions")
        return results
    
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return []


# ============================================================
# TEST 5: Fetch a single decision (full text)
# ============================================================

def test_fetch_decision(session, pow_cookies, result):
    """Fetch a single decision and extract text."""
    print("\n" + "="*70)
    print(f"TEST 5: Fetch decision: {result.get('docket', '?')}")
    print("="*70)
    
    url = result.get("url", "")
    if not url:
        print("  ✗ No URL to fetch")
        return None
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) Gecko/20100101 Firefox/141.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    
    try:
        resp = session.get(url, headers=headers, cookies=pow_cookies, timeout=30)
        print(f"  Status: {resp.status_code}")
        print(f"  Final URL: {resp.url}")
        print(f"  Size: {len(resp.text)} chars")
        
        if "pow.php" in resp.url:
            print("  ⚠ REDIRECTED to pow.php on decision page")
            return None
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Test all text extraction strategies
        strategies = [
            ("div#highlight_content > div.content", soup.select_one("div#highlight_content > div.content")),
            ("div#highlight_content", soup.select_one("div#highlight_content")),
            ("div.paraatf", soup.find("div", class_="paraatf")),
            ("div.content", soup.select_one("div.content")),
            ("div#content", soup.select_one("div#content")),
            ("td.content", soup.select_one("td.content")),
            ("div.WordSection1", soup.select_one("div.WordSection1")),
        ]
        
        for name, el in strategies:
            if el:
                text = el.get_text(strip=True)
                print(f"  ✓ {name}: {len(text)} chars")
                if len(text) > 200:
                    # Show first 300 chars of extracted text
                    preview = el.get_text(separator="\n")[:300].strip()
                    print(f"    Preview: {preview[:200]}...")
                    
                    # Test metadata extraction
                    full_text = el.get_text(separator="\n")
                    
                    # Judges
                    judges_m = re.search(
                        r"(?:Besetzung|Composition|Composizione)\s*:?\s*(.*?)"
                        r"(?:\.\s*\n|\n\s*\n|Parteien|Parties|Parti|Verfahrensbeteiligte)",
                        full_text, re.DOTALL | re.IGNORECASE,
                    )
                    if judges_m:
                        judges_raw = re.sub(r"\s+", " ", judges_m.group(1).strip())[:200]
                        print(f"    Judges: {judges_raw}")
                    
                    # Title/Subject
                    title_m = re.search(
                        r"(?:Gegenstand|Objet|Oggetto)\s*:?\s*\n?\s*(.*?)"
                        r"(?:\n\s*\n|Beschwerde|Recours|Ricorso)",
                        full_text, re.DOTALL | re.IGNORECASE,
                    )
                    if title_m:
                        title = re.sub(r"\s+", " ", title_m.group(1).strip())[:200]
                        print(f"    Title: {title}")
                    
                    # Outcome
                    dispositiv = full_text[-2000:].lower()
                    outcomes = [
                        ("teilweise gutgeheissen", "partial_approval"),
                        ("gutgeheissen", "approved"),
                        ("abgewiesen", "dismissed"),
                        ("nichteintreten", "inadmissible"),
                        ("nicht eingetreten", "inadmissible"),
                        ("partiellement admis", "partial_approval"),
                        ("admis", "approved"),
                        ("rejeté", "dismissed"),
                    ]
                    for pattern, label in outcomes:
                        if pattern in dispositiv:
                            print(f"    Outcome: {label}")
                            break
                    
                    # Language detection
                    sample = full_text[:5000]
                    de_count = len(re.findall(r"\b(?:der|die|das|ein|eine|er|sie|ist|war|sind)\b", sample))
                    fr_count = len(re.findall(r"\b(?:le|lui|elle|je|on|vous|nous|qui|que|sont)\b", sample))
                    it_count = len(re.findall(r"\b(?:della|del|di|una|al|che|diritto|corte)\b", sample))
                    lang = max({"de": de_count, "fr": fr_count, "it": it_count}, key=lambda k: {"de": de_count, "fr": fr_count, "it": it_count}[k])
                    print(f"    Language: {lang} (de={de_count}, fr={fr_count}, it={it_count})")
                    
                    # Citations
                    bge_refs = re.findall(r"\bBGE\s+\d{1,3}\s+[IV]+[a-z]?\s+\d+\b", full_text)
                    docket_refs = re.findall(r"\b\d{1,2}[A-Z]_\d+/\d{4}\b", full_text)
                    if bge_refs:
                        print(f"    BGE citations: {bge_refs[:5]}")
                    if docket_refs:
                        print(f"    Docket references: {docket_refs[:5]}")
                    
                    return {"text_length": len(text), "strategy": name, "language": lang}
            else:
                print(f"  ✗ {name}: not found")
        
        # If nothing worked, show page structure
        print("  ⚠ No known selector matched. Page structure:")
        for tag in soup.find_all(["div", "td", "article", "section"], class_=True):
            classes = " ".join(tag.get("class", []))
            text_len = len(tag.get_text(strip=True))
            if text_len > 100:
                print(f"    {tag.name}.{classes}: {text_len} chars")
        
        return None
    
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================
# TEST 6: JumpCGI direct access
# ============================================================

def test_jump_cgi(session, pow_cookies, docket, decision_date):
    """Test JumpCGI direct decision access."""
    print("\n" + "="*70)
    print(f"TEST 6: JumpCGI direct access: {docket}")
    print("="*70)
    
    url = f"http://relevancy.bger.ch/cgi-bin/JumpCGI?id={decision_date}_{docket}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) Gecko/20100101 Firefox/141.0",
    }
    
    try:
        resp = session.get(url, headers=headers, cookies=pow_cookies, timeout=30, allow_redirects=True)
        print(f"  Status: {resp.status_code}")
        print(f"  Final URL: {resp.url}")
        print(f"  Size: {len(resp.text)} chars")
        print(f"  Redirected: {resp.url != url}")
        
        if resp.status_code == 200 and len(resp.text) > 500:
            soup = BeautifulSoup(resp.text, "html.parser")
            content = soup.select_one("div#highlight_content > div.content")
            if content:
                print(f"  ✓ JumpCGI works, content: {len(content.get_text(strip=True))} chars")
            else:
                print(f"  ⚠ Got page but primary selector not found")
        else:
            print(f"  ⚠ Short or error response")
    
    except Exception as e:
        print(f"  ✗ FAILED: {e}")


# ============================================================
# TEST 7: Neuheiten page
# ============================================================

def test_neuheiten(session, pow_cookies):
    """Test Neuheiten (recently published) page."""
    print("\n" + "="*70)
    print("TEST 7: Neuheiten page")
    print("="*70)
    
    url = "https://search.bger.ch/ext/eurospider/live/de/php/aza/http/index_aza.php"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) Gecko/20100101 Firefox/141.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    
    try:
        resp = session.get(url, headers=headers, cookies=pow_cookies, timeout=30)
        print(f"  Status: {resp.status_code}")
        print(f"  Final URL: {resp.url}")
        print(f"  Size: {len(resp.text)} chars")
        
        if "pow.php" in resp.url:
            print("  ⚠ REDIRECTED to pow.php")
            return
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Parse results
        ranklist = soup.select_one("div.ranklist_content ol")
        if ranklist:
            items = ranklist.find_all("li", recursive=False)
            print(f"  ✓ Found {len(items)} items in Neuheiten")
            for li in items[:3]:
                link = li.select_one("span > a") or li.find("a", href=True)
                if link:
                    print(f"    - {link.get_text(strip=True)[:70]}")
        else:
            print("  ⚠ No ranklist found")
            title = soup.find("title")
            print(f"  Page title: {title.get_text() if title else 'none'}")
    
    except Exception as e:
        print(f"  ✗ FAILED: {e}")


# ============================================================
# TEST 8: Run actual scraper class
# ============================================================

def test_scraper_class():
    """Test the actual BgerScraper class end-to-end."""
    print("\n" + "="*70)
    print("TEST 8: BgerScraper class (max 3 decisions)")
    print("="*70)
    
    sys.path.insert(0, "/home/claude/caselaw")
    try:
        from scrapers.bger import BgerScraper
        
        scraper = BgerScraper(state_dir=Path("/home/claude/caselaw/test_state"))
        decisions = scraper.run(max_decisions=3)
        
        print(f"\n  Results: {len(decisions)} decisions scraped")
        for d in decisions:
            print(f"\n  Decision: {d.decision_id}")
            print(f"    Docket:   {d.docket_number}")
            print(f"    Date:     {d.decision_date}")
            print(f"    Language: {d.language}")
            print(f"    Chamber:  {d.chamber}")
            print(f"    Title:    {(d.title or '')[:80]}")
            print(f"    Outcome:  {d.outcome}")
            print(f"    Text:     {len(d.full_text)} chars")
            print(f"    Judges:   {(d.judges or '')[:80]}")
            print(f"    Citations:{len(d.cited_decisions or [])}")
            if d.cited_decisions:
                print(f"    First 3:  {d.cited_decisions[:3]}")
        
        if decisions:
            # Save sample output
            out = Path("/home/claude/caselaw/test_output")
            out.mkdir(exist_ok=True)
            with open(out / "sample.jsonl", "w") as f:
                for d in decisions:
                    f.write(d.model_dump_json() + "\n")
            print(f"\n  ✓ Saved to test_output/sample.jsonl")
        
        return decisions
    
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return []


# ============================================================
# MAIN
# ============================================================

from pathlib import Path

if __name__ == "__main__":
    print("=" * 70)
    print("BGer LIVE DIAGNOSTIC TEST")
    print(f"Date: {datetime.now().isoformat()}")
    print("=" * 70)
    
    # Test 1: PoW
    pow_cookies = test_pow_mining()
    if not pow_cookies:
        print("\n⚠ PoW mining failed, cannot continue")
        sys.exit(1)
    
    time.sleep(1)
    
    # Test 2: RSS
    rss_results = test_rss_feed()
    
    time.sleep(1)
    
    # Test 3: AZA initial page
    session, aza_ok = test_aza_initial(pow_cookies)
    
    time.sleep(2)
    
    # Test 4: AZA search
    search_results = test_aza_search(session, pow_cookies)
    
    time.sleep(2)
    
    # Test 5: Fetch first decision from search or RSS
    fetch_result = None
    target = None
    if search_results:
        target = search_results[0]
    elif rss_results:
        target = rss_results[0]
    
    if target:
        fetch_result = test_fetch_decision(session, pow_cookies, target)
    else:
        print("\n⚠ No decisions found to fetch")
    
    time.sleep(2)
    
    # Test 6: JumpCGI with a known recent decision
    if search_results and search_results[0].get("docket") and search_results[0].get("date"):
        test_jump_cgi(
            session, pow_cookies,
            search_results[0]["docket"],
            search_results[0]["date"],
        )
    
    time.sleep(2)
    
    # Test 7: Neuheiten
    test_neuheiten(session, pow_cookies)
    
    # Test 8: Full scraper class
    time.sleep(2)
    print("\n\nNow testing the actual BgerScraper class...")
    decisions = test_scraper_class()
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"  PoW mining:    {'✓' if pow_cookies else '✗'}")
    print(f"  RSS feed:      {'✓' if rss_results else '✗/⚠'}")
    print(f"  AZA init:      {'✓' if aza_ok else '✗'}")
    print(f"  AZA search:    {'✓' if search_results else '✗'}")
    print(f"  Decision fetch:{'✓' if fetch_result else '✗'}")
    print(f"  Scraper class: {'✓ ' + str(len(decisions)) + ' decisions' if decisions else '✗'}")

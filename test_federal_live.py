#!/usr/bin/env python3
"""
Live Validation Tests — BVGer, BStGer, BPatGer
================================================

Run on a machine with unrestricted internet access:
    cd swiss-caselaw-scrapers
    pip install -e ".[all]"
    python test_federal_live.py

Tests each scraper's:
  1. Network connectivity / session init
  2. Search / discovery (few recent decisions)
  3. Detail / content fetch
  4. Parsing correctness
  5. Decision object creation

Requires: requests, beautifulsoup4
"""
import json, logging, re, sys, time, traceback
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("test_federal")

PASS = 0
FAIL = 0
SKIP = 0


def result(name: str, ok: bool, detail: str = ""):
    global PASS, FAIL
    status = "✅ PASS" if ok else "❌ FAIL"
    if ok:
        PASS += 1
    else:
        FAIL += 1
    print(f"  {status}  {name}" + (f"  ({detail})" if detail else ""))


def skip(name: str, reason: str = ""):
    global SKIP
    SKIP += 1
    print(f"  ⏭️ SKIP  {name}" + (f"  ({reason})" if reason else ""))


# ============================================================
# BStGer Tests (Weblaw JSON API)
# ============================================================

def test_bstger():
    print("\n" + "=" * 60)
    print("BStGer — Federal Criminal Court (bstger.weblaw.ch)")
    print("=" * 60)

    import requests

    # Test 1: API reachability
    print("\n--- Test 1: API Reachability ---")
    try:
        body = {
            "guiLanguage": "de",
            "userID": "_test12345",
            "sessionDuration": str(int(time.time())),
            "metadataDateMap": {
                "rulingDate": {
                    "from": "2026-01-01T00:00:00.000Z",
                    "to": "2026-02-08T23:59:59.999Z",
                }
            },
            "aggs": {
                "fields": ["year", "language", "court", "rulingDate"],
                "size": "10",
            },
        }
        resp = requests.post(
            "https://bstger.weblaw.ch/api/getDocuments?withAggregations=false",
            json=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "*/*",
                "Origin": "https://bstger.weblaw.ch",
            },
            timeout=30,
        )
        result("HTTP status 200", resp.status_code == 200, f"got {resp.status_code}")
        data = resp.json()
        result("JSON response has status=success", data.get("status") == "success")
    except Exception as e:
        result("API reachable", False, str(e))
        return

    # Test 2: Parse search results
    print("\n--- Test 2: Search Results Parsing ---")
    try:
        r = data["data"]
        total = r.get("totalNumberOfDocuments", 0)
        docs = r.get("documents", [])
        result("totalNumberOfDocuments > 0", total > 0, f"total={total}")
        result("documents list non-empty", len(docs) > 0, f"count={len(docs)}")

        if docs:
            doc = docs[0]
            kw = doc.get("metadataKeywordTextMap", {})
            dt = doc.get("metadataDateMap", {})
            leid = doc.get("leid", "")
            titles = kw.get("title", [])
            result("First doc has title", len(titles) > 0, titles[0] if titles else "")
            result("First doc has leid", len(leid) > 10, leid[:40])
            result("First doc has rulingDate", "rulingDate" in dt, dt.get("rulingDate", "")[:10])
            result("First doc has content", len(doc.get("content", "")) > 0)

            # Extract docket number
            docket = titles[0].split(",")[0] if titles else ""
            result("Docket looks valid", bool(re.match(r"[A-Z]{2}\.\d{4}\.\d+", docket)), docket)
    except Exception as e:
        result("Parse search results", False, str(e))

    # Test 3: Fetch full content
    print("\n--- Test 3: Full Content Fetch ---")
    if docs:
        leid = docs[0].get("leid", "")
        try:
            resp2 = requests.get(
                f"https://bstger.weblaw.ch/api/getDocumentContent/{leid}",
                timeout=30,
            )
            result("Content endpoint returns 200", resp2.status_code == 200)
            text = resp2.text
            result("Content length > 500 chars", len(text) > 500, f"len={len(text)}")

            # Check for typical BStGer markers
            has_tribunal = "Bundesstrafgericht" in text or "Tribunal pénal fédéral" in text
            result("Contains court name", has_tribunal)
            has_legal = any(w in text for w in ["StPO", "StGB", "IRSG", "StBOG", "Art."])
            result("Contains legal references", has_legal)
        except Exception as e:
            result("Content fetch", False, str(e))
    else:
        skip("Full content fetch", "no documents found")

    # Test 4: BStGer scraper class
    print("\n--- Test 4: Scraper Class ---")
    try:
        sys.path.insert(0, ".")
        from scrapers.bstger import BStGerScraper, _random_user_id

        uid = _random_user_id()
        result("Random userID format", uid.startswith("_") and len(uid) == 9, uid)

        scraper = BStGerScraper()
        result("Scraper instantiation", scraper.court_code == "bstger")
    except Exception as e:
        result("Scraper class", False, str(e))


# ============================================================
# BVGer Tests (Dual mode: Weblaw + jurispub.admin.ch)
# ============================================================

def test_bvger():
    print("\n" + "=" * 60)
    print("BVGer — Federal Administrative Court")
    print("=" * 60)

    import requests

    # Test 1: Weblaw API
    print("\n--- Test 1: Weblaw API (bvger.weblaw.ch) ---")
    weblaw_ok = False
    try:
        body = {
            "guiLanguage": "de",
            "userID": "_test12345",
            "sessionDuration": str(int(time.time())),
            "metadataDateMap": {
                "rulingDate": {
                    "from": "2026-01-01T00:00:00.000Z",
                    "to": "2026-02-08T23:59:59.999Z",
                }
            },
            "aggs": {"fields": ["year","language","court","rulingDate"], "size":"10"},
        }
        resp = requests.post(
            "https://bvger.weblaw.ch/api/getDocuments?withAggregations=false",
            json=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "*/*",
                "Origin": "https://bvger.weblaw.ch",
            },
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "success":
                total = data["data"].get("totalNumberOfDocuments", 0)
                docs = data["data"].get("documents", [])
                result("Weblaw API: status=success", True)
                result("Weblaw API: total > 0", total > 0, f"total={total}")
                result("Weblaw API: docs returned", len(docs) > 0, f"count={len(docs)}")
                weblaw_ok = True

                if docs:
                    doc0 = docs[0]
                    kw = doc0.get("metadataKeywordTextMap", {})
                    t = kw.get("title", [""])[0]
                    result("First doc docket", bool(t), t)
                    leid = doc0.get("leid", "")
                    result("First doc leid", len(leid) > 10, leid[:40])
            else:
                result("Weblaw API: success response", False, f"status={data.get('status')}")
        else:
            result("Weblaw API: HTTP 200", False, f"got {resp.status_code}")
    except Exception as e:
        result("Weblaw API reachable", False, str(e))

    # Test 2: jurispub.admin.ch fallback
    print("\n--- Test 2: jurispub.admin.ch (ICEfaces fallback) ---")
    jp_ok = False
    try:
        resp = requests.get("https://jurispub.admin.ch/publiws/?lang=de", timeout=30)
        result("jurispub.admin.ch reachable", resp.status_code == 200, f"status={resp.status_code}")
        has_form = "searchQuery" in resp.text or "calFrom" in resp.text
        result("ICEfaces search form present", has_form)

        # Check for ICE session
        ice_match = re.search(r'(?<=script id=")[^:]+(?=:1:configuration-script)', resp.text)
        result("ICE session token found", ice_match is not None)

        jsession = None
        for cookie in resp.cookies:
            if cookie.name == "JSESSIONID":
                jsession = cookie.value
        result("JSESSIONID cookie received", jsession is not None,
               jsession[:20] + "..." if jsession else "")
        jp_ok = bool(jsession and ice_match)
    except Exception as e:
        result("jurispub fallback", False, str(e))

    # Test 3: PDF download (direct UUID access)
    print("\n--- Test 3: PDF Download (jurispub UUID) ---")
    try:
        # Use a known UUID from search results
        test_uuid = "d0d55922-877b-4fa0-8571-3267538279a3"  # Recent 2025 decision
        resp = requests.get(
            f"https://jurispub.admin.ch/publiws/download?decisionId={test_uuid}",
            timeout=30,
            stream=True,
        )
        result("PDF endpoint returns 200", resp.status_code == 200)
        ct = resp.headers.get("Content-Type", "")
        result("Content-Type is PDF", "pdf" in ct.lower(), ct)
    except Exception as e:
        result("PDF download", False, str(e))

    # Test 4: Scraper class
    print("\n--- Test 4: Scraper Class ---")
    try:
        sys.path.insert(0, ".")
        from scrapers.bvger import BVGerScraper, _detect_abteilung

        # Abteilung detection
        result("Abteilung A→I", "Abteilung I" in (_detect_abteilung("A-123/2025") or ""))
        result("Abteilung D→IV", "Abteilung IV" in (_detect_abteilung("D-456/2025") or ""))
        result("Abteilung F→VI", "Abteilung VI" in (_detect_abteilung("F-789/2025") or ""))

        scraper = BVGerScraper()
        result("Scraper instantiation", scraper.court_code == "bvger")
    except Exception as e:
        result("Scraper class", False, str(e))

    return weblaw_ok, jp_ok


# ============================================================
# BPatGer Tests (TYPO3 HTML)
# ============================================================

def test_bpatger():
    print("\n" + "=" * 60)
    print("BPatGer — Federal Patent Court (bundespatentgericht.ch)")
    print("=" * 60)

    import requests
    from bs4 import BeautifulSoup

    # Test 1: Website reachability
    print("\n--- Test 1: Website Reachability ---")
    try:
        resp = requests.get(
            "https://www.bundespatentgericht.ch/rechtsprechung/aktuelle-entscheide",
            timeout=30,
        )
        result("HTTP 200", resp.status_code == 200)
        result("Contains decisions", "Entscheid" in resp.text or "O20" in resp.text)
    except Exception as e:
        result("Website reachable", False, str(e))
        return

    # Test 2: Decision listing
    print("\n--- Test 2: Decision Listing ---")
    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        # Look for links to decisions
        links = [a for a in soup.find_all("a") if "entscheidanzeige" in (a.get("href",""))]
        result("Decision links found", len(links) > 0, f"count={len(links)}")

        if links:
            first_href = links[0].get("href", "")
            result("Link has ID", bool(re.search(r"/\d+/?$", first_href)), first_href)
    except Exception as e:
        result("Decision listing", False, str(e))

    # Test 3: Detail page parsing
    print("\n--- Test 3: Detail Page (S2024_001) ---")
    try:
        resp2 = requests.get(
            "https://www.bundespatentgericht.ch/rechtsprechung/entscheidanzeige/234/",
            timeout=30,
        )
        result("Detail page HTTP 200", resp2.status_code == 200)
        soup2 = BeautifulSoup(resp2.text, "html.parser")

        # Find the metadata table
        tables = soup2.find_all("table")
        result("Tables found on page", len(tables) > 0, f"count={len(tables)}")

        # Try both selectors: with and without class
        table = soup2.find("table", class_="tx-is-courtcases")
        if not table:
            # Fallback: find table containing "Prozessnummer"
            for t in tables:
                if "Prozessnummer" in t.get_text():
                    table = t
                    break

        result("Metadata table found", table is not None,
               "class=tx-is-courtcases" if soup2.find("table", class_="tx-is-courtcases") else "fallback selector")

        if table:
            # Extract cell values
            def get_cell(label):
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) >= 2 and label in cells[0].get_text():
                        return cells[1].get_text(strip=True)
                return None

            docket = get_cell("Prozessnummer")
            result("Prozessnummer extracted", docket is not None, docket)
            entscheid_date = get_cell("Entscheiddatum")
            result("Entscheiddatum extracted", entscheid_date is not None, entscheid_date)
            verfahren = get_cell("Art des Verfahrens")
            result("Art des Verfahrens", verfahren is not None, verfahren)
            status = get_cell("Status")
            result("Status extracted", status is not None, status)

            # PDF link
            def get_cell_link(label):
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) >= 2 and label in cells[0].get_text():
                        link = cells[1].find("a")
                        return link.get("href") if link else None
                return None

            pdf = get_cell_link("Entscheid als PDF")
            result("PDF link found", pdf is not None, pdf)
            if pdf:
                result("PDF is .pdf file", pdf.endswith(".pdf"))

        # Stichwort / Gegenstand
        stichwort_section = soup2.find("h2", string=lambda s: s and "Stichwort" in s) if soup2 else None
        if not stichwort_section:
            # Try h1
            stichwort_section = soup2.find("h1", string=lambda s: s and "Stichwort" in s) if soup2 else None
        # Check for the text directly
        has_stichwort = "vorsorgliche Massnahme" in resp2.text
        result("Stichwort content present", has_stichwort)

    except Exception as e:
        result("Detail page parsing", False, str(e))

    # Test 4: Search form (TYPO3)
    print("\n--- Test 4: Database Search Form ---")
    try:
        resp3 = requests.get(
            "https://www.bundespatentgericht.ch/rechtsprechung/datenbankabfrage",
            timeout=30,
        )
        result("Search form page loads", resp3.status_code == 200)
        has_form = "tx_iscourtcases" in resp3.text
        result("TYPO3 form fields present", has_form)
    except Exception as e:
        result("Search form", False, str(e))


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Federal Court Scrapers — Live Validation")
    print(f"Date: {date.today().isoformat()}")
    print("=" * 60)

    test_bstger()
    test_bvger()
    test_bpatger()

    print("\n" + "=" * 60)
    total = PASS + FAIL + SKIP
    print(f"Results: {PASS}/{total} passed, {FAIL} failed, {SKIP} skipped")
    if FAIL == 0:
        print("🎉 All tests passed!")
    else:
        print(f"⚠️  {FAIL} test(s) need attention")
    print("=" * 60)
    sys.exit(1 if FAIL > 0 else 0)

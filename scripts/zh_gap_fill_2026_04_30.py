"""Targeted gap-fill for König's 11 ZH dockets missing from the dataset.

Each docket is confirmed live on gerichte-zh.ch livesearch. Our zh_gerichte
scraper missed them — likely due to date-window edge cases.

For each docket: query livesearch, parse the entscheid+details divs, find the
PDF URL, download + extract text via pdfplumber, insert into production
decisions.db with all metadata.

Idempotent: skips if decision_id already exists.
"""
from __future__ import annotations

import io
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from datetime import date

# pdfplumber and bs4 are present on the VPS
import pdfplumber
from bs4 import BeautifulSoup

DB = "/mnt/HC_Volume_104655575/output/decisions.db"
HOST = "https://www.gerichte-zh.ch"
LIVESEARCH = (
    HOST + "/typo3conf/ext/frp_entscheidsammlung_extended/res/php/livesearch.php"
)
USER_AGENT = "Mozilla/5.0 (compatible; OpenCaseLaw-GapFill/1.0; +https://opencaselaw.ch)"

DOCKETS = [
    "UE250499", "PS260001", "LF250113", "RU260001", "PS260035",
    "PS260053", "PS260063", "PS260003", "PS260080", "LF260001",
    "DH260004",
]

# Court mapping based on docket prefix or kammer name
def map_court(gericht: str, kammer: str) -> str:
    g = (gericht or "").lower()
    k = (kammer or "").lower()
    if "obergericht" in g and "z" in g:
        return "zh_obergericht"
    if "bezirksgericht" in g:
        if "horgen" in g: return "zh_bezirksgericht_horgen"
        if "hinwil" in g: return "zh_bezirksgericht_hinwil"
        if "uster" in g: return "zh_bezirksgericht_uster"
        if "winterthur" in g: return "zh_bezirksgericht_winterthur"
        if "meilen" in g: return "zh_bezirksgericht_meilen"
        if "pfäffikon" in g or "pfaeffikon" in g: return "zh_bezirksgericht_pfaeffikon"
        if "zürich" in g or "zuerich" in g: return "zh_bezirksgericht_zuerich"
        if "dielsdorf" in g: return "zh_bezirksgericht_dielsdorf"
        if "bülach" in g or "buelach" in g: return "zh_bezirksgericht_buelach"
        if "andelfingen" in g: return "zh_bezirksgericht_andelfingen"
        if "affoltern" in g: return "zh_bezirksgericht_affoltern"
        if "dietikon" in g: return "zh_bezirksgericht_dietikon"
    if "handelsgericht" in g:
        return "zh_handelsgericht"
    if "mietgericht" in g:
        return "zh_mietgericht"
    return "zh_obergericht"  # default to OG since most missing dockets are OG


def http_get(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def query_livesearch(docket: str) -> str | None:
    params = {
        "q": "",
        "geschaeftsnummer": docket,
        "gericht": "gerichtTitel",
        "kammer": "kammerTitel",
        "entscheiddatum_von": "01.01.2025",
        "entscheiddatum_bis": "30.04.2026",
        "erweitert": "1",
        "suchfilter": "1",
    }
    url = LIVESEARCH + "?" + urllib.parse.urlencode(params)
    try:
        data = http_get(url, timeout=15)
    except Exception as e:
        print(f"    query failed: {e}")
        return None
    return data.decode("utf-8", errors="replace")


def get_detail_field(details_div, label: str) -> str:
    """Extract a labelled field from the entscheidDetails div."""
    rows = details_div.find_all(["dt", "dd"])
    if rows:
        for i, r in enumerate(rows):
            if r.name == "dt" and label.lower() in r.get_text().lower():
                # Next sibling dd
                if i + 1 < len(rows) and rows[i + 1].name == "dd":
                    return rows[i + 1].get_text(strip=True)
    # Fallback: scan for "Label:" in text
    text = details_div.get_text("\n", strip=True)
    pattern = re.compile(rf"{re.escape(label)}\s*:?\s*(.+)", re.IGNORECASE)
    for line in text.split("\n"):
        m = pattern.match(line)
        if m:
            return m.group(1).strip()
    return ""


def parse_html_response(html: str, docket: str) -> dict | None:
    """Parse livesearch HTML response into a stub dict."""
    soup = BeautifulSoup(html, "html.parser")

    # Find entscheid + entscheidDetails divs
    entscheid_divs = soup.find_all(
        "div", class_=re.compile(r"^entscheid\s+entscheid_nummer_")
    )
    details_divs = soup.find_all(
        "div", class_=re.compile(r"^entscheidDetails\s+container_")
    )

    if not entscheid_divs or not details_divs:
        return None

    # Pair by ID
    e_map = {}
    for div in entscheid_divs:
        cls = div.get("class", [])
        cls_str = " ".join(cls) if isinstance(cls, list) else str(cls)
        m = re.search(r"entscheid_nummer_(\S+)", cls_str)
        if m:
            e_map[m.group(1)] = div

    d_map = {}
    for div in details_divs:
        cls = div.get("class", [])
        cls_str = " ".join(cls) if isinstance(cls, list) else str(cls)
        m = re.search(r"container_(\S+)", cls_str)
        if m:
            d_map[m.group(1)] = div

    for doc_id, e_div in e_map.items():
        d_div = d_map.get(doc_id)
        if not d_div:
            continue

        # Find PDF link
        pdf_url = None
        for a in d_div.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower():
                pdf_url = HOST + href if href.startswith("/") else href
                break

        # Extract metadata
        gericht = get_detail_field(d_div, "Gericht/Behörde")
        kammer = get_detail_field(d_div, "Abteilung/Kammer")
        edatum_str = get_detail_field(d_div, "Entscheiddatum")
        entscheidart = get_detail_field(d_div, "Entscheidart")

        title = ""
        strong = e_div.find("strong")
        if strong:
            title = strong.get_text(strip=True)

        leitsatz = ""
        em = e_div.find("em")
        if em:
            leitsatz = em.get_text(strip=True)

        # Parse date
        edatum = None
        if edatum_str:
            m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", edatum_str)
            if m:
                d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
                try:
                    edatum = date(y, mo, d).isoformat()
                except ValueError:
                    pass

        return {
            "doc_id": doc_id,
            "docket": docket,
            "gericht": gericht,
            "kammer": kammer,
            "edatum": edatum,
            "entscheidart": entscheidart,
            "title": title,
            "leitsatz": leitsatz,
            "pdf_url": pdf_url,
        }

    return None


def extract_pdf_text(pdf_bytes: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
        return "\n\n".join(pages).strip()
    except Exception as e:
        print(f"    pdfplumber failed: {e}")
        return ""


def make_decision_id(court: str, docket: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9]", "_", docket)
    return f"{court}_{safe}"


def insert_decision(conn: sqlite3.Connection, stub: dict, full_text: str) -> bool:
    court = map_court(stub["gericht"], stub["kammer"])
    decision_id = make_decision_id(court, stub["docket"])

    # Skip if already present
    existing = conn.execute(
        "SELECT 1 FROM decisions WHERE decision_id = ?", (decision_id,)
    ).fetchone()
    if existing:
        print(f"    SKIP — already in DB: {decision_id}")
        return False

    source_url = (
        HOST + f"/index.php?id=109&type=98&doc_id={stub['doc_id']}&sphrase_id="
    )
    pdf_url = stub.get("pdf_url") or ""
    chamber = stub.get("kammer") or None

    conn.execute(
        """INSERT INTO decisions
           (decision_id, court, canton, chamber, docket_number, decision_date,
            language, title, regeste, full_text, decision_type, source_url, pdf_url,
            source, scraped_at)
           VALUES (?, ?, 'ZH', ?, ?, ?, 'de', ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            decision_id,
            court,
            chamber,
            stub["docket"],
            stub["edatum"],
            stub.get("title") or f"{court} — {stub['docket']}",
            stub.get("leitsatz") or None,
            full_text or None,
            stub.get("entscheidart") or None,
            source_url,
            pdf_url,
            "zh_gerichte_gapfill_2026_04_30",
            date.today().isoformat(),
        ),
    )
    conn.commit()
    print(f"    INSERTED  → {decision_id}  court={court}  date={stub['edatum']}  text_len={len(full_text):,}")
    return True


def main():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA busy_timeout=120000")

    inserted = 0
    skipped = 0
    failed = 0

    for docket in DOCKETS:
        print(f"\n=== {docket} ===")
        html = query_livesearch(docket)
        if not html:
            print("    livesearch failed")
            failed += 1
            continue

        stub = parse_html_response(html, docket)
        if not stub:
            print("    no entscheid div parsed (livesearch returned no result?)")
            failed += 1
            continue

        print(f"    found: court={stub['gericht']!r}  kammer={stub['kammer']!r}  date={stub['edatum']}  pdf={stub['pdf_url']!r}")

        if not stub["pdf_url"]:
            print("    no PDF URL — inserting metadata-only row")
            ok = insert_decision(conn, stub, "")
            if ok:
                inserted += 1
            else:
                skipped += 1
            continue

        # Download PDF
        try:
            pdf_bytes = http_get(stub["pdf_url"], timeout=60)
        except Exception as e:
            print(f"    PDF download failed: {e}")
            failed += 1
            continue

        full_text = extract_pdf_text(pdf_bytes)
        ok = insert_decision(conn, stub, full_text)
        if ok:
            inserted += 1
        else:
            skipped += 1

        time.sleep(2)  # rate limit

    print(f"\n=== SUMMARY ===")
    print(f"  inserted: {inserted}")
    print(f"  skipped:  {skipped}")
    print(f"  failed:   {failed}")
    conn.close()


if __name__ == "__main__":
    main()

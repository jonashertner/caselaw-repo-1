"""Full ti_gerichte date recovery: fetch source URL for each NULL-date row,
extract decision_date via multi-strategy anchor matching."""
import re
import sqlite3
import sys
import urllib.request
import urllib.parse
import time

DB = "/mnt/HC_Volume_104655575/output/decisions.db"
HEADERS = {"User-Agent": "Mozilla/5.0 OpenCaseLaw"}
DRY_RUN = "--apply" not in sys.argv

IT_MONTHS = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4,
    "maggio": 5, "giugno": 6, "luglio": 7, "agosto": 8,
    "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
}
DD_MONTH_RE = re.compile(
    r"\b(\d{1,2})[\.\s]+(gennaio|febbraio|marzo|aprile|maggio|giugno|"
    r"luglio|agosto|settembre|ottobre|novembre|dicembre)\s+(\d{4})\b",
    re.IGNORECASE,
)
DDMMYYYY_RE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b")


def fetch(url, timeout=30):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def extract_date(html):
    # Strategy 1: city anchor ("Lugano, DD [mese] YYYY")
    for city in ("Lugano", "Mendrisio", "Bellinzona", "Locarno", "Ascona"):
        m = re.search(
            rf"{city},?\s*(\d{{1,2}})[\.\s]+([a-zà]+)\s+(\d{{4}})",
            html, re.IGNORECASE,
        )
        if m:
            d, mo, y = int(m.group(1)), m.group(2).lower(), int(m.group(3))
            if mo in IT_MONTHS and 1700 <= y <= 2030:
                try:
                    from datetime import date
                    date(y, IT_MONTHS[mo], d)  # validate
                    return f"{y}-{IT_MONTHS[mo]:02d}-{d:02d}"
                except ValueError:
                    pass

    # Strategy 2: anchor near "Sentenza"/"Decisione"
    for anchor in ("Sentenza", "Decisione", "Decreto"):
        i = 0
        while True:
            j = html.find(anchor, i)
            if j < 0:
                break
            window = html[j:j + 400]
            for m in DD_MONTH_RE.finditer(window):
                d, mo, y = int(m.group(1)), m.group(2).lower(), int(m.group(3))
                if mo in IT_MONTHS and 1700 <= y <= 2030:
                    try:
                        from datetime import date as _date
                        _date(y, IT_MONTHS[mo], d)
                        return f"{y}-{IT_MONTHS[mo]:02d}-{d:02d}"
                    except ValueError:
                        pass
            i = j + len(anchor)

    # Strategy 3: "Data" label
    for label in ("Pubblicazione", "Data della sentenza", "Data della decisione", "Data"):
        i = html.find(label)
        if 0 < i < 10000:
            window = html[i:i + 300]
            m = DDMMYYYY_RE.search(window)
            if m:
                d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
                if 1 <= mo <= 12 and 1700 <= y <= 2030:
                    try:
                        from datetime import date as _date
                        _date(y, mo, d)
                        return f"{y}-{mo:02d}-{d:02d}"
                    except ValueError:
                        pass

    return None


def main():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA busy_timeout=120000")

    rows = conn.execute(
        "SELECT decision_id, docket_number, source_url FROM decisions "
        "WHERE court='ti_gerichte' AND (decision_date IS NULL OR decision_date='') "
        "AND source_url IS NOT NULL"
    ).fetchall()

    print(f"  rows to process: {len(rows):,}  (dry_run={DRY_RUN})")
    n_extracted = 0
    n_failed = 0
    n_fetch_err = 0
    updates = []

    for i, (did, doc, url) in enumerate(rows):
        if i and i % 50 == 0:
            print(f"  [{i}/{len(rows)}] extracted: {n_extracted}, failed: {n_failed}, fetch errs: {n_fetch_err}")

        try:
            html = fetch(url, timeout=20)
        except Exception as e:
            n_fetch_err += 1
            continue

        d = extract_date(html)
        if d:
            updates.append((d, did))
            n_extracted += 1
        else:
            n_failed += 1
        # Rate limit: 1s between requests
        time.sleep(1.0)

    print(f"\n  Final: extracted={n_extracted}/{len(rows)} ({100*n_extracted/len(rows):.1f}%)")
    print(f"  Failed pattern match: {n_failed}")
    print(f"  Fetch errors: {n_fetch_err}")

    if not DRY_RUN and updates:
        conn.executemany(
            "UPDATE decisions SET decision_date=? WHERE decision_id=?",
            updates,
        )
        conn.commit()
        print(f"  WROTE: {len(updates)} updates committed")

    conn.close()
    print("DONE")


if __name__ == "__main__":
    main()

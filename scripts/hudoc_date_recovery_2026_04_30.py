"""Try HUDOC API recovery for the 246 NULL-date hudoc_ch rows.

Each row has source_url like https://hudoc.echr.coe.int/eng?i=001-180707
The itemid is 001-180707. Query HUDOC's app/query API with select=judgementdate
to get the actual ECHR decision date if HUDOC has it indexed.
"""
import json
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request

DB = "/mnt/HC_Volume_104655575/output/decisions.db"
DRY_RUN = "--apply" not in sys.argv
HEADERS = {"User-Agent": "Mozilla/5.0 OpenCaseLaw", "Accept": "application/json"}

API_BASE = "https://hudoc.echr.coe.int/app/query/results"


def query_hudoc(itemid):
    q = f'(itemid="{itemid}")'
    params = {
        "query": q,
        "select": "itemid,judgementdate,judgementdatevalue,docname,doctypebranch,kpdate,kpdatevalue",
        "sort": "",
        "start": "0",
        "length": "1",
    }
    url = API_BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))


def parse_iso(s):
    if not s:
        return None
    # HUDOC dates are like "2018-01-09T00:00:00Z" or "9/1/2018 12:00:00 AM"
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        try:
            from datetime import date
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
        except (ValueError, TypeError):
            pass
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        try:
            from datetime import date
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return date(y, mo, d).isoformat()
        except (ValueError, TypeError):
            pass
    return None


def extract_itemid(source_url):
    m = re.search(r"i=(001-\d+)", source_url)
    return m.group(1) if m else None


def main():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA busy_timeout=120000")

    rows = conn.execute(
        "SELECT decision_id, source_url FROM decisions "
        "WHERE court='hudoc_ch' "
        "AND (decision_date IS NULL OR decision_date='') "
        "AND source_url IS NOT NULL"
    ).fetchall()

    print(f"  rows to process: {len(rows):,}  (dry_run={DRY_RUN})")
    n_recovered = 0
    n_no_date = 0
    n_fetch_err = 0
    updates = []

    for i, (did, url) in enumerate(rows):
        if i and i % 25 == 0:
            print(f"  [{i}/{len(rows)}] recovered={n_recovered}, no_date={n_no_date}, errs={n_fetch_err}")

        itemid = extract_itemid(url)
        if not itemid:
            n_no_date += 1
            continue

        try:
            data = query_hudoc(itemid)
        except Exception as e:
            n_fetch_err += 1
            time.sleep(2)
            continue

        results = data.get("results") or []
        if not results:
            n_no_date += 1
            time.sleep(0.5)
            continue

        cols = results[0].get("columns", {})
        # Try multiple HUDOC date fields
        d = parse_iso(cols.get("judgementdate")) or parse_iso(cols.get("judgementdatevalue")) or parse_iso(cols.get("kpdate")) or parse_iso(cols.get("kpdatevalue"))
        if d:
            n_recovered += 1
            updates.append((d, did))
        else:
            n_no_date += 1
        time.sleep(0.5)

    print(f"\n  Final: recovered={n_recovered}/{len(rows)} ({100*n_recovered/len(rows):.1f}%)")
    print(f"  No date in HUDOC API: {n_no_date}")
    print(f"  Fetch errors:          {n_fetch_err}")

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

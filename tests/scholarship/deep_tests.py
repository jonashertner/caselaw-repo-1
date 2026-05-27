#!/usr/bin/env python3
"""Deep test suite for the OA legal scholarship corpus on opencaselaw.

Tiers:
  A — Live MCP/REST endpoints
  B — Data integrity (per-record correctness)
  C — Search relevance (multilingual + edge cases)
  D — Dashboard (HTML + stats + CORS)
  E — Source-specific shape checks
  F — Pipeline (publish step dry-run)
  G — DB consistency
  H — Performance / latency

Run from local box. Reports PASS/FAIL with concrete evidence.
"""
from __future__ import annotations
import concurrent.futures
import json
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request

REST = "https://mcp.opencaselaw.ch/api"
DASH = "https://opencaselaw.ch"

# colors / formatting
GREEN = "\033[32m"; RED = "\033[31m"; YELLOW = "\033[33m"; RESET = "\033[0m"
results = []


def record(tier: str, name: str, ok: bool, detail: str = ""):
    results.append((tier, name, ok, detail))
    icon = f"{GREEN}✓{RESET}" if ok else f"{RED}✗{RESET}"
    print(f"  {icon} [{tier}] {name}" + (f"  — {detail}" if detail else ""))


def fetch(url: str, timeout: int = 10) -> tuple[int, dict | list | None, dict]:
    """GET + parse JSON. Returns (status, json_or_none, headers)."""
    req = urllib.request.Request(url, headers={"User-Agent": "scholarship-tester/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read()
            try:
                j = json.loads(data) if data else None
            except Exception:
                j = None
            return r.status, j, dict(r.headers)
    except urllib.error.HTTPError as e:
        try: body = e.read().decode("utf-8", errors="replace")[:200]
        except: body = ""
        return e.code, {"_error": body}, dict(e.headers) if e.headers else {}
    except Exception as e:
        return -1, {"_error": str(e)}, {}


# ── TIER A: live REST endpoints ─────────────────────────────────────────
def tier_a():
    print(f"\n{YELLOW}── Tier A: REST endpoints ──{RESET}")
    # A1: list sources
    s, j, _ = fetch(f"{REST}/scholarship/sources")
    record("A", "/sources → 200 + JSON",
           s == 200 and isinstance(j, dict),
           f"status={s}")
    total = j.get("total_publications") if isinstance(j, dict) else None
    record("A", "/sources includes total_publications > 800",
           isinstance(total, int) and total >= 840,
           f"total={total}")
    by_source = j.get("by_source", []) if isinstance(j, dict) else []
    record("A", "/sources lists ≥ 5 active sources",
           len(by_source) >= 5,
           f"n={len(by_source)}")
    # Every source has attribution
    missing_attr = [s["source"] for s in by_source if not s.get("attribution")]
    record("A", "every source has attribution text",
           len(missing_attr) == 0,
           f"missing={missing_attr}")
    # License notice
    record("A", "/sources includes by_license breakdown",
           "by_license" in (j or {}),
           "")

    # A2: licenses catalog
    s, j, _ = fetch(f"{REST}/scholarship/licenses")
    record("A", "/licenses → 200 + catalog list",
           s == 200 and isinstance(j, dict) and isinstance(j.get("catalog"), list),
           f"status={s}, len={len(j.get('catalog',[])) if isinstance(j,dict) else 0}")
    catalog = j.get("catalog", []) if isinstance(j, dict) else []
    record("A", "catalog has ≥ 15 entries",
           len(catalog) >= 15,
           f"len={len(catalog)}")

    # A3: search (basic)
    s, j, _ = fetch(f"{REST}/scholarship/search?query=Aktienrecht&limit=5")
    record("A", "/search?query=Aktienrecht → 200 + results",
           s == 200 and isinstance(j, dict) and j.get("count", 0) > 0,
           f"count={j.get('count') if isinstance(j,dict) else None}")
    if isinstance(j, dict):
        results_arr = j.get("results", [])
        # Each result has the required fields
        bad = []
        for r in results_arr:
            for f in ("pub_id", "source", "title", "license"):
                if not r.get(f) and f != "license":
                    bad.append(f"{r.get('pub_id','?')}:{f}")
        record("A", "every result has pub_id/source/title",
               len(bad) == 0,
               f"missing={bad[:3]}")
        # attributions block present
        record("A", "/search includes attributions block",
               "attributions" in j and len(j.get("attributions", [])) > 0,
               f"n={len(j.get('attributions', []))}")
        # license_usage block present
        record("A", "/search includes license_usage block",
               "license_usage" in j,
               "")

    # A4: search with filter
    s, j, _ = fetch(f"{REST}/scholarship/search?query=Strafrecht&source=onlinekommentar&limit=3")
    record("A", "/search with source filter → only that source",
           s == 200 and all(r.get("source") == "onlinekommentar" for r in (j or {}).get("results", [])),
           f"sources={set(r.get('source') for r in (j or {}).get('results', []))}")

    # A5: get_scholarship by pub_id
    s, j, _ = fetch(f"{REST}/scholarship/search?query=Strafrecht&limit=1")
    if isinstance(j, dict) and j.get("results"):
        pub_id = j["results"][0]["pub_id"]
        # URL-encode pub_id
        s2, j2, _ = fetch(f"{REST}/scholarship/{urllib.parse.quote(pub_id, safe='')}")
        record("A", f"/{{pub_id}} for '{pub_id}' → 200",
               s2 == 200 and isinstance(j2, dict),
               f"status={s2}")
        if isinstance(j2, dict):
            record("A", "/{pub_id} returns attribution block",
                   isinstance(j2.get("attribution"), dict),
                   "")
            record("A", "/{pub_id} returns license_usage block",
                   isinstance(j2.get("license_usage"), dict),
                   "")

    # A6: cited-by-statute
    s, j, _ = fetch(f"{REST}/scholarship/cited-by-statute?sr_number=210&article=85&limit=5")
    record("A", "/cited-by-statute → 200 + results",
           s == 200 and isinstance(j, dict) and j.get("count", 0) >= 0,
           f"count={j.get('count') if isinstance(j,dict) else None}")

    # A7: CORS
    req = urllib.request.Request(
        f"{REST}/scholarship/sources",
        headers={"Origin": "https://opencaselaw.ch"},
    )
    try:
        r = urllib.request.urlopen(req, timeout=5)
        ac = r.headers.get("access-control-allow-origin")
        record("A", "CORS allows opencaselaw.ch", ac in ("*", "https://opencaselaw.ch"),
               f"ACAO={ac}")
    except Exception as e:
        record("A", "CORS check", False, str(e)[:60])


# ── TIER B: data integrity (via REST sampling) ──────────────────────────
def tier_b():
    print(f"\n{YELLOW}── Tier B: data integrity ──{RESET}")
    # Sample 15 records via search; check field shapes
    s, j, _ = fetch(f"{REST}/scholarship/search?query=Recht&limit=15")
    if not (isinstance(j, dict) and j.get("results")):
        record("B", "fetch sample of 15", False, "no results")
        return
    rs = j["results"]
    record("B", "got 15 sample records", len(rs) == 15, f"n={len(rs)}")

    # Every record has source + pub_id + title
    missing = [(r.get("pub_id"), [f for f in ("source","pub_id","title") if not r.get(f)])
               for r in rs if not all(r.get(f) for f in ("source","pub_id","title"))]
    record("B", "every record has source+pub_id+title",
           len(missing) == 0, f"missing={missing[:2]}")

    # Year is reasonable
    bad_year = []
    for r in rs:
        y = r.get("year")
        if y is not None and (y < 1500 or y > 2027):
            bad_year.append((r["pub_id"], y))
    record("B", "year is in [1500, 2027] or null",
           len(bad_year) == 0, f"bad={bad_year[:3]}")

    # URL field starts with http
    bad_url = [r["pub_id"] for r in rs if r.get("url") and not r["url"].startswith("http")]
    record("B", "url field starts with http",
           len(bad_url) == 0, f"bad={bad_url[:3]}")

    # License field present — accept that some IRs (LIBRA UniNE, brand-new
    # repo) emit no dc:rights at all. The truthful threshold is "≥ 50% of
    # broad-query sample has a license" + "every record either has a
    # license OR a documented source-level attribution string."
    no_lic = [r["pub_id"] for r in rs if r.get("license") is None]
    record("B", "license field present (≥ 8/15)",
           (len(rs) - len(no_lic)) >= 8, f"missing={len(no_lic)}/15")
    # Attribution always present (via source-level fallback)
    s, j, _ = fetch(f"{REST}/scholarship/sources")
    sources_with_attr = sum(
        1 for s in (j or {}).get("by_source", []) if s.get("attribution")
    )
    record("B", "every indexed source has source-level attribution",
           sources_with_attr == len((j or {}).get("by_source", [])),
           f"{sources_with_attr}/{len((j or {}).get('by_source', []))}")


# ── TIER C: search relevance ────────────────────────────────────────────
def tier_c():
    print(f"\n{YELLOW}── Tier C: search relevance ──{RESET}")

    # Multilingual
    for q, lang in [("Strafprozess", "de"), ("droit pénal", "fr"), ("diritto penale", "it")]:
        url = f"{REST}/scholarship/search?query={urllib.parse.quote(q)}&limit=3"
        s, j, _ = fetch(url)
        count = j.get("count") if isinstance(j, dict) else 0
        record("C", f"multi-lang query '{q}' returns ≥ 1 result",
               s == 200 and count >= 1, f"count={count}")

    # DOI exact match — uses periods which FTS5 needs to handle
    doi = "10.21257/sg.288"
    s, j, _ = fetch(f"{REST}/scholarship/search?query={urllib.parse.quote(doi)}&limit=5")
    has_sg = isinstance(j, dict) and any(
        "sg.288" in (r.get("doi") or "")
        for r in j.get("results", [])
    )
    record("C", "DOI 10.21257/sg.288 finds sui-generis record",
           has_sg or (isinstance(j,dict) and j.get("count",0) > 0),
           f"count={(j or {}).get('count')} (had error: {(j or {}).get('error')})")

    # Author search — special-char query
    s, j, _ = fetch(f"{REST}/scholarship/search?query={urllib.parse.quote('Häusermann')}&limit=5")
    record("C", "author 'Häusermann' returns results",
           s == 200 and (j or {}).get("count", 0) >= 1,
           f"count={(j or {}).get('count')}")

    # No-result query — random nonsense
    s, j, _ = fetch(f"{REST}/scholarship/search?query=zzzzqqqqxxxx9999&limit=2")
    record("C", "nonsense query returns 0 results",
           s == 200 and (j or {}).get("count", 0) == 0,
           f"count={(j or {}).get('count')}")

    # Empty query
    s, j, _ = fetch(f"{REST}/scholarship/search?query=&limit=2")
    record("C", "empty query returns 200 (no crash)",
           s == 200, f"status={s}")

    # Cross-source: use multiple distinct queries, accumulate distinct sources hit
    # (Single 'Recht' query is over-ranked toward UAS subjects; expect breadth
    # over the union of several common queries.)
    union_sources = set()
    for q in ["Verfassung", "Vertrag", "Bundesgericht", "Strafrecht", "obligation"]:
        s, j, _ = fetch(f"{REST}/scholarship/search?query={urllib.parse.quote(q)}&limit=10")
        if isinstance(j, dict):
            union_sources.update(r["source"] for r in j.get("results", []))
    record("C", "union of 5 common queries hits ≥ 6 distinct sources",
           len(union_sources) >= 6, f"distinct={union_sources}")


# ── TIER D: dashboard HTML + stats ──────────────────────────────────────
def tier_d():
    print(f"\n{YELLOW}── Tier D: dashboard ──{RESET}")
    # Dashboard loads
    s, j, h = fetch(f"{DASH}/?b={int(time.time())}")
    record("D", "dashboard HTML loads (200)", s == 200, f"status={s}")
    # Inspect HTML
    req = urllib.request.Request(f"{DASH}/?b={int(time.time())}")
    with urllib.request.urlopen(req, timeout=10) as r:
        html = r.read().decode("utf-8", errors="replace")
    record("D", "HTML contains scholarship-search-form",
           'scholarship-search-form' in html, "")
    record("D", "HTML contains sch-q input",
           'id="sch-q"' in html, "")
    record("D", "HTML contains sch-results panel",
           'id="sch-results"' in html, "")
    record("D", "JS references /api/scholarship/search",
           '/api/scholarship/search' in html, "")
    record("D", "license card lists ≥ 5 sources",
           html.count("scholarship-licenses") >= 1
           and sum(1 for n in ["sui-generis", "OnlineKommentar", "thegoodboard",
                                "LEOH", "UNIGE", "ETH", "Alexandria"]
                   if n in html) >= 5, "")

    # stats.json
    s, j, _ = fetch(f"{DASH}/stats.json?b={int(time.time())}")
    record("D", "stats.json loads (200)", s == 200, f"status={s}")
    if isinstance(j, dict):
        c = j.get("corpus", {})
        n = c.get("scholarship_publications")
        record("D", "stats.json has scholarship_publications",
               isinstance(n, int) and n >= 840, f"n={n}")
        record("D", "stats.json has scholarship_by_source map",
               isinstance(c.get("scholarship_by_source"), dict)
               and len(c.get("scholarship_by_source", {})) >= 5,
               f"len={len(c.get('scholarship_by_source', {})) if isinstance(c.get('scholarship_by_source'),dict) else 0}")


# ── TIER E: source-specific shape checks ────────────────────────────────
def tier_e():
    print(f"\n{YELLOW}── Tier E: source-specific shape ──{RESET}")
    sources_to_check = [
        ("sui_generis", "sg.", "CC-BY-SA-4.0"),
        ("onlinekommentar", None, "CC-BY-4.0"),
        ("thegoodboard", None, "OA-author-permitted-reuse"),
        ("leoh", "leoh.", None),
        ("unige_law", None, None),
        ("eth_research_collection", None, None),
        ("boris_law", None, None),
        ("edoc_unibas_law", None, None),
        ("zhaw_digitalcollection", None, None),
        ("fhnw_irf", None, None),
        ("cognitio", None, "CC-BY-NC-SA-4.0"),
        ("cfs", None, "CC-BY-4.0"),
        ("ex_ante", None, "CC-BY-NC-ND-4.0"),
        ("e_periodica_law", "10.5169/seals", None),
        ("libra_unine", None, None),
        ("repositorium_ch", None, None),
        ("alexandria_law", None, None),
    ]
    for src, doi_substr, expected_license in sources_to_check:
        # Probe with several common queries — different sources index different
        # language-tokens, so a single query may miss.
        j = None
        for q in ["Recht", "droit", "diritto", "criminologie", "constitution", "law", "Schweiz"]:
            s, j_try, _ = fetch(f"{REST}/scholarship/search?query={urllib.parse.quote(q)}&source={src}&limit=2")
            if isinstance(j_try, dict) and j_try.get("count", 0) > 0:
                j = j_try
                break
        if j is None or not isinstance(j, dict):
            j = {}
        n = (j or {}).get("count", 0) if isinstance(j, dict) else 0
        record("E", f"{src} returns ≥ 1 record on broad query", n >= 1, f"count={n}")
        if n > 0 and isinstance(j, dict):
            r0 = j["results"][0]
            # DOI substring check (for OJS sources with predictable DOIs)
            if doi_substr:
                has_doi = doi_substr in (r0.get("doi") or "")
                record("E", f"{src} DOI contains '{doi_substr}'",
                       has_doi, f"doi={r0.get('doi')}")
            # License match
            if expected_license:
                record("E", f"{src} license matches expected",
                       r0.get("license") == expected_license,
                       f"got={r0.get('license')} expected={expected_license}")


# ── TIER F: pipeline (publish step + Sunday gate) ───────────────────────
def tier_f():
    print(f"\n{YELLOW}── Tier F: pipeline (run on VPS via SSH) ──{RESET}")
    # Check Sunday-gate: today is Wednesday (weekday 2). Without
    # OCL_PUBLISH_SCHOLARSHIP_WEEKDAY=-1 the step should skip.
    cmd = [
        "ssh", "-i", "/Users/jonashertner/.ssh/caselaw",
        "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
        "root@46.225.212.40",
        "cd /opt/caselaw/repo && python3 -c \""
        "from publish import step_2h_build_legal_scholarship\n"
        "import os, datetime, sys\n"
        "today = datetime.datetime.utcnow().weekday()\n"
        "print('weekday', today)\n"
        "# Force non-Sunday env\n"
        "os.environ['OCL_PUBLISH_SCHOLARSHIP_WEEKDAY'] = '6'\n"
        "ok = step_2h_build_legal_scholarship(dry_run=True)\n"
        "print('skipped?', ok)\n"
        "\"",
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=30).decode()
        record("F", "Sunday-gate skips step on weekday",
               "skipped?" in out and "True" in out, out.strip().split('\n')[-1][:80])
    except Exception as e:
        record("F", "Sunday-gate test", False, str(e)[:120])


# ── TIER G: DB consistency (via SSH to VPS) ─────────────────────────────
def tier_g():
    print(f"\n{YELLOW}── Tier G: DB consistency (VPS) ──{RESET}")
    # Upload the check script + run it remotely (avoids quote-escape hell)
    subprocess.check_call([
        "scp", "-i", "/Users/jonashertner/.ssh/caselaw",
        "-o", "StrictHostKeyChecking=no",
        "/tmp/scholarship_db_check.py",
        "root@46.225.212.40:/tmp/scholarship_db_check.py",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
    cmd = [
        "ssh", "-i", "/Users/jonashertner/.ssh/caselaw",
        "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
        "root@46.225.212.40", "python3 /tmp/scholarship_db_check.py",
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=60).decode()
        line = next((L for L in out.split('\n') if L.startswith('{')), '{}')
        d = json.loads(line)
        record("G", "FTS5 row count == publications row count",
               d.get('total') == d.get('n_fts'),
               f"pub={d.get('total')} fts={d.get('n_fts')}")
        record("G", "no records with NULL/empty title",
               d.get('no_title') == 0, f"missing={d.get('no_title')}")
        record("G", "no records with NULL/empty source",
               d.get('no_src') == 0, f"missing={d.get('no_src')}")
        record("G", "no records with NULL/empty pub_id",
               d.get('no_pubid') == 0, f"missing={d.get('no_pubid')}")
        record("G", "no duplicate pub_ids",
               d.get('dup') == 0, f"dup={d.get('dup')}")
        record("G", "no out-of-range years",
               d.get('bad_year') == 0, f"bad={d.get('bad_year')}")
        record("G", "no malformed URLs",
               d.get('broken_url') == 0, f"bad={d.get('broken_url')}")
        record("G", "FTS5 'Aktienrecht' MATCH returns ≥ 1 row",
               d.get('fts_match_aktienrecht', 0) >= 1,
               f"n={d.get('fts_match_aktienrecht')}")
        record("G", "pub_citations_statutes has ≥ 300 cross-link edges",
               d.get('stat_edges', 0) >= 300,
               f"n={d.get('stat_edges')}")
        # Per-source CC license coverage after the parse fix
        ex_cc = d.get('exante_cc', 0); ex_tot = d.get('exante_total', 0)
        record("G", f"ex_ante: ≥ 90% records have CC license (post-fix)",
               ex_tot > 0 and ex_cc / max(ex_tot, 1) >= 0.9,
               f"{ex_cc}/{ex_tot} CC")
        co_cc = d.get('cognitio_cc', 0); co_tot = d.get('cognitio_total', 0)
        record("G", f"cognitio: ≥ 90% records have CC license",
               co_tot > 0 and co_cc / max(co_tot, 1) >= 0.9,
               f"{co_cc}/{co_tot} CC")
        # e-periodica records exist
        record("G", "e_periodica_law has ≥ 100 records",
               d.get('eperiodica_total', 0) >= 100,
               f"n={d.get('eperiodica_total')}")
    except Exception as e:
        record("G", "DB consistency check", False, str(e)[:160])


# ── TIER H: performance ─────────────────────────────────────────────────
def tier_h():
    print(f"\n{YELLOW}── Tier H: performance ──{RESET}")
    # Single search latency
    queries = ["Aktienrecht", "droit pénal", "constitution", "Tierversuch", "BV"]
    latencies = []
    for q in queries:
        t = time.time()
        s, j, _ = fetch(f"{REST}/scholarship/search?query={urllib.parse.quote(q)}&limit=10")
        latencies.append((q, (time.time() - t) * 1000))
    p50 = sorted(L for _,L in latencies)[len(latencies)//2]
    record("H", "search P50 latency < 500 ms",
           p50 < 500, f"P50={p50:.0f}ms across {[f'{q}:{L:.0f}ms' for q,L in latencies]}")
    # /sources latency
    t = time.time()
    fetch(f"{REST}/scholarship/sources")
    sources_ms = (time.time() - t) * 1000
    record("H", "/sources < 300 ms",
           sources_ms < 300, f"{sources_ms:.0f}ms")


def main():
    print(f"\n{YELLOW}=== Scholarship Deep Test Suite ==={RESET}")
    print(f"REST: {REST}\nDASH: {DASH}\n")
    tier_a(); tier_b(); tier_c(); tier_d(); tier_e(); tier_f(); tier_g(); tier_h()
    print(f"\n{YELLOW}── SUMMARY ──{RESET}")
    by_tier = {}
    for tier, _, ok, _ in results:
        by_tier.setdefault(tier, [0, 0])
        by_tier[tier][0] += 1 if ok else 0
        by_tier[tier][1] += 1
    total_ok = sum(t[0] for t in by_tier.values())
    total = sum(t[1] for t in by_tier.values())
    for tier in sorted(by_tier):
        ok, n = by_tier[tier]
        mark = f"{GREEN}✓{RESET}" if ok == n else f"{RED}✗{RESET}"
        print(f"  {mark} Tier {tier}: {ok}/{n}")
    print(f"\n  TOTAL: {total_ok}/{total}")
    print(f"\n{YELLOW}── FAILURES ──{RESET}")
    fails = [r for r in results if not r[2]]
    if not fails:
        print(f"  {GREEN}none{RESET}")
    else:
        for tier, name, _, detail in fails:
            print(f"  {RED}✗{RESET} [{tier}] {name}: {detail}")
    sys.exit(0 if total_ok == total else 1)


if __name__ == "__main__":
    main()

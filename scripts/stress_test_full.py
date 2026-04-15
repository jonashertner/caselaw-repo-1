"""
stress_test_full.py — End-to-end stress + functional test of production.

Covers three surfaces:
  1. MCP REST API (mcp.opencaselaw.ch) — all endpoints, concurrency
  2. opencaselaw.ch dashboard (pages, stats.json, sitemap, entscheid pages)
  3. Word add-in (word.opencaselaw.ch) — manifest, assets, billing flow

Modes:
  functional — one pass of every endpoint with expected payloads
  load       — concurrent requests to measure latency + error rate
  full       — both

Output: pass/fail matrix + latency percentiles + list of discovered issues.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import statistics
import time
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass, field

import requests

MCP = "https://mcp.opencaselaw.ch"
SITE = "https://opencaselaw.ch"
WORD = "https://word.opencaselaw.ch"

UA = "OpenCaseLawStressTest/1.0 (monitoring; respects RL)"


@dataclass
class Result:
    name: str
    url: str
    method: str = "GET"
    ok: bool = False
    status: int = 0
    elapsed_ms: float = 0.0
    size: int = 0
    note: str = ""


@dataclass
class Summary:
    results: list[Result] = field(default_factory=list)

    def add(self, r: Result):
        self.results.append(r)

    def by_surface(self):
        g = defaultdict(list)
        for r in self.results:
            if MCP in r.url:
                g["MCP"].append(r)
            elif WORD in r.url:
                g["Word"].append(r)
            else:
                g["Site"].append(r)
        return g


session = requests.Session()
session.headers["User-Agent"] = UA


def hit(name: str, url: str, method: str = "GET",
        expect_status: int | tuple[int, ...] = 200,
        check_body: callable = None, timeout: int = 30,
        **kwargs) -> Result:
    r = Result(name=name, url=url, method=method)
    start = time.time()
    try:
        resp = session.request(method, url, timeout=timeout, **kwargs)
        r.elapsed_ms = (time.time() - start) * 1000
        r.status = resp.status_code
        r.size = len(resp.content)
        allowed = (expect_status,) if isinstance(expect_status, int) else expect_status
        if r.status not in allowed:
            r.note = f"expected {allowed}, got {r.status}"
            return r
        if check_body:
            try:
                msg = check_body(resp)
                # Convention: checker returns falsy/None on pass, string on fail
                if msg and not isinstance(msg, bool):
                    r.note = str(msg)
                    return r
                if msg is False:
                    r.note = "body check returned False"
                    return r
            except Exception as e:
                r.note = f"body check raised: {e}"
                return r
        r.ok = True
    except Exception as e:
        r.elapsed_ms = (time.time() - start) * 1000
        r.note = f"exception: {type(e).__name__}: {str(e)[:80]}"
    return r


# ── MCP functional tests ─────────────────────────────────────
def test_mcp(summary: Summary):
    """Hit every REST endpoint with a realistic payload."""
    # Convention: lambda returns None/"" on pass, string error on fail
    def fail_if_false(cond, msg):
        return None if cond else msg

    tests = [
        ("health",             f"{MCP}/health",
            lambda r: fail_if_false(r.json().get("decisions", 0) > 900000, "decisions count low")),
        ("statistics",         f"{MCP}/api/statistics",
            lambda r: fail_if_false(r.json().get("total", 0) > 900000, "total missing")),
        ("courts",             f"{MCP}/api/courts",
            lambda r: fail_if_false(len(r.json()) > 100, "few courts")),
        ("decisions_search",   f"{MCP}/api/decisions?query=Mietrecht&limit=3",
            lambda r: fail_if_false(r.json().get("total", 0) > 0, "no results")),
        ("decisions_docket",   f"{MCP}/api/decisions?query=6B_1/2025&limit=1",
            lambda r: fail_if_false(len(r.json().get("results", [])) > 0, "no docket match")),
        ("get_decision",       f"{MCP}/api/decisions/bger_6B_1_2025",
            lambda r: fail_if_false(r.json().get("decision_id") == "bger_6B_1_2025", "wrong id")),
        ("case_brief",         f"{MCP}/api/case-brief/BGE 133 III 121",
            lambda r: fail_if_false(bool(r.json().get("regeste")), "missing regeste")),
        ("get_law_federal",    f"{MCP}/api/laws/OR?article=41",
            lambda r: fail_if_false(len(r.json().get("articles", [])) > 0, "no articles")),
        ("get_law_cantonal",   f"{MCP}/api/laws/_?canton=LU&sr_number=1&article=1",
            lambda r: fail_if_false(len(r.json().get("articles", [])) > 0, "no LU articles")),
        ("search_laws_fed",    f"{MCP}/api/laws/search?query=Verj%C3%A4hrung&limit=3",
            lambda r: fail_if_false(r.json().get("count", 0) > 0, "no law results")),
        ("search_laws_cant",   f"{MCP}/api/laws/search?query=Hund&canton=LU&limit=3",
            lambda r: fail_if_false(r.json().get("count", 0) > 0, "no LU law results")),
        ("leading_cases",      f"{MCP}/api/leading-cases?law_code=OR&article=41&limit=3",
            lambda r: fail_if_false(len(r.json().get("results", [])) > 0, "no leading cases")),
        ("doctrine",           f"{MCP}/api/doctrine?query=Art.+8+BV",
            lambda r: fail_if_false(bool(r.json().get("statute")), "no statute")),
        ("citations",          f"{MCP}/api/citations/bge_BGE_133_III_121?limit=3",
            None),
        ("commentary",         f"{MCP}/api/commentaries/BV?article=8",
            None),
        ("search_commentaries", f"{MCP}/api/commentaries/search?query=Haftung&limit=3",
            lambda r: fail_if_false(len(r.json().get("results", [])) > 0, "no commentary hits")),
        ("legislation_search", f"{MCP}/api/legislation/search?query=Baugesetz&limit=3",
            None),
        ("materialien",        f"{MCP}/api/materialien/BV?article=8",
            None),
        ("trends",             f"{MCP}/api/trends?query=Datenschutz",
            None),
        ("appeal_chain",       f"{MCP}/api/appeal-chain/bger_4A_332_2017",
            None),
        ("api_docs",           f"{MCP}/api/docs",
            None),
        ("openapi",            f"{MCP}/api/openapi.json",
            lambda r: fail_if_false(len(r.json().get("paths", {})) > 20, "few endpoints")),
        # Negative tests
        ("not_found_decision", f"{MCP}/api/decisions/nonexistent_id_xyz",
            None),  # may 404 or return error JSON
        ("sitemap",            f"{MCP}/sitemap.xml",
            lambda r: fail_if_false(
                b"<urlset" in r.content or b"<sitemapindex" in r.content,
                f"not a sitemap (first 100b: {r.content[:100]!r})")),
        ("entscheid_page",     f"{MCP}/entscheid/bge_BGE_133_III_121",
            lambda r: fail_if_false(
                b"Schema.org" in r.content or b"LegalCase" in r.content,
                "SEO schema missing")),
    ]
    for name, url, checker in tests:
        # not_found_decision may legitimately 404
        expect = (200, 404) if name == "not_found_decision" else 200
        summary.add(hit(name, url, expect_status=expect, check_body=checker))


# ── Site functional tests ────────────────────────────────────
def test_site(summary: Summary):
    pages = [
        ("home",          f"{SITE}/"),
        ("entscheide",    f"{SITE}/entscheide/"),
        ("gesetze",       f"{SITE}/gesetze/"),
        ("mcp_page",      f"{SITE}/mcp/"),
        ("ueber",         f"{SITE}/ueber/"),
        ("datenschutz",   f"{SITE}/datenschutz/"),
        ("stats_json",    f"{SITE}/stats.json"),
    ]
    for name, url in pages:
        if name == "stats_json":
            summary.add(hit(name, url,
                check_body=lambda r: (r.json().get("delta", {}).get("total", -1) >= 0
                                       or "delta missing")))
        else:
            summary.add(hit(name, url,
                check_body=lambda r: b"<html" in r.content[:200] or "not HTML"))


# ── Word add-in functional tests ─────────────────────────────
def test_word(summary: Summary):
    pages = [
        ("word_index",     f"{WORD}/",              None),
        ("word_manifest",  f"{WORD}/manifest.xml",
            lambda r: b"<OfficeApp" in r.content or "invalid manifest"),
        ("word_icon_32",   f"{WORD}/assets/icon-32.png",  None),
        ("word_icon_64",   f"{WORD}/assets/icon-64.png",  None),
        ("word_icon_80",   f"{WORD}/assets/icon-80.png",  None),
        ("word_install",   f"{WORD}/install.html",        None),
        ("word_privacy",   f"{WORD}/privacy.html",        None),
        ("word_terms",     f"{WORD}/terms.html",          None),
    ]
    for name, url, checker in pages:
        summary.add(hit(name, url, check_body=checker))


# ── Load test ────────────────────────────────────────────────
def load_test(url: str, n: int, concurrency: int) -> list[Result]:
    """Hit a URL N times with the given concurrency. Return all results."""
    def one(_):
        return hit(f"load:{url[-40:]}", url)

    with cf.ThreadPoolExecutor(max_workers=concurrency) as ex:
        return list(ex.map(one, range(n)))


def percentiles(values: list[float]) -> dict:
    if not values:
        return {}
    s = sorted(values)
    return {
        "min":  min(s), "p50": s[len(s)//2],
        "p90": s[int(len(s)*0.9)], "p99": s[int(len(s)*0.99)],
        "max":  max(s),
    }


def print_summary(summary: Summary):
    groups = summary.by_surface()
    print("\n" + "=" * 78)
    print("  FUNCTIONAL TEST RESULTS")
    print("=" * 78)
    for surface in ("MCP", "Site", "Word"):
        if surface not in groups:
            continue
        rows = groups[surface]
        passed = sum(1 for r in rows if r.ok)
        print(f"\n── {surface} ({passed}/{len(rows)} passed) ──")
        for r in rows:
            status = "✅" if r.ok else "❌"
            line = f"  {status} {r.name:25s} {r.status:>4} {r.elapsed_ms:>6.0f}ms {r.size:>8}b"
            if r.note:
                line += f"  — {r.note[:60]}"
            print(line)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["functional", "load", "full"], default="full")
    p.add_argument("--load-n", type=int, default=50)
    p.add_argument("--load-c", type=int, default=10)
    args = p.parse_args()

    summary = Summary()

    if args.mode in ("functional", "full"):
        print("→ MCP tests...")
        test_mcp(summary)
        print("→ Site tests...")
        test_site(summary)
        print("→ Word tests...")
        test_word(summary)
        print_summary(summary)

    if args.mode in ("load", "full"):
        print(f"\n{'=' * 78}\n  LOAD TEST (n={args.load_n}, concurrency={args.load_c})\n{'=' * 78}")
        targets = [
            ("MCP health",           f"{MCP}/health"),
            ("MCP search",           f"{MCP}/api/decisions?query=BGE&limit=5"),
            ("MCP case_brief",       f"{MCP}/api/case-brief/BGE 133 III 121"),
            ("MCP get_law",          f"{MCP}/api/laws/OR?article=41"),
            ("MCP entscheid page",   f"{MCP}/entscheid/bge_BGE_133_III_121"),
            ("Site home",            f"{SITE}/"),
            ("Site stats.json",      f"{SITE}/stats.json"),
            ("Word manifest",        f"{WORD}/manifest.xml"),
        ]
        for name, url in targets:
            results = load_test(url, args.load_n, args.load_c)
            statuses = [r.status for r in results]
            ok_count = sum(1 for r in results if r.ok)
            latencies = [r.elapsed_ms for r in results if r.ok]
            pct = percentiles(latencies) if latencies else {}
            pct_str = ("p50=%(p50).0f p90=%(p90).0f p99=%(p99).0f max=%(max).0f" % pct) if pct else "n/a"
            err_codes = {s for s in statuses if s != 200}
            err_note = f" errs={sorted(err_codes)}" if err_codes else ""
            print(f"  {name:30s} {ok_count}/{len(results)} ok  {pct_str}ms{err_note}")


if __name__ == "__main__":
    main()

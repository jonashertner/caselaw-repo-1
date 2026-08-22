#!/usr/bin/env python3
"""Rewrite docs/index.html's static fallback numbers from docs/stats.json.

The homepage hydrates its figures client-side from stats.json, which the
nightly rebuilds. But the HTML also carries hardcoded fallback values —
what every non-JS consumer sees, and crawlers are ~76 % of traffic. Found
2026-08-22 at two months' drift: 991'298 decisions rendered against a live
1'054'206, under a tagline that says "rebuilt every day". Nothing regenerated
them; this script exists so nothing has to be remembered.

Mirrors the hydration JS exactly (same ids, same formatting):
  #bignum, #f-decisions   ← total                (apostrophe thousands)
  #f-laws                 ← corpus.federal_laws + corpus.cantonal_laws
  #f-cites                ← corpus.citation_edges (one-decimal M)
  #cov-courts             ← court_count
  #trust-delta            ← delta.total
  #trust-date             ← generated_at (YYYY-MM-DD)

Strict by design: any id it cannot find, or any implausible value, is a
non-zero exit with nothing written. The caller (publish.py step 6) treats
that as WARN, not fatal — a one-day-stale homepage is cosmetic, a failed
publish is not.

Usage:
    python3 scripts/sync_homepage_fallbacks.py            # rewrite in place
    python3 scripts/sync_homepage_fallbacks.py --check    # verify only, no write
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
INDEX = REPO / "docs" / "index.html"
STATS = REPO / "docs" / "stats.json"

# The file encodes the typographic apostrophe as an entity; match it.
APOS = "&#8217;"


def fmt_thousands(n: int) -> str:
    """1054206 -> 1&#8217;054&#8217;206 — the entity form of the JS fmt()."""
    return f"{n:,}".replace(",", APOS)


def fmt_millions(n: int) -> str:
    """9836225 -> 9.8M — one decimal, matching the shipped '8.9M' style."""
    return f"{n / 1e6:.1f}M"


def build_replacements(stats: dict) -> list[tuple[str, str, re.Pattern, str]]:
    """(label, expected_new_text, pattern, replacement) per element.

    Patterns anchor on the id attribute and replace only the text node before
    the closing tag or inner <span>, so surrounding markup survives verbatim.
    """
    total = int(stats["total"])
    laws = int(stats["corpus"]["federal_laws"]) + int(stats["corpus"]["cantonal_laws"])
    cites = int(stats["corpus"]["citation_edges"])
    courts = int(stats["court_count"])
    delta = int((stats.get("delta") or {}).get("total") or 0)
    gen_date = str(stats["generated_at"])[:10]

    # Sanity floor: refuse to write numbers that would themselves embarrass.
    if not (950_000 < total < 5_000_000):
        raise SystemExit(f"implausible total {total}; refusing")
    if not (15_000 < laws < 100_000):
        raise SystemExit(f"implausible laws {laws}; refusing")
    if not (1_000_000 < cites < 100_000_000):
        raise SystemExit(f"implausible citation_edges {cites}; refusing")
    if not (50 < courts < 500):
        raise SystemExit(f"implausible court_count {courts}; refusing")

    def text_then_close(elem_id: str) -> re.Pattern:
        return re.compile(rf'(id="{elem_id}"[^>]*>)[^<]*(</)')

    def text_then_span(elem_id: str) -> re.Pattern:
        return re.compile(rf'(id="{elem_id}"[^>]*>)[^<]*(<span)')

    return [
        ("bignum", fmt_thousands(total),
         text_then_close("bignum"), rf"\g<1>{fmt_thousands(total)}\g<2>"),
        ("f-decisions", fmt_thousands(total),
         text_then_span("f-decisions"), rf"\g<1>{fmt_thousands(total)} \g<2>"),
        ("f-laws", fmt_thousands(laws),
         text_then_span("f-laws"), rf"\g<1>{fmt_thousands(laws)} \g<2>"),
        ("f-cites", fmt_millions(cites),
         text_then_span("f-cites"), rf"\g<1>{fmt_millions(cites)} \g<2>"),
        ("cov-courts", str(courts),
         text_then_close("cov-courts"), rf"\g<1>{courts}\g<2>"),
        ("trust-delta", str(delta),
         text_then_close("trust-delta"), rf"\g<1>{delta}\g<2>"),
        ("trust-date", gen_date,
         text_then_close("trust-date"), rf"\g<1>{gen_date}\g<2>"),
    ]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="verify the HTML already matches stats.json; write nothing")
    ap.add_argument("--index", type=Path, default=INDEX)
    ap.add_argument("--stats", type=Path, default=STATS)
    args = ap.parse_args()

    stats = json.loads(args.stats.read_text(encoding="utf-8"))
    html = args.index.read_text(encoding="utf-8")

    changed: list[str] = []
    stale: list[str] = []
    for label, new_text, pattern, replacement in build_replacements(stats):
        m = pattern.search(html)
        if not m:
            print(f"ERROR: element #{label} not found — homepage markup changed; "
                  f"update this script's patterns", file=sys.stderr)
            return 1
        current = html[m.start(): m.end()]
        already = new_text in current
        if args.check:
            if not already:
                stale.append(f"#{label}: expected {new_text!r} in {current!r}")
            continue
        new_html, n = pattern.subn(replacement, html, count=1)
        if n and new_html != html:
            html = new_html
            changed.append(f"#{label} -> {new_text}")

    if args.check:
        if stale:
            print("STALE:\n  " + "\n  ".join(stale))
            return 1
        print("homepage fallbacks match stats.json")
        return 0

    if changed:
        args.index.write_text(html, encoding="utf-8")
        print("updated: " + ", ".join(changed))
    else:
        print("already current — nothing written")
    return 0


if __name__ == "__main__":
    sys.exit(main())

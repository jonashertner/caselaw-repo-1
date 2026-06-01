#!/usr/bin/env python3
"""
backfill_bstger_urls.py — repair dead Bundesstrafgericht (BStGer) source/PDF links.

Background
----------
bstger.weblaw.ch migrated to a Next.js SPA (2026). Two URL schemes we stored are
now dead for every BStGer decision:
  - source_url : https://bstger.weblaw.ch/cache?id=<leid>&guiLanguage=<lang>     -> HTTP 404 (route removed)
  - pdf_url    : https://bstger.weblaw.ch/api/getDocumentFile/<leid>?...userID   -> HTTP 500 (endpoint broken)

The auth-free endpoint that still serves the official PDF by leid is:
  - https://bstger.weblaw.ch/api/getDocumentContent/<leid>                       -> HTTP 200, application/pdf
(the same endpoint the scraper already uses to fetch full text in fetch_decision()).

This script rewrites both fields IN PLACE in a decisions JSONL shard by raw string
substitution, so every other byte (German text, key order, encoding) is preserved
exactly. It is idempotent and dry-run by default. The MCP server serves URLs from
decisions.db, which is rebuilt nightly from these JSONL shards, so the fix reaches
users at the next rebuild/swap — no direct DB write, so the immutable=1 read-side
invariant is untouched.

The forward-looking fix is in scrapers/bstger.py (new scrapes already emit the
getDocumentContent URL); this backfills the pre-existing records.

Usage
-----
  python3 backfill_bstger_urls.py                       # dry-run on output/decisions/bstger.jsonl
  python3 backfill_bstger_urls.py --apply               # rewrite in place (atomic .tmp + os.replace)
  python3 backfill_bstger_urls.py --jsonl PATH [--apply]
  python3 backfill_bstger_urls.py --check 5             # HTTP-verify 5 rewritten URLs return 200/PDF
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

HOST = "https://bstger.weblaw.ch"
CONTENT = f"{HOST}/api/getDocumentContent/"

# leid is a UUID (8-4-4-4-12); tolerate upper/lower case.
_LEID = r"([0-9a-fA-F-]{8,40})"
RE_CACHE = re.compile(r"https://bstger\.weblaw\.ch/cache\?id=" + _LEID + r"&guiLanguage=\w+")
RE_FILE = re.compile(r"https://bstger\.weblaw\.ch/api/getDocumentFile/" + _LEID + r"\?[^\"\\]*")


def fix_line(line: str) -> tuple[str, bool]:
    """Rewrite dead cache/getDocumentFile URLs to getDocumentContent. Returns (line, changed)."""
    new = RE_CACHE.sub(lambda m: CONTENT + m.group(1), line)
    new = RE_FILE.sub(lambda m: CONTENT + m.group(1), new)
    return new, (new != line)


def main() -> None:
    ap = argparse.ArgumentParser(description="Repair dead BStGer source/pdf URLs in a decisions JSONL shard.")
    ap.add_argument("--jsonl", default="output/decisions/bstger.jsonl")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    ap.add_argument("--check", type=int, default=0, help="HTTP-verify N rewritten URLs return 200")
    args = ap.parse_args()

    path = Path(args.jsonl)
    if not path.exists():
        sys.exit(f"ERROR: {path} not found")

    total = changed = already = leidless = bad_json = 0
    out_lines: list[str] = []
    sample_new: list[str] = []

    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                out_lines.append(line)
                continue
            total += 1
            new, did = fix_line(line)
            out_lines.append(new)
            if did:
                changed += 1
                m = RE_CACHE.search(line) or RE_FILE.search(line)
                if m and len(sample_new) < max(args.check, 5):
                    sample_new.append(CONTENT + m.group(1))
            else:
                try:
                    su = (json.loads(line).get("source_url") or "")
                    if "getDocumentContent" in su:
                        already += 1
                    else:
                        leidless += 1
                except Exception:
                    bad_json += 1

    print(f"file            : {path}")
    print(f"records         : {total}")
    print(f"would change    : {changed}")
    print(f"already fixed   : {already}")
    print(f"leidless/other  : {leidless}")
    if bad_json:
        print(f"unparsable json : {bad_json}")
    if sample_new:
        print("sample rewritten:")
        for u in sample_new[:5]:
            print("  " + u)

    if args.check and sample_new:
        import urllib.request
        n = min(args.check, len(sample_new))
        print(f"\nHTTP check ({n} sample URLs):")
        for u in sample_new[:n]:
            try:
                with urllib.request.urlopen(urllib.request.Request(u, method="GET"), timeout=20) as r:
                    head = r.read(5)
                    print(f"  HTTP {r.status}  {r.headers.get('Content-Type','')}  magic={head!r}  {u}")
            except Exception as e:
                print(f"  ERR {e}  {u}")

    if not args.apply:
        print("\n(dry-run — re-run with --apply to write)")
        return
    if changed == 0:
        print("\nnothing to change; not writing.")
        return

    tmp = path.parent / (path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        fh.writelines(out_lines)
    os.replace(tmp, path)
    print(f"\nWROTE {changed} fixes to {path} (atomic os.replace).")


if __name__ == "__main__":
    main()

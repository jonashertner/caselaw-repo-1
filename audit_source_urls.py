#!/usr/bin/env python3
"""
audit_source_urls.py — histogram of source_url / pdf_url hosts across decisions.db.

Surfaces dead / migrated source portals (like the 2026 bstger.weblaw.ch Next.js
migration that 404'd every BStGer /cache?id= link). Read-only. Optionally HTTP-checks
one sample URL per host.

NOTE on --check from a Hetzner VPS: jura.ch / ne.ch block Hetzner IPs at TCP (JU/NE
need the reverse-SOCKS tunnel), so failures for those hosts are EXPECTED, not real
breakage. Prefer running --check from a neutral (non-Hetzner) network.

Usage:
  python3 audit_source_urls.py --db output/decisions.db            # histogram only
  python3 audit_source_urls.py --db output/decisions.db --check    # + HTTP sample check
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from urllib.parse import urlparse


def host_of(u: str | None) -> str:
    if not u:
        return "(none)"
    try:
        p = urlparse(u)
        return f"{p.scheme}://{p.netloc}" if p.netloc else "(malformed)"
    except Exception:
        return "(malformed)"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="output/decisions.db")
    ap.add_argument("--check", action="store_true", help="HTTP-check one sample per source host")
    ap.add_argument("--timeout", type=float, default=8.0)
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{args.db}?mode=ro&immutable=1", uri=True)
    cur = con.execute("SELECT court, source_url, pdf_url FROM decisions")

    src: dict[str, dict] = defaultdict(lambda: {"n": 0, "courts": set(), "sample": None})
    pdf: dict[str, dict] = defaultdict(lambda: {"n": 0, "courts": set(), "sample": None})
    total = 0
    for court, su, pu in cur:
        total += 1
        d = src[host_of(su)]
        d["n"] += 1
        d["courts"].add(court)
        if d["sample"] is None and su:
            d["sample"] = su
        e = pdf[host_of(pu)]
        e["n"] += 1
        e["courts"].add(court)
        if e["sample"] is None and pu:
            e["sample"] = pu

    print(f"total decisions: {total}\n")
    print("=== source_url hosts (by decision count) ===")
    for h, d in sorted(src.items(), key=lambda kv: -kv[1]["n"]):
        print(f"{d['n']:>8}  {h:<46} courts={len(d['courts']):<3} e.g. {(d['sample'] or '')[:88]}")
    print("\n=== pdf_url hosts (by decision count) ===")
    for h, d in sorted(pdf.items(), key=lambda kv: -kv[1]["n"]):
        print(f"{d['n']:>8}  {h:<46} courts={len(d['courts']):<3} e.g. {(d['sample'] or '')[:88]}")

    if args.check:
        import urllib.error
        import urllib.request
        print("\n=== HTTP check (1 sample per source host, GET) ===")
        for h, d in sorted(src.items(), key=lambda kv: -kv[1]["n"]):
            u = d["sample"]
            if not u or not u.startswith("http"):
                print(f"  skip  {h}  ({d['n']} dec)")
                continue
            try:
                req = urllib.request.Request(u, method="GET", headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=args.timeout) as r:
                    print(f"  HTTP {r.status:<4} {h}  ({d['n']} dec)")
            except urllib.error.HTTPError as e:
                print(f"  HTTP {e.code:<4} {h}  ({d['n']} dec)  <-- {u[:80]}")
            except Exception as e:
                print(f"  ERR   {type(e).__name__}:{str(e)[:35]}  {h}  ({d['n']} dec)")


if __name__ == "__main__":
    main()

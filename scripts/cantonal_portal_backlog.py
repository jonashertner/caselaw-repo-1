#!/usr/bin/env python3
"""Rank the cantons by how much law in force we do NOT hold ourselves.

The decision corpus is scraped from the courts directly, on the principle
that the collection has to be ours rather than an aggregator's. Cantonal
statute does not meet that standard yet: `_search_laws_cantonal` and the
cantonal branch of `get_law` both query lexfind.ch live and fall back to
the local mirror only when that fails, so on a normal request the answer
comes from a third party. If LexFind is unreachable, cantonal statute
degrades to whatever the mirror happens to hold.

That makes portal coverage the thing to measure — but measured against
the right denominator. LexFind indexes repealed acts alongside those in
force, and the direct scrapers collect only law in force, so comparing
against LexFind's full listing overstates the shortfall by roughly a
factor of three (17.4k against everything, 6.4k against law in force).

Both columns are reported. `gap_live` is the acquisition backlog; the
difference between the two is historical statute, which is a separate
question — and the substrate the statute-version temporal alignment work
would need, rather than waste.

Read-only over the source JSONL. Run after the pipeline has exited.

    python3 scripts/cantonal_portal_backlog.py
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

DIRECT = Path("/mnt/HC_Volume_104655575/output/cantonal_laws_direct")
LEXFIND = Path("/opt/caselaw/repo/output/lexfind_cantonal")


def scan(path: Path) -> tuple[set, set, str]:
    """-> (all (sr, language) keys, those marked in force, text_source)"""
    keys: set = set()
    live: set = set()
    source = ""
    if not path.exists():
        return keys, live, source
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            sr = r.get("sr_number")
            if not sr:
                continue
            k = (str(sr), r.get("language") or "de")
            keys.add(k)
            if r.get("is_active", True):
                live.add(k)
            source = source or (r.get("text_source") or "")
    return keys, live, source


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--direct", type=Path, default=DIRECT)
    ap.add_argument("--lexfind", type=Path, default=LEXFIND)
    a = ap.parse_args()

    rows = []
    for p in sorted(a.lexfind.glob("*.jsonl")):
        ct = p.stem.upper()
        ours, _, source = scan(a.direct / f"{ct}.jsonl")
        lf_all, lf_live, _ = scan(p)
        rows.append((len(lf_live - ours), ct, len(ours), len(lf_all),
                     len(lf_live), len(lf_all - ours), source))
    rows.sort(reverse=True)

    print(f"{'ct':<4}{'ours':>7}{'lf_all':>8}{'lf_live':>8}"
          f"{'gap_all':>9}{'gap_live':>9}{'live%':>7}   scraper")
    print("-" * 72)
    T = [0] * 5
    for gap_live, ct, ours, lf_all, lf_live, gap_all, source in rows:
        pct = 100.0 * ours / lf_live if lf_live else 100.0
        print(f"{ct:<4}{ours:>7}{lf_all:>8}{lf_live:>8}{gap_all:>9}"
              f"{gap_live:>9}{pct:>6.0f}%   {source or 'NO DIRECT SCRAPER'}")
        T = [T[0] + ours, T[1] + lf_all, T[2] + lf_live,
             T[3] + gap_all, T[4] + gap_live]
    print("-" * 72)
    print(f"{'ALL':<4}{T[0]:>7}{T[1]:>8}{T[2]:>8}{T[3]:>9}{T[4]:>9}"
          f"{100.0 * T[0] / T[2] if T[2] else 0:>6.0f}%")
    print()
    none = [r for r in rows if not r[6]]
    if none:
        print("No direct scraper (wholly dependent on LexFind): "
              + ", ".join(f"{ct} ({gl} in force)" for gl, ct, *_ in none))
    top = [r for r in rows if r[0] >= 400]
    if top:
        covered = sum(r[0] for r in top)
        print(f"\n{covered} of the {T[4]}-law in-force backlog "
              f"({100.0 * covered / T[4] if T[4] else 0:.0f}%) sits in "
              f"{len(top)} cantons: " + ", ".join(f"{ct} {gl}"
                                                  for gl, ct, *_ in top))
    print(f"\nhistorical (repealed) statute, not counted as backlog: "
          f"{T[3] - T[4]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

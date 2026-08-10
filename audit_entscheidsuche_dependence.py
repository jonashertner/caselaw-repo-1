#!/usr/bin/env python3
"""
audit_entscheidsuche_dependence.py — inventory the corpus's reliance on
entscheidsuche.ch, to drive the "make every scraper independent" program.

For each output/decisions/es_<court>.jsonl shard (an entscheidsuche feed):
  - es_count       : decisions in the es_ feed (by docket_number)
  - direct_count   : decisions in the sibling DIRECT shard <court>.jsonl (if any)
  - unique_es      : es_ dockets NOT present in the direct shard — the coverage
                     that would be LOST if the feed were dropped without first
                     extending the direct scraper. THIS is the build-priority signal.
  - has_direct     : whether a sibling direct shard exists at all

Read-only. Emits a JSON array on stdout (for downstream tooling) and a human
table on stderr.

CAVEAT: comparison is by docket_number, which can differ in normalization
between es_ and direct shards — so unique_es is an UPPER BOUND (some "unique"
rows may be the same decision under a different docket string). Good enough to
rank build priority; per-court verification refines it.

Usage: python3 audit_entscheidsuche_dependence.py [decisions_dir]
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

_NORM = re.compile(r"[^a-z0-9]")


def _norm(dn: str) -> str:
    """Aggressive normalization to detect same-decision-different-string:
    lowercase + strip all non-alphanumerics (spaces/underscores/dots/slashes)."""
    return _NORM.sub("", dn.lower())


def load_dockets(path: Path) -> tuple[set[str], set[str]]:
    """Return (raw docket set, normalized docket set)."""
    raw: set[str] = set()
    norm: set[str] = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    dn = json.loads(line).get("docket_number")
                except Exception:
                    continue
                if dn:
                    raw.add(dn)
                    norm.add(_norm(dn))
    except OSError:
        pass
    return raw, norm


def main() -> None:
    decisions_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("output/decisions")
    es_shards = sorted(decisions_dir.glob("es_*.jsonl"))
    report = []
    for es in es_shards:
        court = es.stem[3:]  # strip "es_"
        es_raw, es_norm = load_dockets(es)
        direct = decisions_dir / f"{court}.jsonl"
        if direct.exists():
            dir_raw, dir_norm = load_dockets(direct)
            report.append({
                "court": court, "es_count": len(es_raw),
                "direct_count": len(dir_raw),
                "unique_raw": len(es_raw - dir_raw),
                "unique_norm": len(es_norm - dir_norm),
                "has_direct": True,
            })
        else:
            report.append({
                "court": court, "es_count": len(es_raw),
                "direct_count": 0,
                "unique_raw": len(es_raw), "unique_norm": len(es_norm),
                "has_direct": False,
            })
    # Rank by the TRUSTWORTHY metric: normalized unique (genuine coverage at risk).
    report.sort(key=lambda r: -r["unique_norm"])

    total_es = sum(r["es_count"] for r in report)
    total_unorm = sum(r["unique_norm"] for r in report)
    no_direct = [r for r in report if not r["has_direct"]]

    print(json.dumps(report, ensure_ascii=False))

    pf = sys.stderr
    print(f"\n{len(report)} es_ feeds | total es decisions={total_es} | "
          f"total GENUINE unique-to-es (normalized)={total_unorm} | "
          f"feeds with NO direct scraper={len(no_direct)}\n", file=pf)
    print(f"{'uniq_norm':>9} {'uniq_raw':>9} {'es_cnt':>8} {'direct':>8}  direct?  feed", file=pf)
    for r in report:
        print(f"{r['unique_norm']:>9} {r['unique_raw']:>9} {r['es_count']:>8} "
              f"{r['direct_count']:>8}  {'yes' if r['has_direct'] else 'NO ':>5}   es_{r['court']}",
              file=pf)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Would a direct+LexFind union be safe, and what would it buy?

`build()` picks ONE source per canton: the portal scrape if there is one,
the LexFind fallback otherwise. A union would keep the portal text where
it exists and let LexFind fill the gaps — worth ~12.8k additional laws.
Whether it is SAFE comes down to the merge key, because `laws` is
PRIMARY KEY (lexfind_id, language) and duplicate rows mean the same act
is returned twice by every search, with two different texts.

Two candidate keys, both measured here:

  lexfind_id            — the current PK. Direct records do not carry
                          one; `_synthetic_id()` mints a key from
                          hash(canton_sr), which can never equal a real
                          LexFind id. So a union on this key duplicates
                          EVERY law held by both sources.
  (canton, sr_number,
   language)            — the canton's own systematic number. Viable
                          only where the two sources agree on it.

Run read-only, after the pipeline has exited (invariant #9).

    python3 scripts/analyse_cantonal_union.py
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

DIRECT = Path("/mnt/HC_Volume_104655575/output/cantonal_laws_direct")
LEXFIND = Path("/opt/caselaw/repo/output/lexfind_cantonal")


def scan(path: Path) -> tuple[set, set, int]:
    """-> (lexfind_ids, (sr, language) keys, records with no lexfind_id)"""
    ids: set = set()
    srs: set = set()
    missing = 0
    if not path.exists():
        return ids, srs, missing
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            lid = r.get("lexfind_id")
            if lid is None:
                missing += 1
            else:
                ids.add(lid)
            sr = r.get("sr_number")
            if sr:
                srs.add((str(sr), r.get("language") or "de"))
    return ids, srs, missing


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--direct", type=Path, default=DIRECT)
    ap.add_argument("--lexfind", type=Path, default=LEXFIND)
    a = ap.parse_args()

    print(f"{'ct':<4}{'direct':>7}{'no_id':>7}{'lexfind':>8}"
          f"{'id_merge':>9}{'sr_merge':>9}{'union_sr':>9}  sr agreement")
    print("-" * 74)
    t_direct = t_lex = t_noid = t_idm = t_srm = t_union = 0
    broken = []
    for p in sorted(a.direct.glob("*.jsonl")):
        ct = p.stem.upper()
        d_ids, d_srs, d_missing = scan(p)
        l_ids, l_srs, _ = scan(a.lexfind / f"{ct}.jsonl")
        id_merge = len(d_ids & l_ids)
        sr_merge = len(d_srs & l_srs)
        union_sr = len(d_srs | l_srs)
        pct = 100.0 * sr_merge / len(d_srs) if d_srs else 0.0
        if d_srs and pct < 50.0:
            broken.append(ct)
        t_direct += len(d_srs); t_lex += len(l_srs); t_noid += d_missing
        t_idm += id_merge; t_srm += sr_merge; t_union += union_sr
        print(f"{ct:<4}{len(d_srs):>7}{d_missing:>7}{len(l_srs):>8}"
              f"{id_merge:>9}{sr_merge:>9}{union_sr:>9}  {pct:5.1f}%"
              f"{'  <-- INCOMPATIBLE' if pct < 50.0 else ''}")
    print("-" * 74)
    print(f"{'ALL':<4}{t_direct:>7}{t_noid:>7}{t_lex:>8}"
          f"{t_idm:>9}{t_srm:>9}{t_union:>9}"
          f"  {100.0 * t_srm / t_direct if t_direct else 0:5.1f}%")
    print()
    print(f"union on lexfind_id : {t_direct + t_lex} rows "
          f"({t_idm} merged) — every shared law stored twice")
    print(f"union on sr_number  : {t_union} rows "
          f"({t_srm} merged) — but {', '.join(broken) or 'no'} canton(s) "
          f"disagree on the numbering and would still duplicate")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Recover the real systematic numbers for the GE and TI law shards.

Both scrapers stored a placeholder where the canton's own number belongs:

  GE  sr_number='rsg_a1_01'  title='A 1 01 Acte d'union de la République...'
  TI  sr_number='1'          title='101.000  Costituzione della Repubblica...'

Geneva's RSG numbers are alphanumeric and `sil.py` only ever matched a
numeric pattern, so every GE law fell through to the source filename.
`ti.py` fell back to the row's position in the index. The consequence is
the same either way: 1,487 laws — about 9.5% of the cantonal corpus —
cannot be found by the number a practitioner would actually cite.
get_law(canton='TI', sr_number='101.000') returns nothing.

The number was never lost, only misfiled: it sits at the front of the
stored title. This rewrites the shards using the same parser the fixed
scrapers now use, so a re-scrape and a repair produce the same records.

Dry run by default — it reports the parse rate and refuses to guess:

    python3 scripts/repair_cantonal_numbers.py                 # report only
    python3 scripts/repair_cantonal_numbers.py --apply         # rewrite

Safeguards on --apply: the previous shard is kept as
`<CT>.jsonl.pre-numbering-<date>`, the record count must be preserved
exactly, and the extracted numbers must be unique within the canton —
sr_number is about to become a lookup key, and a parse that maps two
laws onto one number is worse than the placeholder it replaces.
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.cantonal_laws.numbering import (  # noqa: E402
    slug_matches_number, split_number_and_title)

log = logging.getLogger("repair_numbers")

DEFAULT_DIR = Path("/mnt/HC_Volume_104655575/output/cantonal_laws_direct")
CANTONS = ("GE", "TI")


def repair_rows(rows: list[dict], canton: str) -> tuple[list[dict], dict]:
    """-> (new rows, stats). Rows that do not parse are left untouched."""
    out, stats = [], Counter()
    agree = checked = 0
    for r in rows:
        old_sr = r.get("sr_number") or ""
        sr, title = split_number_and_title(r.get("title") or "",
                                           fallback=old_sr)
        if sr and sr != old_sr:
            # Geneva's slug encodes the same number, so agreement is
            # evidence the parse is right and not merely well-formed.
            if canton == "GE":
                checked += 1
                agree += bool(slug_matches_number(old_sr, sr))
            r = {**r, "sr_number": sr, "title": title}
            stats["repaired"] += 1
        else:
            stats["unchanged"] += 1
        out.append(r)
    if checked:
        stats["slug_checked"], stats["slug_agreed"] = checked, agree
    return out, stats


def _report(canton: str, rows: list[dict], new: list[dict], stats: dict) -> bool:
    srs = [r.get("sr_number") or "" for r in new]
    dupes = [s for s, n in Counter(srs).items() if n > 1 and s]
    log.info("%s: %d records | repaired %d | unchanged %d", canton, len(rows),
             stats.get("repaired", 0), stats.get("unchanged", 0))
    if "slug_checked" in stats:
        log.info("   slug cross-check: %d/%d agree",
                 stats["slug_agreed"], stats["slug_checked"])
    for before, after in list(zip(rows, new))[:3]:
        log.info("   %r -> %r  |  %.44s",
                 before.get("sr_number"), after.get("sr_number"),
                 after.get("title"))
    if dupes:
        log.error("   %d duplicate sr_number(s) after repair, e.g. %s",
                  len(dupes), dupes[:5])
        return False
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--dir", type=Path, default=DEFAULT_DIR)
    ap.add_argument("--cantons", default=",".join(CANTONS))
    ap.add_argument("--apply", action="store_true",
                    help="rewrite the shards (default: report only)")
    ap.add_argument("--stamp", default="", help="backup suffix, e.g. 20260819")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    ok = True
    for canton in [c.strip().upper() for c in a.cantons.split(",") if c.strip()]:
        path = a.dir / f"{canton}.jsonl"
        if not path.exists():
            log.error("%s: missing %s", canton, path)
            ok = False
            continue
        rows = []
        with path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        new, stats = repair_rows(rows, canton)
        if not _report(canton, rows, new, stats):
            ok = False
            continue
        if not a.apply:
            continue
        if len(new) != len(rows):
            log.error("%s: record count changed %d -> %d, refusing",
                      canton, len(rows), len(new))
            ok = False
            continue
        backup = path.with_suffix(f".jsonl.pre-numbering-{a.stamp or 'bak'}")
        shutil.copy2(path, backup)
        tmp = path.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for r in new:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        tmp.replace(path)
        log.info("   wrote %s (backup: %s)", path.name, backup.name)
    if not a.apply:
        log.info("\ndry run — nothing written. Re-run with --apply to rewrite.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

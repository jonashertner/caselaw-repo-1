#!/usr/bin/env python3
"""Seed the vd_gerichte uuid sidecar and dedupe the corpus shard by uuid.

Background (2026-09-04): prestations.vd.ch stopped returning affaireHit.numero,
so scrapers/cantonal/vd_gerichte.py minted new ids for decisions already held
under their ZD number — 8,133 duplicate records entered
output/decisions/vd_gerichte.jsonl in one night. The portal's stable identity
is the decision uuid, present as the last path segment of every record's
pdf_url. The scraper now keeps a sidecar state/vd_gerichte.uuids.txt
("<uuid>\\t<decision_id>") and skips any listing whose uuid is held.

Modes (the shard is streamed once; ~2 GB on the VPS, so run it OUTSIDE the
build window — after opencaselaw-publish and opencaselaw-publish-incremental
are both inactive, see CLAUDE.md invariant 9):

  --check           report duplicate-uuid groups (default, read-only)
  --seed            write the sidecar from the shard: FIRST id per uuid
  --dedupe          rewrite the shard keeping the FIRST record per uuid
                    (dry-run; add --apply to write). Backs the original up as
                    <shard>.bak-<UTC date> and lists dropped ids next to it.

"First" = earliest appended record, i.e. the id the corpus has served longest
(the ZD-scheme id, whose SEO URL and citation-graph node already exist). The
dropped ids stay in state/vd_gerichte.jsonl on purpose: with the sidecar seeded
the scraper never re-fetches them, and a lingering state id is harmless.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scrapers.cantonal.vd_gerichte import uuid_from_pdf_url  # noqa: E402

DEFAULT_SHARD = REPO / "output" / "decisions" / "vd_gerichte.jsonl"
DEFAULT_SIDECAR = REPO / "state" / "vd_gerichte.uuids.txt"


def iter_lines(path: Path):
    """Yield (raw_line, record_or_None). Never raises on a torn last line."""
    with open(path, "rb") as f:
        for raw in f:
            line = raw.rstrip(b"\r\n")
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:
                rec = None
            yield line, rec


def analyse(shard: Path):
    """One pass: first id per uuid, the duplicate lines, and counts."""
    first: dict[str, str] = {}
    dup_ids: list[tuple[str, str, str]] = []   # (dropped_id, kept_id, uuid)
    stats = Counter()
    for _line, rec in iter_lines(shard):
        stats["lines"] += 1
        if rec is None:
            stats["unparseable"] += 1
            continue
        did = rec.get("decision_id") or ""
        u = uuid_from_pdf_url(rec.get("pdf_url"))
        if not u:
            stats["no_uuid"] += 1
            continue
        if u in first:
            stats["duplicates"] += 1
            dup_ids.append((did, first[u], u))
        else:
            first[u] = did
    stats["unique_uuids"] = len(first)
    return first, dup_ids, stats


def write_atomic(path: Path, lines: list[str]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("".join(lines))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def seed(shard: Path, sidecar: Path, first: dict[str, str]) -> None:
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{u}\t{did}\n" for u, did in first.items()]
    write_atomic(sidecar, lines)
    print(f"seeded {sidecar}: {len(lines)} uuids")


def dedupe(shard: Path, apply: bool) -> Counter:
    """Keep the first record per uuid; records without a uuid are kept."""
    seen: set[str] = set()
    stats = Counter()
    kept: list[bytes] = []
    dropped: list[str] = []
    for line, rec in iter_lines(shard):
        stats["lines"] += 1
        u = uuid_from_pdf_url(rec.get("pdf_url")) if rec else None
        if u and u in seen:
            stats["dropped"] += 1
            dropped.append((rec or {}).get("decision_id") or "?")
            continue
        if u:
            seen.add(u)
        kept.append(line + b"\n")
        stats["kept"] += 1
    if not apply:
        print(f"dry-run: {stats['lines']} lines, keep {stats['kept']}, drop {stats['dropped']}")
        return stats
    stamp = time.strftime("%Y%m%d", time.gmtime())
    backup = shard.with_name(f"{shard.name}.bak-{stamp}")
    if backup.exists():
        raise SystemExit(f"refusing to overwrite existing backup {backup}")
    os.link(shard, backup)                      # zero-cost, same inode
    tmp = shard.with_name(shard.name + ".tmp")
    with open(tmp, "wb") as f:
        f.writelines(kept)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, shard)
    dropped_path = shard.with_name(f"{shard.name}.dropped-{stamp}.txt")
    dropped_path.write_text("".join(d + "\n" for d in dropped), encoding="utf-8")
    print(f"rewrote {shard}: kept {stats['kept']}, dropped {stats['dropped']}; "
          f"backup {backup}, dropped ids in {dropped_path}")
    return stats


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--shard", type=Path, default=DEFAULT_SHARD)
    ap.add_argument("--sidecar", type=Path, default=DEFAULT_SIDECAR)
    ap.add_argument("--check", action="store_true", help="report duplicate groups (default)")
    ap.add_argument("--seed", action="store_true", help="write the sidecar from the shard")
    ap.add_argument("--dedupe", action="store_true", help="rewrite the shard, first record per uuid")
    ap.add_argument("--apply", action="store_true", help="with --dedupe: actually write")
    args = ap.parse_args(argv)
    if not args.shard.exists():
        print(f"shard not found: {args.shard}", file=sys.stderr)
        return 2

    if args.dedupe:
        dedupe(args.shard, apply=args.apply)

    first, dup_ids, stats = analyse(args.shard)
    print(f"{args.shard}: {stats['lines']} lines, {stats['unique_uuids']} uuids, "
          f"{stats['duplicates']} duplicate records, {stats['no_uuid']} without uuid, "
          f"{stats['unparseable']} unparseable")
    if dup_ids and (args.check or not (args.seed or args.dedupe)):
        classes = Counter(
            "docket-dash" if " - " in d else "numeric" if d.rsplit("_", 1)[-1].isdigit() else "other"
            for d, _k, _u in dup_ids)
        print("duplicate id classes:", dict(classes))
        for d, k, u in dup_ids[:10]:
            print(f"  drop {d!r} (kept {k!r}, uuid {u})")

    if args.seed:
        seed(args.shard, args.sidecar, first)
    return 0


if __name__ == "__main__":
    sys.exit(main())

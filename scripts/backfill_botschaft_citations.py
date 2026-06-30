#!/usr/bin/env python3
"""Issue #30 backfill: correct bbl_page + bbl_citation in materialien.db.

The existing botschaft_documents rows carry the Fedlex `fga` ELI segment (an
internal sequence number) as bbl_page instead of the printed gazette page
(jolux:memorialPage). This rewrites every row's bbl_page to its memorialPage
and rebuilds bbl_citation with the per-language label (de -> "BBl",
fr/it -> "FF"). The ELI URI is untouched (it correctly uses the segment).

Metadata-only: no PDF re-download. Respects the immutable=1 serve contract by
working on a COPY and atomically swapping it in (os.replace).

Usage:
    python3 scripts/backfill_botschaft_citations.py --db output/materialien.db          # dry-run
    python3 scripts/backfill_botschaft_citations.py --db output/materialien.db --apply   # copy+UPDATE+swap
"""
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.fedlex_materialien import discover_fga_botschaften  # noqa: E402

# Per-language gazette label. German = Bundesblatt; French/Italian = Feuille
# fédérale / Foglio federale (cited "FF").
LABELS = {"de": "BBl", "fr": "FF", "it": "FF"}


def build_page_map(timeout: int = 120) -> dict[tuple[str, str], int]:
    """{(eli_uri, language): memorialPage} for every Fedlex Botschaft."""
    out: dict[tuple[str, str], int] = {}
    for lang in ("de", "fr", "it"):
        rows = discover_fga_botschaften(lang, timeout=timeout)
        for (_year, page, eli, _title) in rows:
            out[(eli, lang)] = page
    return out


def compute_updates(conn: sqlite3.Connection, page_map) -> list[tuple[int, int, str]]:
    """Return [(botschaft_id, new_page, new_citation), ...] for rows that change."""
    updates = []
    rows = conn.execute(
        "SELECT botschaft_id, bbl_year, bbl_page, bbl_citation, eli_uri, language "
        "FROM botschaft_documents"
    ).fetchall()
    for bid, year, page, citation, eli, lang in rows:
        mp = page_map.get((eli, lang))
        if mp is None:
            continue  # no memorialPage (post-2022): segment is the doc number, already correct
        new_cit = "%s %d %d" % (LABELS.get(lang, "BBl"), year, mp)
        if mp != page or new_cit != citation:
            updates.append((bid, mp, new_cit))
    return updates, len(rows)


def apply_updates(tmp_db: str, updates) -> None:
    """Two-pass UPDATE to avoid transient UNIQUE(bbl_year, bbl_page, language)
    collisions: park every changing row on a unique negative page first, then
    set the real memorialPage (final pages are unique per document)."""
    w = sqlite3.connect(tmp_db)
    try:
        w.execute("BEGIN")
        for bid, _mp, _cit in updates:
            w.execute(
                "UPDATE botschaft_documents SET bbl_page = -botschaft_id WHERE botschaft_id = ?",
                (bid,),
            )
        for bid, mp, cit in updates:
            w.execute(
                "UPDATE botschaft_documents SET bbl_page = ?, bbl_citation = ? WHERE botschaft_id = ?",
                (mp, cit, bid),
            )
        w.execute("COMMIT")
    finally:
        w.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--timeout", type=int, default=120)
    args = ap.parse_args()

    page_map = build_page_map(timeout=args.timeout)
    print("memorialPage map (eli,lang) entries:", len(page_map))

    ro = sqlite3.connect("file:%s?mode=ro&immutable=1" % args.db, uri=True)
    updates, total = compute_updates(ro, page_map)
    ro.close()
    print("docs total=%d  would_change=%d" % (total, len(updates)))
    for bid, mp, cit in updates[:8]:
        print("   id=%d -> %s" % (bid, cit))

    if not args.apply:
        print("(dry-run; pass --apply to copy+UPDATE+swap)")
        return 0

    if not updates:
        print("nothing to update")
        return 0

    tmp = args.db + ".backfill.tmp"
    if os.path.exists(tmp):
        os.remove(tmp)
    shutil.copy2(args.db, tmp)
    apply_updates(tmp, updates)
    # sanity: row count preserved
    chk = sqlite3.connect(tmp)
    n = chk.execute("SELECT count(*) FROM botschaft_documents").fetchone()[0]
    bad = chk.execute(
        "SELECT count(*) FROM botschaft_documents WHERE bbl_page < 0"
    ).fetchone()[0]
    chk.close()
    if n != total or bad:
        print("ABORT: count drift (%d vs %d) or %d negative pages left" % (n, total, bad))
        os.remove(tmp)
        return 2
    os.replace(tmp, args.db)
    print("SWAPPED: %d rows corrected (count %d preserved)" % (len(updates), n))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

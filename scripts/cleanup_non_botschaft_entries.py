"""Remove botschaft_documents rows that fail the v0.3 is_botschaft filter.

v0.2 ingested 3 false-positives before the typeDocument SPARQL filter
shipped:
  - BBl 2024 2485 (Bundesbeschluss Umweltfinanzierung, td=8)
  - BBl 2018 2827 (Trichlorethylen Mitteilung, no manifestation)
  - BBl 2017  399 (KESR/Vorsorgeauftrag, no DE manifestation)

Together these carry 504 garbage entries in article_botschaft_links
that route get_article_purpose to non-Botschaft text. Cleanup is a
3-row DELETE with ON DELETE CASCADE; safe to re-run.

Usage:
    python3 scripts/cleanup_non_botschaft_entries.py [--db PATH] [--dry-run]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from search_stack.build_botschaft_corpus import is_botschaft, bbl_eli_uri  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--db",
        default=str(REPO_ROOT / "output" / "materialien.db"),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys = ON")

    rows = conn.execute(
        "SELECT botschaft_id, bbl_year, bbl_page, language "
        "FROM botschaft_documents"
    ).fetchall()
    print(f"Auditing {len(rows)} ingested entries against is_botschaft…")
    targets = []
    for bid, y, p, lang in rows:
        eli = bbl_eli_uri(y, p)
        if not is_botschaft(eli, language=lang):
            npara = conn.execute(
                "SELECT COUNT(*) FROM botschaft_paragraphs WHERE botschaft_id=?",
                (bid,),
            ).fetchone()[0]
            nlink = conn.execute(
                "SELECT COUNT(*) FROM article_botschaft_links WHERE botschaft_id=?",
                (bid,),
            ).fetchone()[0]
            targets.append((bid, y, p, lang, npara, nlink))
            print(
                f"  flag: BBl {y} {p} ({lang}) "
                f"botschaft_id={bid} paragraphs={npara} links={nlink}"
            )

    if not targets:
        print("No cleanup targets — DB is consistent with v0.3 filter.")
        return 0

    if args.dry_run:
        print(f"\nDRY RUN — would delete {len(targets)} botschaft_documents rows")
        return 0

    print(f"\nDeleting {len(targets)} rows (cascade removes paragraphs + links)…")
    for bid, *_ in targets:
        conn.execute("DELETE FROM botschaft_documents WHERE botschaft_id=?", (bid,))
    conn.commit()

    after_doc = conn.execute("SELECT COUNT(*) FROM botschaft_documents").fetchone()[0]
    after_para = conn.execute("SELECT COUNT(*) FROM botschaft_paragraphs").fetchone()[0]
    after_link = conn.execute("SELECT COUNT(*) FROM article_botschaft_links").fetchone()[0]
    print(f"\nPost-cleanup: docs={after_doc} paragraphs={after_para} links={after_link}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

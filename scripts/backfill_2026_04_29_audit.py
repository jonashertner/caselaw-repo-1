"""
Backfill for the three findings in the 2026-04-29 corpus audit
(Adrian König).  Idempotent: running twice is a no-op after the first
successful run.  Logs counts at every step so the operator can verify
each transformation independently.

Findings:
  1. 474 EGMR decisions are duplicated under court='bge' (with cedh
     marker in source_url) AND court='bge_egmr' for the same docket.
     The two paths have complementary strengths: bge has longer full_text
     and content_hash; bge_egmr has title, chamber, regeste. Best-of-both
     merge into bge_egmr, then delete the bge duplicates.
  2. 692 GL + 2 BS rows have a relative source_url (/cgi-bin/...) without
     a host prefix. Prepend the verified host for each canton.
  3. 1 GL row uses http:// instead of https://. Upgrade.

The scraper-side fix lives in scrapers/entscheidsuche_ingest.py
(commit accompanying this script). After running this backfill once and
re-publishing, the next entscheidsuche ingest should produce no new
duplicates or relative URLs.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = Path(os.environ.get(
    "SWISS_CASELAW_DECISIONS_DB",
    "/opt/caselaw/repo/output/decisions.db",
))

GL_HOST = "https://findinfo.gl.ch"
BS_HOST = "https://rechtsprechung.gerichte.bs.ch"


def banner(msg: str) -> None:
    print(f"\n{'=' * 72}\n{msg}\n{'=' * 72}")


def step_egmr_merge(conn: sqlite3.Connection) -> dict:
    """Merge bge+cedh rows into their bge_egmr counterparts and delete the
    bge duplicates.  Best-of-both: keep bge_egmr's title/chamber/regeste,
    copy bge's longer full_text + content_hash + source."""
    c = conn.cursor()
    pre_bge_cedh = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE court='bge' AND source_url LIKE '%cedh%'"
    ).fetchone()[0]
    pre_bge_egmr = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE court='bge_egmr'"
    ).fetchone()[0]
    paired = c.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT docket_number FROM decisions
          WHERE court='bge' AND source_url LIKE '%cedh%'
          INTERSECT
          SELECT docket_number FROM decisions WHERE court='bge_egmr'
        )
        """
    ).fetchone()[0]
    print(f"  pre: bge+cedh={pre_bge_cedh}, bge_egmr={pre_bge_egmr}, "
          f"paired by docket_number={paired}")

    # Best-of-both merge: copy the longer full_text + content_hash + source
    # from the bge row into the bge_egmr row (only when the bge row's
    # full_text is at least as long, which the audit confirms is the case).
    rows_updated = c.execute(
        """
        UPDATE decisions AS b
           SET full_text = COALESCE(
                   (SELECT a.full_text FROM decisions a
                     WHERE a.court='bge' AND a.source_url LIKE '%cedh%'
                       AND a.docket_number = b.docket_number),
                   b.full_text),
               content_hash = COALESCE(
                   (SELECT a.content_hash FROM decisions a
                     WHERE a.court='bge' AND a.source_url LIKE '%cedh%'
                       AND a.docket_number = b.docket_number),
                   b.content_hash),
               source = COALESCE(
                   (SELECT a.source FROM decisions a
                     WHERE a.court='bge' AND a.source_url LIKE '%cedh%'
                       AND a.docket_number = b.docket_number),
                   b.source)
         WHERE b.court='bge_egmr'
           AND EXISTS (
               SELECT 1 FROM decisions a
                WHERE a.court='bge' AND a.source_url LIKE '%cedh%'
                  AND a.docket_number = b.docket_number)
        """
    ).rowcount

    # Delete the bge+cedh duplicates.
    rows_deleted = c.execute(
        "DELETE FROM decisions WHERE court='bge' AND source_url LIKE '%cedh%'"
    ).rowcount

    post_bge_cedh = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE court='bge' AND source_url LIKE '%cedh%'"
    ).fetchone()[0]
    post_bge_egmr = c.execute(
        "SELECT COUNT(*) FROM decisions WHERE court='bge_egmr'"
    ).fetchone()[0]
    print(f"  merge: updated {rows_updated} bge_egmr rows with bge fields")
    print(f"  delete: removed {rows_deleted} bge+cedh duplicates")
    print(f"  post: bge+cedh={post_bge_cedh}, bge_egmr={post_bge_egmr}")
    return {
        "pre_bge_cedh": pre_bge_cedh,
        "pre_bge_egmr": pre_bge_egmr,
        "paired": paired,
        "merged": rows_updated,
        "deleted": rows_deleted,
        "post_bge_cedh": post_bge_cedh,
        "post_bge_egmr": post_bge_egmr,
    }


def step_url_repair(conn: sqlite3.Connection) -> dict:
    """Prefix relative GL/BS source_urls with their host. Also upgrade the
    one known http:// GL row to https://."""
    c = conn.cursor()

    # Pre counts
    pre_gl_rel = c.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE court='gl_gerichte' AND source_url LIKE '/%'"
    ).fetchone()[0]
    pre_bs_rel = c.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE court='bs_gerichte' AND source_url LIKE '/%'"
    ).fetchone()[0]
    pre_gl_http = c.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE court='gl_gerichte' AND source_url LIKE 'http://%'"
    ).fetchone()[0]
    print(f"  pre: gl rel={pre_gl_rel}, bs rel={pre_bs_rel}, gl http://={pre_gl_http}")

    gl_rel = c.execute(
        "UPDATE decisions SET source_url = ? || source_url "
        "WHERE court='gl_gerichte' AND source_url LIKE '/%'",
        (GL_HOST,),
    ).rowcount
    bs_rel = c.execute(
        "UPDATE decisions SET source_url = ? || source_url "
        "WHERE court='bs_gerichte' AND source_url LIKE '/%'",
        (BS_HOST,),
    ).rowcount
    gl_http = c.execute(
        "UPDATE decisions SET source_url = 'https' || substr(source_url, 5) "
        "WHERE court='gl_gerichte' AND source_url LIKE 'http://%'"
    ).rowcount

    # Post counts
    post_gl_rel = c.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE court='gl_gerichte' AND source_url LIKE '/%'"
    ).fetchone()[0]
    post_bs_rel = c.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE court='bs_gerichte' AND source_url LIKE '/%'"
    ).fetchone()[0]
    post_gl_http = c.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE court='gl_gerichte' AND source_url LIKE 'http://%'"
    ).fetchone()[0]
    print(f"  prefix gl: {gl_rel} updated; prefix bs: {bs_rel}; "
          f"http→https: {gl_http}")
    print(f"  post: gl rel={post_gl_rel}, bs rel={post_bs_rel}, "
          f"gl http={post_gl_http}")
    return {
        "pre_gl_rel": pre_gl_rel, "pre_bs_rel": pre_bs_rel, "pre_gl_http": pre_gl_http,
        "gl_prefixed": gl_rel, "bs_prefixed": bs_rel, "http_upgraded": gl_http,
        "post_gl_rel": post_gl_rel, "post_bs_rel": post_bs_rel,
        "post_gl_http": post_gl_http,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB,
                        help=f"Path to decisions.db (default: {DEFAULT_DB})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print pre-counts without modifying the DB.")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"ERROR: DB not found at {args.db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(args.db))
    conn.execute("PRAGMA journal_mode=WAL")

    if args.dry_run:
        c = conn.cursor()
        banner("DRY RUN — no changes will be made")
        for label, sql in [
            ("EGMR bge+cedh duplicates",
             "SELECT COUNT(*) FROM decisions WHERE court='bge' AND source_url LIKE '%cedh%'"),
            ("EGMR bge_egmr rows",
             "SELECT COUNT(*) FROM decisions WHERE court='bge_egmr'"),
            ("GL relative-path source_urls",
             "SELECT COUNT(*) FROM decisions WHERE court='gl_gerichte' AND source_url LIKE '/%'"),
            ("BS relative-path source_urls",
             "SELECT COUNT(*) FROM decisions WHERE court='bs_gerichte' AND source_url LIKE '/%'"),
            ("GL http:// source_urls",
             "SELECT COUNT(*) FROM decisions WHERE court='gl_gerichte' AND source_url LIKE 'http://%'"),
        ]:
            n = c.execute(sql).fetchone()[0]
            print(f"  {label}: {n}")
        return 0

    banner("Step 1 — EGMR best-of-both merge + duplicate deletion")
    conn.execute("BEGIN")
    egmr_stats = step_egmr_merge(conn)

    banner("Step 2 — GL/BS host prefix + http→https upgrade")
    url_stats = step_url_repair(conn)

    print()
    print("running PRAGMA quick_check ... ", end="", flush=True)
    qc = conn.execute("PRAGMA quick_check").fetchone()[0]
    print(qc)
    if qc != "ok":
        print("ERROR: quick_check failed; rolling back", file=sys.stderr)
        conn.execute("ROLLBACK")
        return 3

    conn.execute("COMMIT")
    banner("DONE — backfill committed")
    print(f"egmr: {egmr_stats}")
    print(f"url:  {url_stats}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

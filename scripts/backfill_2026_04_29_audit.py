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

    # Performance: a previous version used correlated subqueries with
    # `source_url LIKE '%cedh%'` against the full 985k-row decisions table
    # for every target row, which fails to use any index and runs in
    # O(N×M). Build a small temp lookup table once (474 rows ≈ 25 MB),
    # index it on docket_number, and join through it instead.
    c.execute("DROP TABLE IF EXISTS _bge_cedh_lookup")
    c.execute(
        """
        CREATE TEMP TABLE _bge_cedh_lookup AS
        SELECT docket_number, full_text, content_hash, source, decision_id
          FROM decisions
         WHERE court='bge' AND source_url LIKE '%cedh%'
        """
    )
    c.execute(
        "CREATE INDEX _bge_cedh_lookup_idx ON _bge_cedh_lookup(docket_number)"
    )
    n_lookup = c.execute(
        "SELECT COUNT(*) FROM _bge_cedh_lookup"
    ).fetchone()[0]
    print(f"  built temp lookup of {n_lookup} bge+cedh rows")

    # Best-of-both merge via the temp table. Indexed docket lookups make
    # this O(N) in the size of bge_egmr × log(N_lookup).
    rows_updated = c.execute(
        """
        UPDATE decisions
           SET full_text = COALESCE(
                   (SELECT t.full_text FROM _bge_cedh_lookup t
                     WHERE t.docket_number = decisions.docket_number),
                   decisions.full_text),
               content_hash = COALESCE(
                   (SELECT t.content_hash FROM _bge_cedh_lookup t
                     WHERE t.docket_number = decisions.docket_number),
                   decisions.content_hash),
               source = COALESCE(
                   (SELECT t.source FROM _bge_cedh_lookup t
                     WHERE t.docket_number = decisions.docket_number),
                   decisions.source)
         WHERE decisions.court = 'bge_egmr'
           AND decisions.docket_number IN (
               SELECT docket_number FROM _bge_cedh_lookup)
        """
    ).rowcount

    # Delete the bge+cedh duplicates by their decision_id (precomputed in
    # the lookup table) so we avoid the repeated LIKE.
    rows_deleted = c.execute(
        """
        DELETE FROM decisions
         WHERE decision_id IN (SELECT decision_id FROM _bge_cedh_lookup)
        """
    ).rowcount

    c.execute("DROP TABLE _bge_cedh_lookup")

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

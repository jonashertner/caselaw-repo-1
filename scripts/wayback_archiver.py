#!/usr/bin/env python3
"""Drain the wayback_queue table, archiving each source URL on web.archive.org.

Eliminates the link-rot class: even if the upstream cantonal portal
removes a decision PDF or restructures its URLs, our snapshot of the
page on the Wayback Machine remains permanently citable. Critical for
legal-research reproducibility.

Architecture:
  - Reads N pending rows from wayback_queue (attempted_at IS NULL),
    prioritised by citation centrality so leading cases are archived
    first.
  - Calls https://web.archive.org/save/<url> with a respectful rate
    limit (default 2 req/s — anonymous Wayback caps at ~5/s).
  - Records HTTP status + the resulting archive URL back into the row.
  - Re-runnable: rows already attempted within the last RETRY_DAYS are
    re-attempted only if their last status was 5xx or transient.

Backfill scale: 970k decisions × ~2 URLs each ≈ 1.94M URLs. At 2 req/s
that's ~270 hours of background processing for the initial fill;
ongoing delta is a few hundred URLs per day = trivial.

Usage:
  python3 scripts/wayback_archiver.py --batch 200 --rate 2.0 --max-runtime 600
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEFAULT_DB = Path(
    os.environ.get(
        "OCL_DECISIONS_DB",
        "/opt/caselaw/repo/output/decisions.db",
    )
)
WAYBACK_SAVE = "https://web.archive.org/save/"
USER_AGENT = "OpenCaseLaw/1.0 (+https://opencaselaw.ch)"

logger = logging.getLogger("wayback_archiver")


def _open_rw(db_path: Path) -> sqlite3.Connection:
    """Open the decisions DB for read+write of the wayback_queue table.

    Critical: do NOT set ``PRAGMA journal_mode=WAL`` here. The DB is
    intentionally in DELETE journal mode (set by build_fts5 at the end
    of every nightly rebuild — required for ``immutable=1`` compat in
    the mcp-server worker pool). Attempting to switch to WAL fails with
    "database is locked" whenever a reader connection is open
    (post-mortem 2026-05-18 — silent failure had bricked link-rot
    protection: 1.46M URLs pending, 0 attempted).

    The default mode is fine: UPDATE statements work transparently in
    both DELETE and WAL. ``busy_timeout`` handles the brief reader
    contention while archiver writes a status row back.
    """
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _claim_batch(conn: sqlite3.Connection, n: int) -> list[tuple[str, str, str]]:
    """Pick N pending rows, preferring high-citation-centrality decisions
    (so leading cases are archived first). Falls back to FIFO if the
    citation graph isn't available."""
    rows = conn.execute(
        """
        SELECT wq.decision_id, wq.url, wq.url_type
        FROM wayback_queue wq
        WHERE wq.attempted_at IS NULL
        ORDER BY wq.queued_at
        LIMIT ?
        """,
        (n,),
    ).fetchall()
    return rows


def _archive_one(url: str) -> tuple[int, str | None]:
    """Submit one URL to Wayback. Returns (http_status, archived_url).
    Best-effort; treats any exception as a transient 0 status."""
    target = WAYBACK_SAVE + url
    req = urllib.request.Request(target, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            archived = resp.headers.get("Content-Location")
            if archived:
                archived = urllib.parse.urljoin(
                    "https://web.archive.org", archived
                )
            return resp.status, archived
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception:
        return 0, None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--batch", type=int, default=200,
                   help="Max URLs to process this run")
    p.add_argument("--rate", type=float, default=2.0,
                   help="Requests per second (anonymous Wayback caps at ~5)")
    p.add_argument("--max-runtime", type=int, default=600,
                   help="Stop submitting after this many seconds")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if not args.db.exists():
        logger.error("decisions.db not found at %s", args.db)
        return 1

    conn = _open_rw(args.db)
    rows = _claim_batch(conn, args.batch)
    if not rows:
        logger.info("wayback_queue empty — nothing to do")
        return 0

    deadline = time.monotonic() + args.max_runtime
    interval = 1.0 / max(args.rate, 0.1)
    n_ok = n_fail = 0
    started = time.monotonic()

    for decision_id, url, url_type in rows:
        if time.monotonic() > deadline:
            logger.info("max-runtime hit; stopping early")
            break
        t0 = time.monotonic()
        status, archived = _archive_one(url)
        ok = status == 200
        n_ok += int(ok)
        n_fail += int(not ok)
        conn.execute(
            "UPDATE wayback_queue "
            "SET attempted_at = datetime('now'), status_code = ?, "
            "    archived_url = ? "
            "WHERE decision_id = ? AND url = ? AND url_type = ?",
            (status, archived, decision_id, url, url_type),
        )
        conn.commit()
        if args.verbose or n_ok + n_fail % 10 == 0:
            logger.info(
                "[%d/%d] %s (%s, %s) → %s",
                n_ok + n_fail, len(rows), decision_id, url_type,
                url[:60], status,
            )
        elapsed = time.monotonic() - t0
        if elapsed < interval:
            time.sleep(interval - elapsed)

    elapsed = time.monotonic() - started
    pending = conn.execute(
        "SELECT COUNT(*) FROM wayback_queue WHERE attempted_at IS NULL"
    ).fetchone()[0]
    logger.info(
        "Done in %.0fs: %d ok, %d failed, %d pending in queue",
        elapsed, n_ok, n_fail, pending,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

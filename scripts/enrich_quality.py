#!/usr/bin/env python3
"""
enrich_quality.py — Post-FTS5 data quality enrichment
======================================================

Runs against the FTS5 SQLite database to fill metadata gaps:
  1. Title backfill   — extract from full_text (Gegenstand/Objet/Oggetto)
  2. Regeste backfill  — extract from full_text (Regeste/Regesto header)
  3. Date repair       — fix NULL/invalid dates from docket or full_text
  4. Content hash      — compute MD5(full_text) for dedup
  5. Dedup report      — flag duplicates (metadata + content hash)

Usage:
    python3 scripts/enrich_quality.py --db output/decisions.db
    python3 scripts/enrich_quality.py --db output/decisions.db --dry-run -v
    python3 scripts/enrich_quality.py --db output/decisions.db --skip-hashes --skip-dedup

Pipeline (publish.py step 2d):
    python3 scripts/enrich_quality.py --db output/decisions.db
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("enrich_quality")

BATCH_SIZE = 1000
STREAM_LIMIT = 10_000  # rows per SELECT for streaming content hash

# ─── Title extraction patterns (from scrapers/bger.py:1042-1045) ─────────────

TITLE_PATTERNS = [
    # Primary: "Gegenstand:" followed by content until double newline or section start
    re.compile(
        r"(?:Gegenstand|Objet(?:\s+du\s+recours)?|Oggetto)\s*:?\s*\n?\s*(.*?)"
        r"(?:\n\s*\n|Beschwerde|Recours|Ricorso|Sachverhalt|Faits|Fatti)",
        re.DOTALL | re.IGNORECASE,
    ),
    # Fallback: single-line after label
    re.compile(
        r"(?:Gegenstand|Objet(?:\s+du\s+recours)?|Oggetto)\s*:?\s*(.*?)(?:\n|;)",
        re.IGNORECASE,
    ),
]


def extract_title(full_text: str) -> str | None:
    """Extract title from first 2000 chars of full_text."""
    snippet = full_text[:2000]
    for pat in TITLE_PATTERNS:
        m = pat.search(snippet)
        if m:
            title = re.sub(r"\s+", " ", m.group(1).strip())
            if 10 < len(title) < 500:
                return title
    return None


# ─── Regeste extraction pattern (from scrapers/bger.py:1064-1068) ────────────

REGESTE_RE = re.compile(
    r"(?:Regeste|Regesto)\s*(?:\([^)]*\))?\s*:?\s*\n"
    r"(.*?)"
    r"(?:\nSachverhalt|\nFaits|\nFatti|\nAus den Erwägungen"
    r"|\nConsidérant en droit|\nExtrait|\nA\.\s)",
    re.DOTALL,
)


def extract_regeste(full_text: str) -> str | None:
    """Extract regeste from first 5000 chars of full_text."""
    snippet = full_text[:5000]
    m = REGESTE_RE.search(snippet)
    if m:
        regeste = m.group(1).strip()
        if 50 < len(regeste) < 3000:
            return regeste
    return None


# ─── Date extraction ─────────────────────────────────────────────────────────

# Year from docket: 6B_1234/2025, 1C_372/2024, A-1234/2020, B.2005.00123
DOCKET_YEAR_RE = re.compile(r"\b(\d{4})\b")

# Swiss date in text: 12. März 2024, 3 janvier 2023, 15.03.2024
SWISS_DATE_RE = re.compile(
    r"\b(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})\b"
)

# ISO date: 2024-03-15
ISO_DATE_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")

INVALID_DATES = {None, "", "None", "0000-00-00"}


def is_invalid_date(d: str | None) -> bool:
    return d in INVALID_DATES


def extract_date_from_docket(docket: str | None) -> str | None:
    """Extract year from docket number, return YYYY-01-01.

    Uses the last 4-digit number that looks like a valid year,
    since docket patterns like '6B_1234/2025' have the year at the end.
    """
    if not docket:
        return None
    candidates = DOCKET_YEAR_RE.findall(docket)
    for year_str in reversed(candidates):
        year = int(year_str)
        if 1800 < year < 2100:
            return f"{year}-01-01"
    return None


def extract_date_from_text(full_text: str) -> str | None:
    """Extract first valid date from first 500 chars."""
    snippet = full_text[:500]
    # Try Swiss DD.MM.YYYY
    m = SWISS_DATE_RE.search(snippet)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1800 < year < 2100 and 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year}-{month:02d}-{day:02d}"
    # Try ISO YYYY-MM-DD
    m = ISO_DATE_RE.search(snippet)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1800 < year < 2100 and 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year}-{month:02d}-{day:02d}"
    return None


# ─── Substep runners ─────────────────────────────────────────────────────────

def enrich_titles(conn: sqlite3.Connection, dry_run: bool, min_rowid: int = 0) -> dict:
    """Substep 1: Backfill missing titles from full_text."""
    logger.info("─── Substep 1: Title backfill ───")

    cur = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE (title IS NULL OR title = '') AND full_text IS NOT NULL AND rowid > ?",
        (min_rowid,),
    )
    total = cur.fetchone()[0]
    logger.info(f"  Candidates: {total}")

    if dry_run or total == 0:
        return {"candidates": total, "filled": 0}

    filled = 0
    last_rowid = min_rowid
    processed = 0
    while True:
        rows = conn.execute(
            "SELECT rowid, decision_id, full_text FROM decisions "
            "WHERE (title IS NULL OR title = '') AND full_text IS NOT NULL "
            "AND rowid > ? ORDER BY rowid LIMIT ?",
            (last_rowid, BATCH_SIZE),
        ).fetchall()
        if not rows:
            break

        last_rowid = rows[-1][0]

        updates = []
        for _rowid, did, ft in rows:
            title = extract_title(ft)
            if title:
                updates.append((title, did))

        if updates:
            conn.executemany(
                "UPDATE decisions SET title = ? WHERE decision_id = ?", updates
            )
            conn.commit()
            filled += len(updates)

        processed += len(rows)
        if processed % 10_000 == 0:
            logger.info(f"  Progress: {processed}/{total}, filled {filled}")

    logger.info(f"  Filled {filled} / {total} titles")
    return {"candidates": total, "filled": filled}


def enrich_regeste(conn: sqlite3.Connection, dry_run: bool, min_rowid: int = 0) -> dict:
    """Substep 2: Backfill missing regeste from full_text."""
    logger.info("─── Substep 2: Regeste backfill ───")

    cur = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE (regeste IS NULL OR regeste = '') AND full_text IS NOT NULL AND rowid > ?",
        (min_rowid,),
    )
    total = cur.fetchone()[0]
    logger.info(f"  Candidates: {total}")

    if dry_run or total == 0:
        return {"candidates": total, "filled": 0}

    filled = 0
    last_rowid = min_rowid
    processed = 0
    while True:
        rows = conn.execute(
            "SELECT rowid, decision_id, full_text FROM decisions "
            "WHERE (regeste IS NULL OR regeste = '') AND full_text IS NOT NULL "
            "AND rowid > ? ORDER BY rowid LIMIT ?",
            (last_rowid, BATCH_SIZE),
        ).fetchall()
        if not rows:
            break

        last_rowid = rows[-1][0]

        updates = []
        for _rowid, did, ft in rows:
            regeste = extract_regeste(ft)
            if regeste:
                updates.append((regeste, did))

        if updates:
            conn.executemany(
                "UPDATE decisions SET regeste = ? WHERE decision_id = ?", updates
            )
            conn.commit()
            filled += len(updates)

        processed += len(rows)
        if processed % 10_000 == 0:
            logger.info(f"  Progress: {processed}/{total}, filled {filled}")

    logger.info(f"  Filled {filled} / {total} regeste")
    return {"candidates": total, "filled": filled}


def repair_dates(conn: sqlite3.Connection, dry_run: bool, min_rowid: int = 0) -> dict:
    """Substep 3: Repair invalid/missing dates."""
    logger.info("─── Substep 3: Date repair ───")

    cur = conn.execute(
        "SELECT COUNT(*) FROM decisions "
        "WHERE (decision_date IS NULL OR decision_date = '' "
        "OR decision_date = 'None' OR decision_date = '0000-00-00') "
        "AND rowid > ?",
        (min_rowid,),
    )
    total = cur.fetchone()[0]
    logger.info(f"  Candidates: {total}")

    if dry_run or total == 0:
        return {"candidates": total, "from_docket": 0, "from_text": 0}

    from_docket = 0
    from_text = 0
    last_rowid = min_rowid

    while True:
        rows = conn.execute(
            "SELECT rowid, decision_id, docket_number, full_text FROM decisions "
            "WHERE (decision_date IS NULL OR decision_date = '' "
            "OR decision_date = 'None' OR decision_date = '0000-00-00') "
            "AND rowid > ? "
            "ORDER BY rowid LIMIT ?",
            (last_rowid, BATCH_SIZE),
        ).fetchall()
        if not rows:
            break

        last_rowid = rows[-1][0]

        updates = []
        for _rowid, did, docket, ft in rows:
            # Try docket first (more reliable)
            date = extract_date_from_docket(docket)
            if date:
                updates.append((date, did))
                from_docket += 1
                continue
            # Fallback to text
            if ft:
                date = extract_date_from_text(ft)
                if date:
                    updates.append((date, did))
                    from_text += 1

        if updates:
            conn.executemany(
                "UPDATE decisions SET decision_date = ? WHERE decision_id = ?", updates
            )
            conn.commit()

    logger.info(f"  Fixed {from_docket + from_text} / {total} dates (docket: {from_docket}, text: {from_text})")
    return {"candidates": total, "from_docket": from_docket, "from_text": from_text}


def _ensure_content_hash_column(conn: sqlite3.Connection) -> None:
    """Add content_hash column if it doesn't exist (older DBs lack it)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(decisions)").fetchall()}
    if "content_hash" not in cols:
        logger.info("  Adding missing content_hash column to decisions table")
        conn.execute("ALTER TABLE decisions ADD COLUMN content_hash TEXT")
        conn.commit()


def compute_content_hashes(conn: sqlite3.Connection, dry_run: bool, min_rowid: int = 0) -> dict:
    """Substep 4: Compute MD5(full_text) for rows missing content_hash."""
    logger.info("─── Substep 4: Content hash ───")
    _ensure_content_hash_column(conn)

    cur = conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE content_hash IS NULL AND full_text IS NOT NULL AND rowid > ?",
        (min_rowid,),
    )
    total = cur.fetchone()[0]
    logger.info(f"  Candidates: {total}")

    if dry_run or total == 0:
        return {"candidates": total, "computed": 0}

    computed = 0
    offset = 0

    while True:
        # Use rowid ordering for stable streaming (no OFFSET drift from UPDATEs)
        rows = conn.execute(
            "SELECT decision_id, full_text FROM decisions "
            "WHERE content_hash IS NULL AND full_text IS NOT NULL AND rowid > ? "
            "LIMIT ?",
            (min_rowid, STREAM_LIMIT),
        ).fetchall()
        if not rows:
            break

        updates = []
        for did, ft in rows:
            h = hashlib.md5(ft.encode("utf-8")).hexdigest()
            updates.append((h, did))

        conn.executemany(
            "UPDATE decisions SET content_hash = ? WHERE decision_id = ?", updates
        )
        conn.commit()
        computed += len(updates)
        offset += len(rows)

        if computed % 50_000 == 0:
            logger.info(f"  Hashed {computed} / {total}")

    logger.info(f"  Computed {computed} / {total} content hashes")
    return {"candidates": total, "computed": computed}


def generate_dedup_report(conn: sqlite3.Connection, output_path: Path, dry_run: bool) -> dict:
    """Substep 5: Find duplicates and write report."""
    logger.info("─── Substep 5: Dedup report ───")
    _ensure_content_hash_column(conn)

    # Metadata duplicates: same (court, docket_number, decision_date)
    meta_dupes = conn.execute(
        "SELECT court, docket_number, decision_date, GROUP_CONCAT(decision_id, '|'), COUNT(*) as cnt "
        "FROM decisions "
        "WHERE docket_number IS NOT NULL AND docket_number != '' "
        "AND decision_date IS NOT NULL AND decision_date != '' "
        "GROUP BY court, docket_number, decision_date "
        "HAVING cnt > 1 "
        "ORDER BY cnt DESC "
        "LIMIT 5000"
    ).fetchall()

    meta_groups = []
    for court, docket, date, ids_str, cnt in meta_dupes:
        meta_groups.append({
            "court": court,
            "docket_number": docket,
            "decision_date": date,
            "decision_ids": ids_str.split("|"),
            "count": cnt,
        })

    # Content hash duplicates
    hash_dupes = conn.execute(
        "SELECT content_hash, GROUP_CONCAT(decision_id, '|'), COUNT(*) as cnt "
        "FROM decisions "
        "WHERE content_hash IS NOT NULL "
        "GROUP BY content_hash "
        "HAVING cnt > 1 "
        "ORDER BY cnt DESC "
        "LIMIT 5000"
    ).fetchall()

    hash_groups = []
    for h, ids_str, cnt in hash_dupes:
        hash_groups.append({
            "content_hash": h,
            "decision_ids": ids_str.split("|"),
            "count": cnt,
        })

    # Summary by court
    court_meta_summary = {}
    for g in meta_groups:
        c = g["court"]
        court_meta_summary[c] = court_meta_summary.get(c, 0) + g["count"] - 1

    report = {
        "metadata_duplicate_groups": len(meta_groups),
        "metadata_duplicate_decisions": sum(g["count"] - 1 for g in meta_groups),
        "content_hash_duplicate_groups": len(hash_groups),
        "content_hash_duplicate_decisions": sum(g["count"] - 1 for g in hash_groups),
        "court_meta_duplicates": court_meta_summary,
        "metadata_groups": meta_groups[:200],  # top 200 for inspection
        "hash_groups": hash_groups[:200],
    }

    logger.info(
        f"  Metadata dupes: {report['metadata_duplicate_groups']} groups "
        f"({report['metadata_duplicate_decisions']} extra decisions)"
    )
    logger.info(
        f"  Content hash dupes: {report['content_hash_duplicate_groups']} groups "
        f"({report['content_hash_duplicate_decisions']} extra decisions)"
    )

    if not dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info(f"  Wrote {output_path}")

    return report


# ─── Main ────────────────────────────────────────────────────────────────────

def _load_checkpoint(db_path: Path) -> dict | None:
    """Load checkpoint from .enrich_checkpoint.json next to the DB."""
    cp_path = db_path.parent / ".enrich_checkpoint.json"
    if cp_path.exists():
        try:
            with open(cp_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"  Could not load checkpoint: {e}")
    return None


def _save_checkpoint(db_path: Path, max_rowid: int, decision_count: int) -> None:
    """Save checkpoint after successful enrichment run."""
    cp_path = db_path.parent / ".enrich_checkpoint.json"
    data = {
        "max_rowid": max_rowid,
        "decision_count": decision_count,
        "last_run": datetime.now(timezone.utc).isoformat(),
    }
    with open(cp_path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"  Saved checkpoint: {cp_path} (rowid={max_rowid}, count={decision_count})")


def run(
    db_path: Path,
    output_dir: Path,
    dry_run: bool = False,
    skip_titles: bool = False,
    skip_regeste: bool = False,
    skip_dates: bool = False,
    skip_hashes: bool = False,
    skip_dedup: bool = False,
) -> dict:
    """Run all enrichment substeps. Returns summary dict."""
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    # ── Checkpoint: skip if no new decisions ──
    row = conn.execute("SELECT COUNT(*), MAX(rowid) FROM decisions").fetchone()
    current_count, current_max_rowid = row[0], row[1] or 0

    checkpoint = _load_checkpoint(db_path)
    min_rowid = 0
    if checkpoint:
        prev_count = checkpoint.get("decision_count", 0)
        prev_max_rowid = checkpoint.get("max_rowid", 0)
        if current_count == prev_count and current_max_rowid == prev_max_rowid:
            logger.info(
                f"No new decisions since last enrichment "
                f"(count={current_count}, max_rowid={current_max_rowid}). Skipping."
            )
            conn.close()
            return {"skipped": True, "reason": "no_new_decisions", "decision_count": current_count}
        min_rowid = prev_max_rowid
        logger.info(
            f"Checkpoint found: {prev_count} decisions (max_rowid={prev_max_rowid}). "
            f"New: {current_count - prev_count} decisions, processing rowid > {min_rowid}"
        )
    else:
        logger.info(f"No checkpoint found — full enrichment run ({current_count} decisions)")

    summary = {}
    t0 = time.time()

    if not skip_titles:
        summary["titles"] = enrich_titles(conn, dry_run, min_rowid)

    if not skip_regeste:
        summary["regeste"] = enrich_regeste(conn, dry_run, min_rowid)

    if not skip_dates:
        summary["dates"] = repair_dates(conn, dry_run, min_rowid)

    if not skip_hashes:
        summary["hashes"] = compute_content_hashes(conn, dry_run, min_rowid)

    if not skip_dedup:
        summary["dedup"] = generate_dedup_report(
            conn, output_dir / "dedup_report.json", dry_run
        )

    conn.close()

    # ── Save checkpoint (only if not dry-run) ──
    if not dry_run:
        _save_checkpoint(db_path, current_max_rowid, current_count)

    elapsed = time.time() - t0
    summary["elapsed_seconds"] = round(elapsed, 1)
    logger.info(f"═══ Enrichment complete in {elapsed:.1f}s ═══")
    logger.info(json.dumps({k: v for k, v in summary.items() if k != "dedup"}, indent=2))

    return summary


def main():
    parser = argparse.ArgumentParser(description="Post-FTS5 data quality enrichment")
    parser.add_argument("--db", type=str, default="output/decisions.db", help="FTS5 database path")
    parser.add_argument("--output", type=str, default="output", help="Output directory for reports")
    parser.add_argument("--dry-run", action="store_true", help="Report counts without modifying DB")
    parser.add_argument("--skip-titles", action="store_true")
    parser.add_argument("--skip-regeste", action="store_true")
    parser.add_argument("--skip-dates", action="store_true")
    parser.add_argument("--skip-hashes", action="store_true")
    parser.add_argument("--skip-dedup", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    run(
        db_path=Path(args.db),
        output_dir=Path(args.output),
        dry_run=args.dry_run,
        skip_titles=args.skip_titles,
        skip_regeste=args.skip_regeste,
        skip_dates=args.skip_dates,
        skip_hashes=args.skip_hashes,
        skip_dedup=args.skip_dedup,
    )


if __name__ == "__main__":
    main()

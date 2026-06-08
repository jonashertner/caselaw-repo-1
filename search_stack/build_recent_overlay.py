#!/usr/bin/env python3
"""Recent-publication overlay for get_decision (closes the ~24h publish-lag).

The BGer Neuheiten poller captures recently-published rulings into bger.jsonl
with a `publication_date` stamp, but they aren't *served* until the next
nightly rebuild ingests them — so a decision published today (with a decision
date weeks earlier) is missing from the corpus for up to a publish cycle. This
builds a small immutable overlay of those recent publications so get_decision
can serve them on a corpus miss within the poll interval. See the 2026-06-08
recency investigation.

Run after each poller cycle (bger-poller.service ExecStartPost). Pure-local —
no live fetch. Output: output/recent_overlay.db (atomic swap).
"""
import argparse
import json
import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path

INPUT = Path(os.environ.get("OCL_BGER_JSONL", "output/decisions/bger.jsonl"))
OUTPUT = Path(os.environ.get("SWISS_CASELAW_RECENT_OVERLAY_DB", "output/recent_overlay.db"))

# Columns get_decision renders — must mirror get_decision_by_id's row shape so a
# fresh overlay row flows through the existing handler unchanged.
_COLS = [
    "decision_id", "docket_number", "court", "decision_date", "publication_date",
    "language", "title", "regeste", "full_text", "source_url", "pdf_url",
    "chamber", "collection", "bge_reference", "cited_decisions",
]


def _norm(v):
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def build_overlay(input_jsonl: Path, output_db: Path, days: int = 14,
                  today: date | None = None) -> int:
    """Build recent_overlay.db from bger.jsonl rows whose publication_date is in
    the last `days` days. Returns the number of decisions written."""
    today = today or date.today()
    cutoff = (today - timedelta(days=days)).isoformat()
    today_s = today.isoformat()
    output_db.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(output_db) + ".tmp")
    tmp.unlink(missing_ok=True)
    conn = sqlite3.connect(str(tmp))
    conn.execute("CREATE TABLE recent_decisions (" + ", ".join(f"{c} TEXT" for c in _COLS) + ")")
    conn.execute("CREATE INDEX idx_ro_id ON recent_decisions(decision_id)")
    conn.execute("CREATE INDEX idx_ro_docket ON recent_decisions(docket_number)")

    n = 0
    seen: set[str] = set()
    if input_jsonl.exists():
        with open(input_jsonl, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                pub = (o.get("publication_date") or "")[:10]
                if not pub or pub < cutoff or pub > today_s:
                    continue
                did = o.get("decision_id")
                if not did or did in seen:
                    continue
                seen.add(did)
                conn.execute(
                    "INSERT INTO recent_decisions (" + ",".join(_COLS) + ") VALUES ("
                    + ",".join("?" * len(_COLS)) + ")",
                    tuple(_norm(o.get(c)) for c in _COLS),
                )
                n += 1
    conn.commit()
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.close()
    os.replace(tmp, output_db)  # atomic — no window where a served reader sees no file
    return n


def lookup_overlay(conn: sqlite3.Connection, docket_or_id: str):
    """Return the overlay row for a decision_id or docket_number, or None."""
    return conn.execute(
        "SELECT * FROM recent_decisions WHERE decision_id = ? OR docket_number = ? LIMIT 1",
        (docket_or_id, docket_or_id),
    ).fetchone()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(INPUT))
    ap.add_argument("--output", default=str(OUTPUT))
    ap.add_argument("--days", type=int, default=14)
    args = ap.parse_args()
    n = build_overlay(Path(args.input), Path(args.output), days=args.days)
    print(f"recent_overlay.db built: {n} recent-publication decisions (last {args.days}d)")


if __name__ == "__main__":
    main()

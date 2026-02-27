# study/resolve_decision_ids.py
"""Map BGE refs in curriculum JSON files to actual decision_ids in the FTS5 DB.

Usage:
    python -m study.resolve_decision_ids [--db PATH] [--dry-run]

Walks all curriculum JSON files, queries the FTS5 DB for each blank decision_id,
writes resolved IDs back in place. Non-destructive: never overwrites existing IDs.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Any

CURRICULUM_DIR = Path(__file__).resolve().parent / "curriculum"
DEFAULT_DB = Path(__file__).resolve().parent.parent / "output" / "swiss_caselaw_fts5.db"

_BGE_PATTERN = re.compile(
    r"BGE\s+(\d+)\s+(Ia|Ib|I{1,3}V?|VI?|IV)\s+(\d+)", re.IGNORECASE
)


def parse_bge_ref(bge_ref: str) -> dict[str, str] | None:
    """Parse 'BGE 135 III 1' → {'volume': '135', 'collection': 'III', 'page': '1'}."""
    m = _BGE_PATTERN.search(bge_ref or "")
    if not m:
        return None
    return {"volume": m.group(1), "collection": m.group(2).upper(), "page": m.group(3)}


def build_fts_query(bge_ref: str) -> str:
    """Build an FTS5 phrase query string for a BGE ref.

    Utility for callers that query the FTS5 virtual table directly.
    Note: the internal resolver uses a LIKE query on docket_number instead.
    """
    parts = parse_bge_ref(bge_ref)
    if not parts:
        return ""
    return f'"{parts["volume"]} {parts["collection"]} {parts["page"]}"'


def _query_db(db_path: str, bge_ref: str) -> list[dict[str, Any]]:
    """Query FTS5 DB for a BGE ref. Returns list of matching rows."""
    parts = parse_bge_ref(bge_ref)
    if not parts:
        return []
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        try:
            cur = con.cursor()
            # Search docket_number for pattern like "135 III 1"
            pattern = f"%{parts['volume']} {parts['collection']} {parts['page']}%"
            cur.execute(
                "SELECT decision_id, docket_number FROM decisions "
                "WHERE court = 'bge' AND docket_number LIKE ? LIMIT 3",
                (pattern,),
            )
            rows = [dict(r) for r in cur.fetchall()]
        finally:
            con.close()
        return rows
    except sqlite3.OperationalError:
        return []


def resolve_all(
    *,
    curriculum_dir: str | None = None,
    db_path: str | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Resolve blank decision_ids in all curriculum JSON files.

    Returns stats dict with keys: resolved, not_found, already_set, errors.
    """
    cdir = Path(curriculum_dir) if curriculum_dir else CURRICULUM_DIR
    dbp = db_path if db_path is not None else str(DEFAULT_DB)
    stats: dict[str, int] = {
        "resolved": 0, "not_found": 0, "already_set": 0, "errors": 0,
    }

    for json_path in sorted(cdir.glob("*.json")):
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"  ERROR reading {json_path.name}: {e}")
            stats["errors"] += 1
            continue

        changed = False
        for mod in data.get("modules", []):
            for case in mod.get("cases", []):
                if case.get("decision_id"):
                    stats["already_set"] += 1
                    continue
                bge_ref = case.get("bge_ref", "")
                if not bge_ref or bge_ref.startswith("RESOLVE_"):
                    stats["not_found"] += 1
                    print(f"  SKIP (placeholder): {bge_ref}")
                    continue
                rows = _query_db(dbp, bge_ref)
                if rows:
                    did = rows[0]["decision_id"]
                    print(f"  RESOLVED: {bge_ref} → {did}")
                    if not dry_run:
                        case["decision_id"] = did
                        changed = True
                    stats["resolved"] += 1
                else:
                    print(f"  NOT FOUND: {bge_ref}")
                    stats["not_found"] += 1

        if changed and not dry_run:
            json_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve BGE refs to decision_ids")
    parser.add_argument("--db", default=None, help="Path to FTS5 DB")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    stats = resolve_all(db_path=args.db, dry_run=args.dry_run)
    print(f"\nSummary: {stats}")


if __name__ == "__main__":
    main()

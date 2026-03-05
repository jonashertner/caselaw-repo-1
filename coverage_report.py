#!/usr/bin/env python3
"""
coverage_report.py — coverage target + snapshot tracking and gap reporting.

This module adds two persistent models in SQLite:
- coverage_targets: registry of source-level coverage goals
- source_snapshots: expected decision IDs per source/year snapshot

Use `gap-report` to compute missing IDs and counts against ingested decisions.
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

from db_schema import COVERAGE_SCHEMA_SQL

logger = logging.getLogger("coverage_report")


def ensure_coverage_tables(conn: sqlite3.Connection) -> None:
    """Create coverage tables if missing."""
    conn.executescript(COVERAGE_SCHEMA_SQL)


def _infer_source_kind(module_name: str) -> str:
    if ".cantonal." in module_name:
        return "cantonal_court"
    if module_name.startswith("scrapers."):
        return "federal_or_authority"
    return "unknown"


def seed_targets_from_scrapers(conn: sqlite3.Connection, *, only_missing: bool = False) -> tuple[int, int]:
    """Upsert coverage targets from the scraper registry."""
    # Local import avoids a module-import cycle when run_scraper imports
    # coverage helpers for automatic snapshot recording.
    from run_scraper import SCRAPERS

    inserted = 0
    updated = 0
    for source_key, (module_name, class_name) in sorted(SCRAPERS.items()):
        row = conn.execute(
            "SELECT source_key FROM coverage_targets WHERE source_key = ?",
            (source_key,),
        ).fetchone()
        source_name = source_key.replace("_", " ")
        source_kind = _infer_source_kind(module_name)
        notes = f"{module_name}:{class_name}"

        if row:
            if only_missing:
                continue
            conn.execute(
                """
                UPDATE coverage_targets
                SET source_name = ?, source_kind = ?, active = 1, notes = ?, updated_at = datetime('now')
                WHERE source_key = ?
                """,
                (source_name, source_kind, notes, source_key),
            )
            updated += 1
            continue

        conn.execute(
            """
            INSERT INTO coverage_targets (
                source_key, source_name, source_kind, active, notes, created_at, updated_at
            ) VALUES (?, ?, ?, 1, ?, datetime('now'), datetime('now'))
            """,
            (source_key, source_name, source_kind, notes),
        )
        inserted += 1

    conn.commit()
    return inserted, updated


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        value = item.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _load_ids_from_file(path: Path) -> list[str]:
    """Load decision IDs from JSON/JSONL/text files."""
    text = path.read_text(encoding="utf-8")
    stripped = text.strip()
    if not stripped:
        return []

    # JSON list or object wrapper
    if stripped[0] in "[{":
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, list):
                if parsed and isinstance(parsed[0], dict):
                    return _dedupe_preserve_order(
                        [str(row.get("decision_id", "")).strip() for row in parsed]
                    )
                return _dedupe_preserve_order([str(x).strip() for x in parsed])
            if isinstance(parsed, dict):
                if isinstance(parsed.get("decision_ids"), list):
                    return _dedupe_preserve_order(
                        [str(x).strip() for x in parsed["decision_ids"]]
                    )
        except json.JSONDecodeError:
            pass

    # JSONL or plain text one-ID-per-line
    ids: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{"):
            try:
                row = json.loads(line)
                decision_id = str(row.get("decision_id", "")).strip()
                if decision_id:
                    ids.append(decision_id)
                continue
            except json.JSONDecodeError:
                pass
        ids.append(line)
    return _dedupe_preserve_order(ids)


def _ensure_target_row(conn: sqlite3.Connection, source_key: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO coverage_targets (
            source_key, source_name, source_kind, active, created_at, updated_at
        ) VALUES (?, ?, 'unknown', 1, datetime('now'), datetime('now'))
        """,
        (source_key, source_key.replace("_", " "),),
    )


def record_snapshot(
    conn: sqlite3.Connection,
    *,
    source_key: str,
    snapshot_year: int,
    snapshot_date: str,
    decision_ids: list[str],
    notes: str | None = None,
) -> tuple[int, int]:
    """Insert or update one source/year snapshot."""
    raw_count = len(decision_ids)
    ids = _dedupe_preserve_order(decision_ids)
    expected_count = len(ids)
    _ensure_target_row(conn, source_key)
    conn.execute(
        """
        INSERT INTO source_snapshots (
            source_key, snapshot_year, snapshot_date, expected_count, expected_ids_json, notes
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key, snapshot_year, snapshot_date)
        DO UPDATE SET
            expected_count = excluded.expected_count,
            expected_ids_json = excluded.expected_ids_json,
            notes = excluded.notes
        """,
        (
            source_key,
            snapshot_year,
            snapshot_date,
            expected_count,
            json.dumps(ids, ensure_ascii=False),
            notes,
        ),
    )
    conn.execute(
        "UPDATE coverage_targets SET updated_at = datetime('now') WHERE source_key = ?",
        (source_key,),
    )
    conn.commit()
    return raw_count, expected_count


def _latest_snapshots(
    conn: sqlite3.Connection,
    *,
    sources: list[str] | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
) -> list[sqlite3.Row]:
    outer_where: list[str] = []
    inner_where: list[str] = []
    inner_params: list[Any] = []
    outer_params: list[Any] = []
    if sources:
        placeholders = ",".join("?" for _ in sources)
        inner_where.append(f"ss.source_key IN ({placeholders})")
        inner_params.extend(sources)
        outer_where.append(f"s.source_key IN ({placeholders})")
        outer_params.extend(sources)
    if year_from is not None:
        inner_where.append("ss.snapshot_year >= ?")
        inner_params.append(year_from)
        outer_where.append("s.snapshot_year >= ?")
        outer_params.append(year_from)
    if year_to is not None:
        inner_where.append("ss.snapshot_year <= ?")
        inner_params.append(year_to)
        outer_where.append("s.snapshot_year <= ?")
        outer_params.append(year_to)

    inner_filter = f"AND {' AND '.join(inner_where)}" if inner_where else ""
    outer_filter = f"AND {' AND '.join(outer_where)}" if outer_where else ""
    params = inner_params + outer_params

    sql = f"""
        SELECT s.id, s.source_key, s.snapshot_year, s.snapshot_date,
               s.expected_count, s.expected_ids_json, t.source_name
        FROM source_snapshots s
        LEFT JOIN coverage_targets t ON t.source_key = s.source_key
        WHERE s.id = (
            SELECT ss.id
            FROM source_snapshots ss
            WHERE ss.source_key = s.source_key
              AND ss.snapshot_year = s.snapshot_year
              {inner_filter}
            ORDER BY ss.snapshot_date DESC, ss.id DESC
            LIMIT 1
        )
        {outer_filter}
        ORDER BY s.source_key, s.snapshot_year
    """
    return conn.execute(sql, params).fetchall()


def _fetch_existing_ids(conn: sqlite3.Connection, decision_ids: list[str]) -> set[str]:
    if not decision_ids:
        return set()
    found: set[str] = set()
    chunk_size = 800
    for i in range(0, len(decision_ids), chunk_size):
        chunk = decision_ids[i:i + chunk_size]
        rows = conn.execute(
            f"SELECT decision_id FROM decisions WHERE decision_id IN ({','.join('?' for _ in chunk)})",
            chunk,
        ).fetchall()
        for row in rows:
            found.add(row[0])
    return found


def _ingested_count_by_year(conn: sqlite3.Connection, source_key: str, snapshot_year: int) -> int:
    # Try source_spider first (scraper key), fall back to court for older data
    return int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM decisions
            WHERE (source_spider = ? OR court = ?)
              AND SUBSTR(COALESCE(decision_date, ''), 1, 4) = ?
            """,
            (source_key, source_key, str(snapshot_year)),
        ).fetchone()[0]
    )


def _has_decisions_table(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'decisions'
        """
    ).fetchone()
    return row is not None


def generate_gap_report(
    conn: sqlite3.Connection,
    *,
    sources: list[str] | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    missing_only: bool = False,
    include_missing_ids: bool = False,
    max_missing_ids: int = 25,
) -> list[dict[str, Any]]:
    """Compute missing coverage per source/year from latest snapshots."""
    has_decisions = _has_decisions_table(conn)
    rows = _latest_snapshots(
        conn,
        sources=sources,
        year_from=year_from,
        year_to=year_to,
    )

    report: list[dict[str, Any]] = []
    for row in rows:
        source_key = str(row["source_key"])
        expected_count = int(row["expected_count"] or 0)
        expected_ids_raw = str(row["expected_ids_json"] or "[]")

        try:
            expected_ids = _dedupe_preserve_order(
                [str(x).strip() for x in json.loads(expected_ids_raw)]
            )
        except json.JSONDecodeError:
            expected_ids = []

        if expected_ids:
            existing_ids = _fetch_existing_ids(conn, expected_ids) if has_decisions else set()
            missing_ids = [did for did in expected_ids if did not in existing_ids]
            ingested_count = len(expected_ids) - len(missing_ids)
            if expected_count < len(expected_ids):
                expected_count = len(expected_ids)
            missing_count = max(expected_count - ingested_count, 0)
        else:
            ingested_count = (
                _ingested_count_by_year(conn, source_key, int(row["snapshot_year"]))
                if has_decisions else 0
            )
            missing_count = max(expected_count - ingested_count, 0)
            missing_ids = []

        if missing_only and missing_count == 0:
            continue

        item: dict[str, Any] = {
            "source_key": source_key,
            "source_name": row["source_name"] or source_key,
            "snapshot_year": int(row["snapshot_year"]),
            "snapshot_date": row["snapshot_date"],
            "expected_count": expected_count,
            "ingested_count": ingested_count,
            "missing_count": missing_count,
            "coverage_ratio": round((ingested_count / expected_count), 4) if expected_count else 1.0,
        }
        if include_missing_ids:
            item["missing_ids"] = missing_ids[:max_missing_ids]
            item["missing_ids_truncated"] = max(len(missing_ids) - max_missing_ids, 0)
        report.append(item)

    return report


def _print_report(rows: list[dict[str, Any]], *, show_ids: bool = False) -> None:
    if not rows:
        print("No snapshot rows matched filters.")
        return

    header = (
        "source_key".ljust(24)
        + "year".rjust(6)
        + "expected".rjust(10)
        + "ingested".rjust(10)
        + "missing".rjust(9)
        + "coverage".rjust(10)
        + "  snapshot"
    )
    print(header)
    print("-" * len(header))
    for row in rows:
        print(
            str(row["source_key"]).ljust(24)
            + str(row["snapshot_year"]).rjust(6)
            + str(row["expected_count"]).rjust(10)
            + str(row["ingested_count"]).rjust(10)
            + str(row["missing_count"]).rjust(9)
            + f"{row['coverage_ratio'] * 100:9.1f}%"
            + f"  {row['snapshot_date']}"
        )
        if show_ids and row.get("missing_ids"):
            print(f"  missing_ids: {', '.join(row['missing_ids'])}")
            truncated = int(row.get("missing_ids_truncated", 0) or 0)
            if truncated:
                print(f"  ... +{truncated} more")


def main() -> None:
    parser = argparse.ArgumentParser(description="Coverage target/snapshot management and gap reporting")
    parser.add_argument(
        "--db",
        type=str,
        default="output/decisions.db",
        help="SQLite database path (default: output/decisions.db)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")

    sub = parser.add_subparsers(dest="command", required=True)

    seed = sub.add_parser("seed-targets", help="Seed coverage_targets from run_scraper SCRAPERS registry")
    seed.add_argument(
        "--only-missing",
        action="store_true",
        help="Insert missing targets only (do not update existing rows)",
    )

    record = sub.add_parser("record-snapshot", help="Record expected IDs for one source/year snapshot")
    record.add_argument("--source", required=True, help="Source key, e.g. bger or zh_gerichte")
    record.add_argument("--year", required=True, type=int, help="Snapshot year")
    record.add_argument(
        "--snapshot-date",
        default=date.today().isoformat(),
        help="Snapshot date YYYY-MM-DD (default: today)",
    )
    record.add_argument(
        "--ids-file",
        required=True,
        type=str,
        help="Path to file containing expected decision IDs (JSON list, JSONL, or plain text)",
    )
    record.add_argument("--notes", type=str, default=None, help="Optional notes")

    report = sub.add_parser("gap-report", help="Compute missing IDs/counts per source/year")
    report.add_argument(
        "--source",
        action="append",
        default=None,
        help="Filter source key (repeatable)",
    )
    report.add_argument("--year-from", type=int, default=None, help="Filter minimum year")
    report.add_argument("--year-to", type=int, default=None, help="Filter maximum year")
    report.add_argument("--missing-only", action="store_true", help="Show only rows with missing_count > 0")
    report.add_argument(
        "--show-missing-ids",
        action="store_true",
        help="Include missing decision IDs in output",
    )
    report.add_argument(
        "--max-missing-ids",
        type=int,
        default=25,
        help="Maximum missing IDs to print/export per row",
    )
    report.add_argument(
        "--json-output",
        type=str,
        default=None,
        help="Optional JSON output path",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    ensure_coverage_tables(conn)

    if args.command == "seed-targets":
        inserted, updated = seed_targets_from_scrapers(conn, only_missing=args.only_missing)
        print(f"seed-targets: inserted={inserted}, updated={updated}")
        conn.close()
        return

    if args.command == "record-snapshot":
        ids = _load_ids_from_file(Path(args.ids_file))
        raw_count, expected_count = record_snapshot(
            conn,
            source_key=args.source,
            snapshot_year=args.year,
            snapshot_date=args.snapshot_date,
            decision_ids=ids,
            notes=args.notes,
        )
        print(
            f"record-snapshot: source={args.source} year={args.year} "
            f"raw={raw_count} expected_count={expected_count}"
        )
        conn.close()
        return

    if args.command == "gap-report":
        rows = generate_gap_report(
            conn,
            sources=args.source,
            year_from=args.year_from,
            year_to=args.year_to,
            missing_only=args.missing_only,
            include_missing_ids=args.show_missing_ids,
            max_missing_ids=args.max_missing_ids,
        )
        _print_report(rows, show_ids=args.show_missing_ids)
        if args.json_output:
            out_path = Path(args.json_output)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"wrote {len(rows)} rows to {out_path}")
        conn.close()
        return

    conn.close()


if __name__ == "__main__":
    main()

"""Incremental decision_structure rebuild — shadow mode v0.1 (2026-05-06).

The full extractor (``extract_decision_structure.py --build``) reads every
JSONL shard, runs the regex/heuristic extractor over each row's
``full_text``, and writes a fresh sidecar DB. With 110 shards × 971 K
decisions × silent FTS5 paragraph rebuild, that's ~85 min on the data
volume — most of which is wasted when only a handful of decisions
changed since the last build.

This module mirrors ``build_reference_graph_incremental.py`` (which we
shipped earlier today): track an extractor_hash per decision_id in a
``processed_decisions`` state table inside decision_structure.db, diff
against the live decisions.db, and re-extract only new/changed rows.

Key invariants (same shape as the reference-graph one)
------------------------------------------------------
* Default output is a SIBLING file (``decision_structure_incremental.db``);
  must opt into ``--in-place`` to touch the live sidecar.
* ``EXTRACTOR_VERSION`` bump forces a full rebuild on the next run; any
  schema or extraction-logic change must bump this.
* The hash covers every field the extractor reads from a decision
  (``full_text``, ``language``, ``decision_id``, plus the metadata that
  flows to the structure table).
* Atomic-swap pattern with sidecar cleanup (post-mortem 2026-05-04 → 05).
* FTS5 keeps in lockstep via triggers — no end-of-build silent rebuild.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from search_stack.extract_decision_structure import (  # noqa: E402
    SCHEMA,
    extract,
)


EXTRACTOR_VERSION = 1

# Schema additions on top of the existing decision_structure schema. The
# triggers keep FTS5 lockstep so the post-build "rebuild" silent phase
# isn't needed on incremental runs.
INCREMENTAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_decisions (
    decision_id TEXT PRIMARY KEY,
    extractor_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS erwaegungen_paragraph_ai
AFTER INSERT ON erwaegungen_paragraph BEGIN
    INSERT INTO erwaegungen_paragraph_fts(rowid, text)
    VALUES (new.rowid, new.text);
END;

CREATE TRIGGER IF NOT EXISTS erwaegungen_paragraph_ad
AFTER DELETE ON erwaegungen_paragraph BEGIN
    INSERT INTO erwaegungen_paragraph_fts(erwaegungen_paragraph_fts, rowid, text)
    VALUES ('delete', old.rowid, old.text);
END;
"""

# Fields the extractor reads from each row. If the extractor is ever
# extended to read additional fields, bump EXTRACTOR_VERSION above so
# existing sidecars are forced through a full rebuild on the next run.
_HASHED_FIELDS = (
    "decision_id",
    "court",
    "canton",
    "language",
    "decision_date",
    "full_text",
)


def _extractor_hash(row: dict) -> str:
    parts = [(row.get(f) or "") for f in _HASHED_FIELDS]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _ensure_state_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.executescript(INCREMENTAL_SCHEMA)


def _get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def _peek_extractor_version(db_path: Path) -> str | None:
    """Read meta.extractor_version from a candidate base DB, or None."""
    if not db_path.exists():
        return None
    try:
        peek = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    except sqlite3.Error:
        return None
    try:
        try:
            return _get_meta(peek, "extractor_version")
        except sqlite3.OperationalError:
            return None
    finally:
        peek.close()


def _select_diff_base(live_db: Path, output_path: Path,
                      force_full: bool) -> tuple[Path | None, str | None]:
    """Pick the DB whose processed-state we diff against.

    Mirrors the reference-graph incremental builder: prefer the PREVIOUS
    incremental output (the sibling) — in shadow mode the live DB is
    full-rebuilt nightly WITHOUT state tables, so peeking only at the
    live DB forced a full bootstrap every night. Falls back to the live
    DB (also the in-place path, where output_path == live_db). Returns
    (base, bootstrap_reason); base is None when a full bootstrap is
    required.
    """
    if force_full:
        return None, "force_full"
    candidates = ([output_path, live_db]
                  if output_path != live_db else [live_db])
    mismatches = []
    for cand in candidates:
        stored = _peek_extractor_version(cand)
        if stored == str(EXTRACTOR_VERSION):
            return cand, None
        if stored is not None:
            mismatches.append(f"{cand.name}:{stored}")
    if mismatches:
        return None, "version_mismatch:" + ",".join(mismatches)
    return None, "no_state"


def _open_decisions_ro(decisions_db: Path) -> sqlite3.Connection:
    last: Exception | None = None
    for _ in range(5):
        try:
            return sqlite3.connect(
                f"file:{decisions_db}?mode=ro&immutable=1",
                uri=True, timeout=1.0,
            )
        except sqlite3.OperationalError as e:
            last = e
            time.sleep(0.2)
    raise sqlite3.OperationalError(f"Unable to open decisions.db: {last}")


def _iter_decision_rows(decisions_db: Path) -> Iterator[dict]:
    conn = _open_decisions_ro(decisions_db)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT decision_id, court, canton, language, decision_date,
                   full_text, regeste
            FROM decisions
            WHERE full_text IS NOT NULL AND length(full_text) >= 500
            ORDER BY rowid
            """
        )
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                yield dict(row)
    finally:
        conn.close()


def _diff_state(
    decisions_db: Path,
    sc_conn: sqlite3.Connection,
) -> tuple[set[str], set[str], set[str], dict[str, str], dict[str, dict]]:
    """Return (new, changed, deleted, hashes_by_id, rows_for_writes)."""
    processed: dict[str, str] = {
        r[0]: r[1]
        for r in sc_conn.execute(
            "SELECT decision_id, extractor_hash FROM processed_decisions"
        )
    }

    new_ids: set[str] = set()
    changed_ids: set[str] = set()
    seen: set[str] = set()
    hashes_by_id: dict[str, str] = {}
    rows_for_writes: dict[str, dict] = {}

    for row in _iter_decision_rows(decisions_db):
        did = row.get("decision_id") or ""
        if not did:
            continue
        seen.add(did)
        h = _extractor_hash(row)
        hashes_by_id[did] = h
        prior = processed.get(did)
        if prior is None:
            new_ids.add(did)
            rows_for_writes[did] = row
        elif prior != h:
            changed_ids.add(did)
            rows_for_writes[did] = row

    deleted_ids = set(processed.keys()) - seen
    return new_ids, changed_ids, deleted_ids, hashes_by_id, rows_for_writes


def _delete_for_decisions(conn: sqlite3.Connection, ids: set[str]) -> None:
    """Cascade-delete every artifact for ``ids``. FTS5 trigger fires on
    erwaegungen_paragraph DELETE so the FTS5 vtab stays in lockstep."""
    if not ids:
        return
    cur = conn.cursor()
    for did in ids:
        cur.execute(
            "DELETE FROM erwaegungen_paragraph WHERE decision_id = ?", (did,),
        )
        cur.execute(
            "DELETE FROM structure WHERE decision_id = ?", (did,),
        )
        cur.execute(
            "DELETE FROM processed_decisions WHERE decision_id = ?", (did,),
        )


def _apply_extraction(
    conn: sqlite3.Connection,
    rows: dict[str, dict],
    hashes_by_id: dict[str, str],
) -> tuple[int, int]:
    """Run extract() over the given rows; upsert ``structure`` +
    ``erwaegungen_paragraph``. Returns (decisions_written, paragraphs_written).
    """
    decisions_n = 0
    paragraphs_n = 0
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for did, row in rows.items():
        ft = row.get("full_text") or ""
        if len(ft) < 500:
            continue
        s = extract(ft, row.get("language", "de"), did)

        # Upsert into structure
        conn.execute(
            """
            INSERT INTO structure
            (decision_id, court, canton, language, decision_date, regeste,
             sachverhalt, sachverhalt_method,
             erwaegungen, erwaegungen_method, erwaegungen_paragraph_count,
             dispositiv, dispositiv_method, dispositiv_orders, extracted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(decision_id) DO UPDATE SET
                court = excluded.court,
                canton = excluded.canton,
                language = excluded.language,
                decision_date = excluded.decision_date,
                regeste = excluded.regeste,
                sachverhalt = excluded.sachverhalt,
                sachverhalt_method = excluded.sachverhalt_method,
                erwaegungen = excluded.erwaegungen,
                erwaegungen_method = excluded.erwaegungen_method,
                erwaegungen_paragraph_count = excluded.erwaegungen_paragraph_count,
                dispositiv = excluded.dispositiv,
                dispositiv_method = excluded.dispositiv_method,
                dispositiv_orders = excluded.dispositiv_orders,
                extracted_at = excluded.extracted_at
            """,
            (
                did, row.get("court"), row.get("canton"),
                s.language, row.get("decision_date"),
                row.get("regeste") or None,
                s.sachverhalt, s.sachverhalt_method,
                s.erwaegungen, s.erwaegungen_method,
                len(s.erwaegungen_paragraphs),
                s.dispositiv, s.dispositiv_method,
                json.dumps(s.dispositiv_orders, ensure_ascii=False)
                if s.dispositiv_orders else None,
                now,
            ),
        )

        # Replace this decision's paragraphs (delete then insert; trigger
        # keeps FTS5 in sync). Skip the synthetic depth=0 fallback.
        # Use INSERT OR REPLACE to match extract_decision_structure.py's
        # full-builder semantics — the extractor can emit two paragraphs
        # with the same e_number for a single decision when the regex
        # backtracks across nested numbering (e.g., "2." inside an
        # "Erwägung 2"). The full builder silently last-wins on those;
        # the incremental builder was crashing with UNIQUE constraint
        # violations on the first-real-run today 2026-05-18 16:51 UTC
        # (decision_structure_incremental.py:273).
        conn.execute(
            "DELETE FROM erwaegungen_paragraph WHERE decision_id = ?", (did,),
        )
        for p in s.erwaegungen_paragraphs:
            if p.get("depth", 0) == 0:
                continue
            conn.execute(
                """
                INSERT OR REPLACE INTO erwaegungen_paragraph
                (decision_id, e_number, depth, parent, text)
                VALUES (?, ?, ?, ?, ?)
                """,
                (did, p["e_number"], p["depth"], p.get("parent"), p["text"]),
            )
            paragraphs_n += 1

        conn.execute(
            "INSERT INTO processed_decisions(decision_id, extractor_hash) "
            "VALUES (?, ?) "
            "ON CONFLICT(decision_id) DO UPDATE SET extractor_hash = excluded.extractor_hash",
            (did, hashes_by_id[did]),
        )
        decisions_n += 1

    return decisions_n, paragraphs_n


def _cleanup_sidecars(target: Path) -> None:
    for ext in ("-wal", "-shm"):
        p = Path(str(target) + ext)
        if p.exists():
            p.unlink()


def _bootstrap_via_full(
    *,
    decisions_db: Path,
    output_path: Path,
) -> dict:
    """No prior state — write a fresh DB by running ``extract()`` over
    every decision in decisions.db (mirrors what extract_decision_structure
    does over JSONL, but keyed on the decisions.db row set so we stay
    consistent with downstream tools).
    """
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    tmp = output_path.with_name(f".{output_path.name}.tmp")
    if tmp.exists():
        tmp.unlink()

    conn = sqlite3.connect(str(tmp))
    conn.executescript(SCHEMA)
    conn.executescript(INCREMENTAL_SCHEMA)

    # Read every decision once; treat all as "new" relative to an empty state.
    rows_for_writes: dict[str, dict] = {}
    hashes_by_id: dict[str, str] = {}
    for row in _iter_decision_rows(decisions_db):
        did = row["decision_id"]
        rows_for_writes[did] = row
        hashes_by_id[did] = _extractor_hash(row)

    decisions_n, paragraphs_n = _apply_extraction(
        conn, rows_for_writes, hashes_by_id,
    )

    _set_meta(conn, "extractor_version", str(EXTRACTOR_VERSION))
    _set_meta(
        conn,
        "last_full_rebuild_at",
        datetime.now(timezone.utc).isoformat(),
    )

    conn.commit()
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.close()
    _cleanup_sidecars(tmp)

    os.replace(tmp, output_path)
    _cleanup_sidecars(output_path)

    return {
        "mode": "full_bootstrap",
        "decisions_written": decisions_n,
        "paragraphs_written": paragraphs_n,
    }


def build_structure_incremental(
    *,
    decisions_db: Path,
    structure_db: Path,
    output_path: Path | None = None,
    force_full: bool = False,
) -> dict:
    """Mirrors ``build_graph_incremental`` from the reference graph
    incremental builder."""
    decisions_db = decisions_db.resolve()
    structure_db = structure_db.resolve()
    output_path = (
        output_path.resolve()
        if output_path is not None
        else structure_db.with_name(
            structure_db.stem + "_incremental" + structure_db.suffix
        )
    )

    t0 = time.time()
    stats: dict = {
        "decisions_db": str(decisions_db),
        "structure_db": str(structure_db),
        "output_path": str(output_path),
        "extractor_version": EXTRACTOR_VERSION,
    }

    base, bootstrap_reason = _select_diff_base(
        structure_db, output_path, force_full)

    if base is None:
        stats["bootstrap_reason"] = bootstrap_reason
        full_stats = _bootstrap_via_full(
            decisions_db=decisions_db,
            output_path=output_path,
        )
        full_stats["elapsed_seconds"] = round(time.time() - t0, 2)
        stats.update(full_stats)
        return stats

    # Copy base → tmp; clean any sidecars that came along.
    stats["diff_base"] = str(base)
    tmp_path = output_path.with_name(f".{output_path.name}.tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(base, tmp_path)
    _cleanup_sidecars(tmp_path)

    conn = sqlite3.connect(str(tmp_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_state_tables(conn)

    try:
        new_ids, changed_ids, deleted_ids, hashes_by_id, rows_for_writes = (
            _diff_state(decisions_db, conn)
        )
        stats["counts"] = {
            "new": len(new_ids),
            "changed": len(changed_ids),
            "deleted": len(deleted_ids),
        }

        _delete_for_decisions(conn, changed_ids | deleted_ids)
        decisions_n, paragraphs_n = _apply_extraction(
            conn, rows_for_writes, hashes_by_id,
        )
        stats["decisions_written"] = decisions_n
        stats["paragraphs_written"] = paragraphs_n

        _set_meta(conn, "extractor_version", str(EXTRACTOR_VERSION))
        _set_meta(
            conn,
            "last_incremental_run_at",
            datetime.now(timezone.utc).isoformat(),
        )

        conn.commit()
        stats["totals"] = {
            "structure": conn.execute(
                "SELECT COUNT(*) FROM structure"
            ).fetchone()[0],
            "erwaegungen_paragraphs": conn.execute(
                "SELECT COUNT(*) FROM erwaegungen_paragraph"
            ).fetchone()[0],
            "processed_decisions": conn.execute(
                "SELECT COUNT(*) FROM processed_decisions"
            ).fetchone()[0],
        }
    except Exception:
        conn.close()
        if tmp_path.exists():
            tmp_path.unlink()
        _cleanup_sidecars(tmp_path)
        raise

    conn.execute("PRAGMA journal_mode=DELETE")
    conn.close()
    _cleanup_sidecars(tmp_path)

    os.replace(tmp_path, output_path)
    _cleanup_sidecars(output_path)

    stats["mode"] = "incremental"
    stats["elapsed_seconds"] = round(time.time() - t0, 2)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Incremental decision_structure rebuild. Default writes to a "
            "sibling .incremental.db file — pass --in-place to mutate the "
            "live sidecar."
        ),
    )
    parser.add_argument(
        "--decisions-db", type=Path, required=True,
        help="Source decisions.db (read-only, immutable=1)",
    )
    parser.add_argument(
        "--structure-db", type=Path, required=True,
        help="Existing decision_structure.db (state diffed against this)",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help=("Where to write the rebuilt sidecar. Defaults to a sibling "
              "with `_incremental` suffix (shadow mode)."),
    )
    parser.add_argument(
        "--in-place", action="store_true",
        help="Overwrite --structure-db at the end. NOT default; opt-in.",
    )
    parser.add_argument(
        "--force-full", action="store_true",
        help="Skip diff path and run a full rebuild.",
    )
    args = parser.parse_args()

    output_path = args.output
    if output_path is None and args.in_place:
        output_path = args.structure_db

    stats = build_structure_incremental(
        decisions_db=args.decisions_db,
        structure_db=args.structure_db,
        output_path=output_path,
        force_full=args.force_full,
    )
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

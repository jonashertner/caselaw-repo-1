#!/usr/bin/env python3
"""
Build a local reference graph (decision->decision, decision->statute) from JSONL.

This enables:
- citation traversal ("what cites this decision?")
- statute traversal ("which decisions mention Art. 8 EMRK?")
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path
from typing import Iterator

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from search_stack.reference_extraction import extract_case_citations, extract_statute_references  # noqa: E402


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT PRIMARY KEY,
    docket_number TEXT,
    docket_norm TEXT,
    court TEXT,
    canton TEXT,
    language TEXT,
    decision_date TEXT
);

CREATE INDEX IF NOT EXISTS idx_decisions_docket_norm ON decisions(docket_norm);
CREATE INDEX IF NOT EXISTS idx_decisions_court ON decisions(court);
CREATE INDEX IF NOT EXISTS idx_decisions_date ON decisions(decision_date);

CREATE TABLE IF NOT EXISTS statutes (
    statute_id TEXT PRIMARY KEY,
    law_code TEXT NOT NULL,
    article TEXT NOT NULL,
    paragraph TEXT
);

CREATE INDEX IF NOT EXISTS idx_statutes_law_article ON statutes(law_code, article);

CREATE TABLE IF NOT EXISTS decision_statutes (
    decision_id TEXT NOT NULL,
    statute_id TEXT NOT NULL,
    mention_count INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (decision_id, statute_id),
    FOREIGN KEY (decision_id) REFERENCES decisions(decision_id),
    FOREIGN KEY (statute_id) REFERENCES statutes(statute_id)
);

CREATE INDEX IF NOT EXISTS idx_decision_statutes_statute ON decision_statutes(statute_id);

CREATE TABLE IF NOT EXISTS decision_citations (
    source_decision_id TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    target_type TEXT NOT NULL, -- decision|bge|docket
    mention_count INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (source_decision_id, target_ref),
    FOREIGN KEY (source_decision_id) REFERENCES decisions(decision_id)
);

CREATE INDEX IF NOT EXISTS idx_decision_citations_target_ref ON decision_citations(target_ref);

CREATE TABLE IF NOT EXISTS citation_targets (
    source_decision_id TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    target_decision_id TEXT NOT NULL,
    match_type TEXT NOT NULL DEFAULT 'docket_norm',
    confidence_score REAL NOT NULL DEFAULT 0.5,
    PRIMARY KEY (source_decision_id, target_ref, target_decision_id),
    FOREIGN KEY (source_decision_id, target_ref)
        REFERENCES decision_citations(source_decision_id, target_ref),
    FOREIGN KEY (target_decision_id) REFERENCES decisions(decision_id)
);

CREATE INDEX IF NOT EXISTS idx_citation_targets_target_decision_id
    ON citation_targets(target_decision_id);
"""


def _docket_norm(value: str | None) -> str:
    if not value:
        return ""
    out = value.strip().upper().replace("-", "_").replace(".", "_").replace("/", "_")
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_")


# BStGer two-letter prefixes
_BSTGER_PREFIXES = frozenset({
    "BB", "BG", "BH", "BK", "BN", "BP",
    "CA", "CB", "CR", "RR",
    "SK", "SN", "SP", "TP",
})


def _infer_court_from_docket(docket_norm: str) -> str | None:
    """Infer the likely court from a normalized docket reference.

    Swiss federal courts use distinctive docket prefixes:
    - BGer: digit+letter (e.g. 6B_1234_2025, 4A_291_2017)
    - BVGer: single letter A-F (e.g. E_5783_2024, D_8226_2025)
    - BStGer: two-letter codes (e.g. SK_2025_1234, BB_2024_100)
    """
    if not docket_norm:
        return None
    # BGer: starts with digit + uppercase letter + underscore + digit
    if re.match(r"^[1-9][A-Z]_\d", docket_norm):
        return "bger"
    # BVGer: single letter A-F + underscore + digits + underscore + 4-digit year
    if re.match(r"^[A-F]_\d{1,6}_\d{4}$", docket_norm):
        return "bvger"
    # BStGer: known two-letter prefix
    m = re.match(r"^([A-Z]{2})_", docket_norm)
    if m and m.group(1) in _BSTGER_PREFIXES:
        return "bstger"
    return None


def _parse_iso_date(value: str | None) -> date | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _citation_confidence(
    *,
    source_court: str | None,
    source_canton: str | None,
    source_date: str | None,
    target_court: str | None,
    target_canton: str | None,
    target_date: str | None,
    target_ref: str | None = None,
    candidate_rank: int,
    candidate_count: int,
) -> float:
    score = 0.55

    # Docket pattern implies court — strongest disambiguation signal.
    # E.g. "4A_291_2017" is almost certainly BGer, "E_5783_2024" is BVGer.
    if target_ref:
        inferred_court = _infer_court_from_docket(target_ref)
        if inferred_court and target_court:
            if target_court == inferred_court:
                score += 0.20
            else:
                score -= 0.20

    if source_canton and target_canton and source_canton == target_canton:
        score += 0.10
    if source_court and target_court and source_court == target_court:
        score += 0.08

    src_dt = _parse_iso_date(source_date)
    tgt_dt = _parse_iso_date(target_date)
    if src_dt and tgt_dt:
        delta_days = (src_dt - tgt_dt).days
        if delta_days >= 0:
            score += 0.15
        else:
            score -= 0.15

        abs_days = abs(delta_days)
        if abs_days <= 365:
            score += 0.10
        elif abs_days <= (3 * 365):
            score += 0.05

    if candidate_rank == 1:
        score += 0.05
    elif candidate_rank == 2:
        score += 0.02

    if candidate_count > 1:
        score -= min(0.15, 0.03 * (candidate_count - 1))

    return max(0.05, min(0.99, round(score, 4)))


def _resolve_citation_targets(conn: sqlite3.Connection) -> None:
    cursor = conn.execute(
        """
        WITH candidate_matches AS (
            SELECT
                dc.source_decision_id,
                dc.target_ref,
                sd.court AS source_court,
                sd.canton AS source_canton,
                sd.decision_date AS source_date,
                td.decision_id AS target_decision_id,
                td.court AS target_court,
                td.canton AS target_canton,
                td.decision_date AS target_date,
                ROW_NUMBER() OVER (
                    PARTITION BY dc.source_decision_id, dc.target_ref
                    ORDER BY td.decision_date DESC, td.decision_id
                ) AS candidate_rank,
                COUNT(*) OVER (
                    PARTITION BY dc.source_decision_id, dc.target_ref
                ) AS candidate_count
            FROM decision_citations dc
            JOIN decisions td
              ON td.docket_norm = dc.target_ref
            LEFT JOIN decisions sd
              ON sd.decision_id = dc.source_decision_id
            WHERE dc.target_type = 'docket'
              AND td.decision_id <> dc.source_decision_id
        )
        SELECT
            source_decision_id,
            target_ref,
            target_decision_id,
            source_court,
            source_canton,
            source_date,
            target_court,
            target_canton,
            target_date,
            candidate_rank,
            candidate_count
        FROM candidate_matches
        ORDER BY source_decision_id, target_ref, candidate_rank
        """
    )
    insert_sql = """
        INSERT OR IGNORE INTO citation_targets
        (source_decision_id, target_ref, target_decision_id, match_type, confidence_score)
        VALUES (?, ?, ?, ?, ?)
    """
    batch_size = 10_000
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        payload: list[tuple[str, str, str, str, float]] = []
        for row in rows:
            payload.append(
                (
                    row["source_decision_id"],
                    row["target_ref"],
                    row["target_decision_id"],
                    "docket_norm",
                    _citation_confidence(
                        source_court=row["source_court"],
                        source_canton=row["source_canton"],
                        source_date=row["source_date"],
                        target_court=row["target_court"],
                        target_canton=row["target_canton"],
                        target_date=row["target_date"],
                        target_ref=row["target_ref"],
                        candidate_rank=int(row["candidate_rank"] or 1),
                        candidate_count=int(row["candidate_count"] or 1),
                    ),
                )
            )
        conn.executemany(insert_sql, payload)

    # Second pass: resolve BGE citations.
    # BGE target_ref = "BGE 147 I 268", BGE docket_norm = "147 I 268".
    cursor = conn.execute(
        """
        WITH bge_matches AS (
            SELECT
                dc.source_decision_id,
                dc.target_ref,
                sd.court AS source_court,
                sd.canton AS source_canton,
                sd.decision_date AS source_date,
                td.decision_id AS target_decision_id,
                td.court AS target_court,
                td.canton AS target_canton,
                td.decision_date AS target_date,
                1 AS candidate_rank,
                1 AS candidate_count
            FROM decision_citations dc
            JOIN decisions td
              ON td.docket_norm = SUBSTR(dc.target_ref, 5)
             AND td.court IN ('bge', 'bger')
            LEFT JOIN decisions sd
              ON sd.decision_id = dc.source_decision_id
            WHERE dc.target_type = 'bge'
              AND dc.target_ref LIKE 'BGE %'
              AND td.decision_id <> dc.source_decision_id
        )
        SELECT
            source_decision_id,
            target_ref,
            target_decision_id,
            source_court,
            source_canton,
            source_date,
            target_court,
            target_canton,
            target_date,
            candidate_rank,
            candidate_count
        FROM bge_matches
        ORDER BY source_decision_id, target_ref
        """
    )
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        payload = []
        for row in rows:
            payload.append(
                (
                    row["source_decision_id"],
                    row["target_ref"],
                    row["target_decision_id"],
                    "bge_norm",
                    _citation_confidence(
                        source_court=row["source_court"],
                        source_canton=row["source_canton"],
                        source_date=row["source_date"],
                        target_court=row["target_court"],
                        target_canton=row["target_canton"],
                        target_date=row["target_date"],
                        candidate_rank=int(row["candidate_rank"] or 1),
                        candidate_count=int(row["candidate_count"] or 1),
                    ),
                )
            )
        conn.executemany(insert_sql, payload)


def build_graph(
    *,
    input_dir: Path,
    db_path: Path,
    limit: int | None = None,
    source_db: Path | None = None,
    courts: list[str] | None = None,
) -> dict:
    t0 = time.time()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = db_path.with_name(f".{db_path.name}.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(tmp_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(SCHEMA_SQL)

        decisions = 0
        statute_edges = 0
        citation_edges = 0

        row_iter = _iter_rows_from_source(
            input_dir=input_dir,
            source_db=source_db,
            courts=courts,
        )
        for row in row_iter:
            decision_id = row.get("decision_id")
            if not decision_id:
                continue

            docket_number = row.get("docket_number") or ""
            conn.execute(
                """
                INSERT OR IGNORE INTO decisions
                (decision_id, docket_number, docket_norm, court, canton, language, decision_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    docket_number,
                    _docket_norm(docket_number),
                    row.get("court"),
                    row.get("canton"),
                    row.get("language"),
                    row.get("decision_date"),
                ),
            )
            decisions += 1

            text = " ".join(
                [
                    row.get("title") or "",
                    row.get("regeste") or "",
                    row.get("full_text") or "",
                ]
            )
            statutes = extract_statute_references(text)
            citations = extract_case_citations(text)

            for statute in statutes:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO statutes(statute_id, law_code, article, paragraph)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        statute.normalized,
                        statute.law_code,
                        statute.article,
                        statute.paragraph,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO decision_statutes(decision_id, statute_id, mention_count)
                    VALUES (?, ?, 1)
                    ON CONFLICT(decision_id, statute_id)
                    DO UPDATE SET mention_count = mention_count + 1
                    """,
                    (decision_id, statute.normalized),
                )
                statute_edges += 1

            for citation in citations:
                target_type = "bge" if citation.citation_type == "bge" else "docket"
                conn.execute(
                    """
                    INSERT INTO decision_citations
                    (source_decision_id, target_ref, target_type, mention_count)
                    VALUES (?, ?, ?, 1)
                    ON CONFLICT(source_decision_id, target_ref)
                    DO UPDATE SET mention_count = mention_count + 1
                    """,
                    (decision_id, citation.normalized, target_type),
                )
                citation_edges += 1

            if decisions % 1000 == 0:
                conn.commit()
                if decisions % 10000 == 0:
                    elapsed = time.time() - t0
                    print(
                        f"  [{elapsed:.0f}s] {decisions:,} decisions processed, "
                        f"{statute_edges:,} statute edges, {citation_edges:,} citation edges",
                        file=sys.stderr,
                        flush=True,
                    )

            if limit and decisions >= limit:
                break

        conn.commit()
        _resolve_citation_targets(conn)
        conn.commit()

        total_decisions = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        total_statutes = conn.execute("SELECT COUNT(*) FROM statutes").fetchone()[0]
        total_citations = conn.execute("SELECT COUNT(*) FROM decision_citations").fetchone()[0]
        resolved_refs = conn.execute(
            "SELECT COUNT(DISTINCT source_decision_id || '|' || target_ref) FROM citation_targets"
        ).fetchone()[0]
        resolved_links = conn.execute(
            "SELECT COUNT(*) FROM citation_targets"
        ).fetchone()[0]
    except Exception:
        if conn is not None:
            conn.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    else:
        conn.close()
        os.replace(tmp_path, db_path)

    return {
        "db_path": str(db_path),
        "source_db": str(source_db) if source_db else None,
        "courts_filter": courts or [],
        "decisions_ingested_lines": decisions,
        "statute_edges_ingested": statute_edges,
        "citation_edges_ingested": citation_edges,
        "decisions_total": total_decisions,
        "statutes_total": total_statutes,
        "citations_total": total_citations,
        "citations_resolved": resolved_refs,
        "citation_target_links": resolved_links,
    }


def _iter_rows_from_source(
    *,
    input_dir: Path,
    source_db: Path | None,
    courts: list[str] | None,
) -> Iterator[dict]:
    if source_db:
        yield from _iter_rows_from_db(source_db=source_db, courts=courts)
        return
    yield from _iter_rows_from_jsonl(input_dir)


def _iter_rows_from_jsonl(input_dir: Path) -> Iterator[dict]:
    for jsonl_path in sorted(input_dir.glob("*.jsonl")):
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def _iter_rows_from_db(*, source_db: Path, courts: list[str] | None) -> Iterator[dict]:
    conn = _open_sqlite_readonly(source_db)
    conn.row_factory = sqlite3.Row
    try:
        params: list[str] = []
        where = ""
        if courts:
            placeholders = ",".join("?" for _ in courts)
            where = f"WHERE lower(court) IN ({placeholders})"
            params = [c.lower() for c in courts]
        sql = f"""
            SELECT decision_id, docket_number, court, canton, language, decision_date,
                   title, regeste, full_text
            FROM decisions
            {where}
            ORDER BY rowid
        """
        cur = conn.execute(sql, params)
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                yield dict(row)
    finally:
        conn.close()


def _open_sqlite_readonly(path: Path) -> sqlite3.Connection:
    last_error: Exception | None = None
    for _ in range(5):
        try:
            return sqlite3.connect(
                f"file:{path}?mode=ro",
                uri=True,
                timeout=1.0,
            )
        except sqlite3.OperationalError as e:
            last_error = e
            time.sleep(0.2)
    raise sqlite3.OperationalError(f"Unable to open source DB {path}: {last_error}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build citation/statute graph from decision JSONL files")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("output/decisions"),
        help="Directory containing *.jsonl decision files",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/reference_graph.db"),
        help="Output SQLite graph database path",
    )
    parser.add_argument(
        "--source-db",
        type=Path,
        help="Optional source SQLite decisions database (uses decisions table instead of JSONL input)",
    )
    parser.add_argument(
        "--courts",
        type=str,
        help="Optional comma-separated court filter when using --source-db (e.g. bger,bge,bvger)",
    )
    parser.add_argument("--limit", type=int, help="Optional limit for quick test runs")
    args = parser.parse_args()

    courts = None
    if args.courts:
        courts = [c.strip() for c in args.courts.split(",") if c.strip()]

    stats = build_graph(
        input_dir=args.input,
        db_path=args.db,
        limit=args.limit,
        source_db=args.source_db,
        courts=courts,
    )
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

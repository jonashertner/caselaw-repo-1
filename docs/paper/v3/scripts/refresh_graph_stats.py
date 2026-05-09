"""Refresh the citation-graph fields of corpus_graph_stats.json from a freshly
rebuilt reference_graph.db.

Only the citation-graph derived fields change after a resolver rebuild:

  * rg_decisions, rg_citation_edges, rg_resolved_citations
  * rg_distinct_targets, rg_citing_decisions
  * top30_cited, in_degree_buckets
  * match_types
  * cross_lang_matrix

All other fields (corpus stats, statute graph, Materialien, etc.) are
preserved verbatim.

Usage::

    python -m docs.paper.v3.scripts.refresh_graph_stats \
        --graph /opt/caselaw/repo/output/reference_graph.db \
        --json  docs/paper/v3/tables/corpus_graph_stats.json

Run on the VPS where reference_graph.db lives, then ``scp`` the updated
JSON back to the local repo and re-run ``build_tables.py``.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def _corpus_stats_from_fts5(fts_db_path: str) -> dict:
    """Refresh the FTS5-derived corpus fields (total_decisions, total_courts,
    languages). Skips silently if the FTS5 DB isn't accessible at the
    given path — the script then preserves whatever values were in the
    JSON before, so it is safe to run on hosts without the FTS5 DB.
    """
    try:
        c = sqlite3.connect(f"file:{fts_db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        return {}
    try:
        out: dict = {}
        out["total_decisions"] = c.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0]
        out["total_courts"] = c.execute(
            "SELECT COUNT(DISTINCT court) FROM decisions"
        ).fetchone()[0]
        out["total_cantons"] = c.execute(
            "SELECT COUNT(DISTINCT canton) FROM decisions"
        ).fetchone()[0]
        rows = c.execute(
            "SELECT language, COUNT(*) FROM decisions "
            "WHERE language IN ('de','fr','it') GROUP BY language"
        ).fetchall()
        out["languages"] = [{"lang": r[0], "n": r[1]} for r in rows]
        return out
    finally:
        c.close()


def _resolved_stats(cur: sqlite3.Cursor) -> dict:
    out: dict = {}

    cur.execute("SELECT COUNT(*) FROM decisions")
    out["rg_decisions"] = cur.fetchone()[0]

    # rg_citation_edges = distinct (source, target_ref) pairs in
    # decision_citations.  Matches the paper's "outgoing references" prose.
    cur.execute("SELECT COUNT(*) FROM decision_citations")
    out["rg_citation_edges"] = cur.fetchone()[0]

    # rg_resolved_citations = distinct (source, target_ref) pairs that
    # resolved to at least one target decision.  Matches the paper's
    # "edges with target resolved to a corpus decision" prose.
    cur.execute(
        "SELECT COUNT(*) FROM ("
        " SELECT 1 FROM citation_targets"
        " GROUP BY source_decision_id, target_ref"
        ")"
    )
    out["rg_resolved_citations"] = cur.fetchone()[0]

    # citation_target_links: total citation_targets rows (with multi-match).
    # Strictly larger than rg_resolved_citations whenever a target_ref maps
    # to several candidate decisions.
    cur.execute("SELECT COUNT(*) FROM citation_targets")
    out["rg_citation_target_links"] = cur.fetchone()[0]

    # Distinct cited targets, aggregated across BGE id-format variants so the
    # number is comparable to OpenCaseLaw's user-facing decision count rather
    # than reflecting incidental shard duplication.
    cur.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT DISTINCT
              CASE
                  WHEN target_decision_id LIKE 'bge_BGE_%'
                      THEN 'bge_' || REPLACE(SUBSTR(target_decision_id, 9), '_', ' ')
                  ELSE target_decision_id
              END AS canon_id
          FROM citation_targets
        )
        """
    )
    out["rg_distinct_targets"] = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(DISTINCT source_decision_id) FROM citation_targets"
    )
    out["rg_citing_decisions"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM decision_statutes")
    out["rg_statute_edges"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT statute_id) FROM decision_statutes")
    out["rg_distinct_statutes"] = cur.fetchone()[0]

    return out


def _top_cited(cur: sqlite3.Cursor, n: int = 30) -> list:
    """Top decisions by in-degree, aggregating across BGE decision_id format
    variants (the corpus carries both the canonical "bge_125 V 351" and the
    legacy "bge_BGE_125_V_351" form for the same case from different scraper
    shards).  We canonicalise to "BGE <vol> <div> <page>" before grouping.
    """
    cur.execute(
        """
        WITH normed AS (
            SELECT
                CASE
                    WHEN target_decision_id LIKE 'bge_BGE_%'
                        THEN 'bge_' || REPLACE(SUBSTR(target_decision_id, 9), '_', ' ')
                    ELSE target_decision_id
                END AS canon_id,
                source_decision_id
            FROM citation_targets
        )
        SELECT canon_id, COUNT(DISTINCT source_decision_id) AS in_deg
        FROM normed
        GROUP BY canon_id
        ORDER BY in_deg DESC
        LIMIT ?
        """,
        (n,),
    )
    return [{"target_id": r[0], "in_degree": r[1]} for r in cur.fetchall()]


def _in_degree_buckets(cur: sqlite3.Cursor) -> list:
    cur.execute(
        """
        WITH normed AS (
            SELECT
                CASE
                    WHEN target_decision_id LIKE 'bge_BGE_%'
                        THEN 'bge_' || REPLACE(SUBSTR(target_decision_id, 9), '_', ' ')
                    ELSE target_decision_id
                END AS canon_id,
                source_decision_id
            FROM citation_targets
        ),
        in_deg AS (
            SELECT canon_id, COUNT(DISTINCT source_decision_id) AS d
            FROM normed
            GROUP BY canon_id
        )
        SELECT
            SUM(d >= 10000),
            SUM(d >= 1000 AND d < 10000),
            SUM(d >= 100 AND d < 1000),
            SUM(d >= 10 AND d < 100),
            SUM(d >= 2 AND d < 10),
            SUM(d = 1)
        FROM in_deg
        """
    )
    a, b, c, d, e, f = cur.fetchone()
    return [
        {"bucket": "10000+", "n": int(a or 0)},
        {"bucket": "1000-9999", "n": int(b or 0)},
        {"bucket": "100-999", "n": int(c or 0)},
        {"bucket": "10-99", "n": int(d or 0)},
        {"bucket": "2-9", "n": int(e or 0)},
        {"bucket": "1", "n": int(f or 0)},
    ]


def _match_types(cur: sqlite3.Cursor) -> list:
    cur.execute(
        """
        SELECT match_type, COUNT(*) AS n
        FROM citation_targets
        GROUP BY match_type
        ORDER BY n DESC
        """
    )
    return [{"type": r[0], "n": r[1]} for r in cur.fetchall()]


def _cross_lang_matrix(cur: sqlite3.Cursor) -> list:
    cur.execute(
        """
        SELECT sd.language AS src, td.language AS tgt, COUNT(*) AS n
        FROM citation_targets ct
        JOIN decisions sd ON sd.decision_id = ct.source_decision_id
        JOIN decisions td ON td.decision_id = ct.target_decision_id
        WHERE sd.language IN ('de','fr','it')
          AND td.language IN ('de','fr','it')
        GROUP BY sd.language, td.language
        ORDER BY n DESC
        """
    )
    return [{"src": r[0], "tgt": r[1], "n": r[2]} for r in cur.fetchall()]


def refresh(json_path: Path, graph_path: Path, fts_db_path: str | None = None) -> dict:
    snapshot = json.loads(json_path.read_text())

    # Refresh FTS5-derived fields (total_decisions, total_courts, languages)
    # so the paper's corpus_overview matches the deployed search index.
    # If fts_db_path isn't given, default to /opt/caselaw/repo path used in
    # production; on a developer laptop without that DB the function is
    # a no-op and the existing values are preserved.
    if fts_db_path is None:
        fts_db_path = "/opt/caselaw/repo/output/decisions.db"
    fts_stats = _corpus_stats_from_fts5(fts_db_path)
    if fts_stats:
        snapshot.update(fts_stats)

    conn = sqlite3.connect(f"file:{graph_path}?mode=ro", uri=True)
    cur = conn.cursor()

    snapshot.update(_resolved_stats(cur))
    snapshot["top30_cited"] = _top_cited(cur)
    snapshot["in_degree_buckets"] = _in_degree_buckets(cur)
    snapshot["match_types"] = _match_types(cur)
    snapshot["cross_lang_matrix"] = _cross_lang_matrix(cur)

    conn.close()
    json_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False))
    return {
        "rg_decisions": snapshot["rg_decisions"],
        "rg_citation_edges": snapshot["rg_citation_edges"],
        "rg_resolved_citations": snapshot["rg_resolved_citations"],
        "resolution_pct": (
            100.0 * snapshot["rg_resolved_citations"] / snapshot["rg_citation_edges"]
        ),
        "match_types": snapshot["match_types"],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--graph",
        default="/opt/caselaw/repo/output/reference_graph.db",
        help="Path to reference_graph.db",
    )
    ap.add_argument(
        "--fts",
        default="/opt/caselaw/repo/output/decisions.db",
        help="Path to FTS5 decisions.db (for total_decisions / total_courts)",
    )
    ap.add_argument(
        "--json",
        default=str(
            Path(__file__).resolve().parents[1]
            / "tables"
            / "corpus_graph_stats.json"
        ),
        help="Path to corpus_graph_stats.json",
    )
    args = ap.parse_args()

    summary = refresh(Path(args.json), Path(args.graph), args.fts)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

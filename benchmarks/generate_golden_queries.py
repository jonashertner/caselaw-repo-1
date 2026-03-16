#!/usr/bin/env python3
"""Generate candidate golden set queries from citation graph.

For each legal domain (statute article, court type), finds top-cited
decisions and generates candidate search queries from their regeste text.

Output: JSON with candidate queries for manual curation.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path

# Legal domains to expand — targeting weak tags in current benchmark
STATUTE_DOMAINS = [
    # Tenancy (MRR=0.148)
    ("ART.271.OR", "tenancy", "Anfechtung der Kündigung"),
    ("ART.261.OR", "tenancy", "Veräusserung der Mietsache"),
    ("ART.269.OR", "tenancy", "Missbräuchliche Mietzinse"),
    ("ART.269a.OR", "tenancy", "Mietzinserhöhung"),
    # Tax (MRR=0.083)
    ("ART.16.DBG", "tax", "Einkommenssteuer"),
    ("ART.127.BV", "tax", "Grundsätze der Besteuerung"),
    ("ART.190.DBG", "tax", "Steuerhinterziehung"),
    # Human rights / public law
    ("ART.10.BV", "constitutional", "Recht auf Leben und persönliche Freiheit"),
    ("ART.29.BV", "constitutional", "Rechtliches Gehör"),
    # Employment (MRR=0.240)
    ("ART.337.OR", "employment", "Fristlose Kündigung"),
    ("ART.328.OR", "employment", "Schutz der Persönlichkeit im Arbeitsverhältnis"),
    # Insurance
    ("ART.6.UVG", "insurance", "Unfallbegriff"),
    # Family
    ("ART.133.ZGB", "family", "Kindesunterhalt"),
    ("ART.176.ZGB", "family", "Eheschutz"),
    # Property
    ("ART.641.ZGB", "property", "Eigentum"),
    ("ART.679.ZGB", "property", "Nachbarrecht"),
    # Liability
    ("ART.42.OR", "liability", "Schadensbeweis"),
    ("ART.58.SVG", "liability", "Haftung des Motorfahrzeughalters"),
]


def find_top_decisions_by_statute(
    graph_conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
    statute_id: str,
    limit: int = 8,
) -> list[dict]:
    """Find top-cited decisions for a given statute article."""
    rows = graph_conn.execute(
        """
        SELECT ds.decision_id,
               SUM(ds.mention_count) AS mentions,
               COALESCE(
                   (SELECT COUNT(DISTINCT ct.source_decision_id)
                    FROM citation_targets ct
                    WHERE ct.target_decision_id = ds.decision_id), 0
               ) AS citations
        FROM decision_statutes ds
        WHERE ds.statute_id = ?
        GROUP BY ds.decision_id
        ORDER BY citations DESC
        LIMIT ?
        """,
        (statute_id, limit),
    ).fetchall()

    results = []
    for r in rows:
        fts_row = fts_conn.execute(
            "SELECT regeste, language, docket_number, court FROM decisions WHERE decision_id = ?",
            (r[0],),
        ).fetchone()
        if fts_row and fts_row[0] and len(fts_row[0]) >= 50:
            results.append({
                "decision_id": r[0],
                "docket_number": fts_row[2] or "",
                "court": fts_row[3] or "",
                "citations": r[2],
                "mentions": r[1],
                "language": fts_row[1] or "",
                "regeste": fts_row[0][:400],
            })
    return results


def extract_query_terms(regeste: str) -> str:
    """Extract key legal terms from regeste for query generation."""
    # Remove "Regeste\n" prefix and "Regesto" / "Regeste" headers
    text = re.sub(r"^(Regeste|Regesto|Résumé)\s*\n?\s*", "", regeste)
    # Take first sentence
    first_sentence = re.split(r"[.;]\s", text)[0]
    # Remove article references for a cleaner query hint
    cleaned = re.sub(
        r"Art\.?\s*\d+\w?\s*(Abs\.?\s*\d+\s*)?(lit\.?\s*\w\s*)?[A-Z]{2,}\d*/?[A-Z]*\s*[;,]?\s*",
        "",
        first_sentence,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:100].strip()


def main():
    parser = argparse.ArgumentParser(description="Generate golden set expansion candidates")
    parser.add_argument("--graph-db", type=Path, required=True)
    parser.add_argument("--fts-db", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/golden_candidates.json"))
    parser.add_argument("--top-n", type=int, default=3, help="Top N decisions per statute")
    args = parser.parse_args()

    graph_conn = sqlite3.connect(str(args.graph_db))
    fts_conn = sqlite3.connect(str(args.fts_db))

    candidates = []
    for statute_id, tag, description in STATUTE_DOMAINS:
        decisions = find_top_decisions_by_statute(
            graph_conn, fts_conn, statute_id, limit=args.top_n * 2,
        )
        top = decisions[:args.top_n]
        if not top:
            print(f"  SKIP {statute_id} — no decisions with regeste")
            continue

        # Generate one candidate query per statute
        best = top[0]
        query_hint = extract_query_terms(best["regeste"])

        candidates.append({
            "statute": statute_id,
            "tag": tag,
            "description": description,
            "suggested_query": f"{statute_id.replace('ART.','Art. ').replace('.',' ',1)} {query_hint}".strip(),
            "relevant_decisions": [
                {
                    "decision_id": d["decision_id"],
                    "docket_number": d["docket_number"],
                    "citations": d["citations"],
                    "language": d["language"],
                    "grade": 3 if i == 0 else 2,
                    "regeste_excerpt": d["regeste"][:200],
                }
                for i, d in enumerate(top)
            ],
            "status": "candidate",
        })
        print(f"  {statute_id:20s} → {len(top)} decisions (top: {best['docket_number']}, {best['citations']} citations)")

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump({"candidates": candidates, "total": len(candidates)}, f, indent=2, ensure_ascii=False)

    print(f"\nGenerated {len(candidates)} candidate queries → {args.output}")
    graph_conn.close()
    fts_conn.close()


if __name__ == "__main__":
    main()

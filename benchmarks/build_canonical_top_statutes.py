"""Compute alias-canonicalized top-statutes for paper §4 Table 6.

Background. The Swiss federal statutes mostly come with three official
language abbreviations (e.g. German *BGG*, French *LTF*, Italian *LTF*
all designate the same Federal Tribunal Act, SR 173.110). The raw
`statutes` table in `reference_graph.db` carries one row per
abbreviation, so an aggregation by (law_code, article) double-counts
the same provision across languages — and the v1.0 paper's Table 6
explicitly shows this (LTF Art. 42 and BGG Art. 42 as separate rows).

This script:

1. Defines an explicit alias table covering the 20+ major Swiss
   federal statutes whose multilingual abbreviations the v1.0 top-200
   table contained.
2. Re-aggregates with COUNT(DISTINCT decision_id) under an in-SQL
   CASE WHEN to collapse the alias variants — so a decision citing
   both BGG Art. 42 and LTF Art. 42 in the same document is counted
   once, not twice.
3. Writes top-N canonicalized rows to JSON, ready for
   `build_tables.py` to render as paper Table 6.

OG (Bundesrechtspflegegesetz, repealed 2007 in favour of BGG) is
deliberately kept separate — that's a temporal-validity question and
belongs in roadmap item 5, not in this alias collapsing.

Usage on the VPS:

    python3 -m benchmarks.build_canonical_top_statutes \\
        --graph /opt/caselaw/repo/output/reference_graph.db \\
        --out   docs/paper/v3/tables/top_statutes_canonical.json \\
        --top   30
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Optional


# Each tuple: (display name, list of variants seen in raw data).
# Display name shows multilingual identity to the reader; variants
# include both proper-cased and the all-caps artefacts the upstream
# scraper sometimes emits (VWVG, STGB, STPO, AUG, LASI). OG is NOT
# included — it's a different statute (pre-2007 Bundesrechtspflege),
# superseded by BGG/LTF; the canonicalization question for OG is
# temporal, not lexical.
ALIAS_GROUPS: list[tuple[str, list[str]]] = [
    ("BGG/LTF",          ["BGG", "LTF"]),
    ("BV/Cst./Cost.",    ["BV", "CST", "Cost"]),
    ("ZGB/CC",           ["ZGB", "CC"]),
    ("OR/CO",            ["OR", "CO"]),
    ("StGB/CP",          ["STGB", "StGB", "CP"]),
    ("StPO/CPP",         ["STPO", "StPO", "CPP"]),
    ("ZPO/CPC",          ["ZPO", "CPC"]),
    ("VwVG/PA",          ["VWVG", "VwVG", "PA"]),
    ("ATSG/LPGA",        ["ATSG", "LPGA"]),
    ("AsylG/LAsi",       ["ASYLG", "AsylG", "LASI", "LAsi"]),
    ("AIG/LEI",          ["AIG", "AUG", "AuG", "LEI"]),
    ("UVG/LAA",          ["UVG", "LAA", "LAINF"]),
    ("IVG/LAI",          ["IVG", "LAI"]),
    ("AHVG/LAVS",        ["AHVG", "LAVS"]),
    ("AVIG/LACI",        ["AVIG", "LACI", "LADI"]),
    ("KVG/LAMal",        ["KVG", "LAMal"]),
    ("EMRK/CEDH",        ["EMRK", "CEDH", "CEDU"]),
    ("SchKG/LP",         ["SchKG", "LP", "LEF"]),
    ("BVG/LPP",          ["BVG", "LPP"]),
    ("VGG/LTAF",         ["VGG", "LTAF"]),
]


def _sql_case_expression() -> str:
    """Build a SQL CASE expression that maps any known alias to its
    canonical display name; unknown codes pass through unchanged.
    Produces something like:

        CASE
          WHEN s.law_code IN ('BGG','LTF') THEN 'BGG/LTF'
          WHEN s.law_code IN (...) THEN '...'
          ELSE s.law_code
        END
    """
    parts = ["CASE"]
    for display, variants in ALIAS_GROUPS:
        in_list = ",".join(f"'{v}'" for v in variants)
        parts.append(f"  WHEN s.law_code IN ({in_list}) THEN '{display}'")
    parts.append("  ELSE s.law_code")
    parts.append("END")
    return "\n".join(parts)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--graph", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--top", type=int, default=30)
    args = p.parse_args(argv)

    case_expr = _sql_case_expression()
    sql = f"""
        SELECT
          ({case_expr}) AS canonical_law,
          s.article AS article,
          COUNT(DISTINCT ds.decision_id) AS n
        FROM decision_statutes ds
        JOIN statutes s ON ds.statute_id = s.statute_id
        WHERE s.law_code IS NOT NULL AND s.article IS NOT NULL
        GROUP BY canonical_law, s.article
        ORDER BY n DESC
        LIMIT {args.top}
    """

    print("  running alias-canonicalised aggregation...", file=sys.stderr)
    g = sqlite3.connect(f"file:{args.graph}?mode=ro", uri=True)
    rows = g.execute(sql).fetchall()
    g.close()

    out_rows = [
        {"law_code": law, "article": art, "n": n}
        for (law, art, n) in rows
    ]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({
        "alias_groups": [
            {"display": d, "variants": v} for (d, v) in ALIAS_GROUPS
        ],
        "schema_version": "canonical_top_statutes/v1",
        "n_alias_groups": len(ALIAS_GROUPS),
        "top": args.top,
        "rows": out_rows,
    }, indent=2))

    print(f"  wrote top {len(out_rows)} canonical rows to {args.out}")
    print()
    print(f"  {'rank':>4}  {'law':<18s}  {'article':<8s}  {'n':>10s}")
    for i, r in enumerate(out_rows[:15], 1):
        print(f"  {i:>4}  {r['law_code']:<18s}  Art. {r['article']:<6s}  {r['n']:>10,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

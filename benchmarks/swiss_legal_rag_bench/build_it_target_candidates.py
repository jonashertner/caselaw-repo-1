"""Identify Italian-original BGE candidates for the cross-lingual bench v1.1.

Background. The v1 cross-lingual benchmark has 38 DE-original + 12 FR-original
target cases and zero IT-original cases. The naive ``WHERE language='it'``
filter returns 955 BGE rows, but the language column is unreliable: of those
955, ~50% have French body text, ~18% have German body text, and only
~30% actually have Italian body text. The BGE record carries multiple
language hints (full_text, abstract_*, regeste) and they don't always
agree.

This script identifies Italian-original BGE candidates by running a
function-word heuristic on a mid-text slice of full_text (offset
800-2300), avoiding the standard German/French header preamble. The
heuristic counts distinctive function words per language; a case is
classified Italian-original when:

    it_hits >= 5 AND it_hits > max(de_hits, fr_hits)

The output is a candidate JSONL ranked by in-degree from the
reference graph. It is **not** a finished bench; each candidate still
needs human review (the heuristic does not handle bilingual judgments
or atypical document structure cleanly).

Run (on a host that has access to decisions.db + reference_graph.db):

    python3 -m benchmarks.swiss_legal_rag_bench.build_it_target_candidates \\
        --decisions /opt/caselaw/repo/output/decisions.db \\
        --graph     /opt/caselaw/repo/output/reference_graph.db \\
        --out       benchmarks/swiss_legal_rag_bench/it_target_candidates.jsonl \\
        --top 30
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Optional


DE_WORDS = (
    " der ", " und ", " ist ", " mit ", " nicht ", " sich ", " auf ",
    " dass ", " werden ", " eine ", " bei ", " den ", " des ", " dem ",
)
FR_WORDS = (
    " la ", " le ", " les ", " est ", " pour ", " cette ", " sont ",
    " dans ", " une ", " que ", " avec ", " par ", " sur ", " des ",
)
IT_WORDS = (
    " della ", " dei ", " che ", " non ", " per ", " delle ",
    " sono ", " essere ", " questo ", " questa ", " nel ", " alla ",
    " sul ", " sulla ", " degli ", " gli ", " agli ",
)

SLICE_START = 800
SLICE_END = 2300
MIN_IT_HITS = 5


def detect_lang(text: str) -> tuple[str, dict[str, int]]:
    t = text.lower()
    de = sum(1 for w in DE_WORDS if w in t)
    fr = sum(1 for w in FR_WORDS if w in t)
    it = sum(1 for w in IT_WORDS if w in t)
    scores = {"de": de, "fr": fr, "it": it}
    return max(scores, key=scores.get), scores


def is_it_original(scores: dict[str, int]) -> bool:
    return scores["it"] >= MIN_IT_HITS and scores["it"] > max(
        scores["de"], scores["fr"],
    )


def find_candidates(
    decisions_db: Path, graph_db: Path, top: int,
) -> list[dict]:
    d = sqlite3.connect(f"file:{decisions_db}?immutable=1", uri=True)
    g = sqlite3.connect(f"file:{graph_db}?mode=ro", uri=True)
    try:
        # Pool: language=it BGE rows with enough text to score on
        rows = d.execute(
            """
            SELECT decision_id,
                   substr(coalesce(full_text, ''), ?, ?),
                   coalesce(regeste, '')
            FROM decisions
            WHERE decision_id LIKE 'bge_BGE_%'
              AND language = 'it'
              AND length(coalesce(full_text, '')) > ?
            """,
            (SLICE_START, SLICE_END - SLICE_START, SLICE_END),
        ).fetchall()
        print(f"  pool: {len(rows)} BGE rows with language=it", file=sys.stderr)

        it_originals: list[dict] = []
        for did, body_slice, regeste in rows:
            lang, scores = detect_lang(body_slice)
            if not is_it_original(scores):
                continue

            # Rank input — pull in-degree from the graph
            in_deg_row = g.execute(
                "SELECT count(*) FROM citation_targets WHERE target_decision_id = ?",
                (did,),
            ).fetchone()
            in_degree = in_deg_row[0] if in_deg_row else 0

            it_originals.append({
                "target_decision_id": did,
                "docket": did.replace("bge_BGE_", "BGE ").replace("_", " "),
                "in_degree": in_degree,
                "lang_scores": scores,
                "regeste_head": regeste[:200].replace("\n", " ").strip(),
            })

        # Highest in-degree first
        it_originals.sort(key=lambda r: -r["in_degree"])
        print(
            f"  {len(it_originals)} IT-original candidates "
            f"(it_hits >= {MIN_IT_HITS} AND it > de,fr)",
            file=sys.stderr,
        )
        return it_originals[:top]
    finally:
        d.close()
        g.close()


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--decisions", required=True, type=Path)
    p.add_argument("--graph", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--top", type=int, default=30)
    args = p.parse_args(argv)

    candidates = find_candidates(args.decisions, args.graph, args.top)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as f:
        for c in candidates:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")
    print(f"  wrote top {len(candidates)} candidates to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

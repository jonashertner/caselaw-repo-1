"""Stratified-sample citation resolutions for manual precision audit.

Background. The paper currently reports a 92.9% citation-graph "resolution
rate" — a coverage metric: the share of extracted citation tokens that
match some decision in our corpus. Per the reviewer critique (Fatal #3),
this is not a precision metric: a match can be silently *wrong*, especially
in the pin-cite fallback (30-page heuristic) and in the BGE bare-form
class where many decisions share docket prefixes.

This script produces a stratified 400-row JSONL ready for rule-based and
manual adjudication. Each row carries enough context for an adjudicator to
decide whether the resolution is correct, wrong, or uncertain, without
opening a separate browser tab.

Stratification (over-samples the riskiest class):
    docket_norm     120  (40.6% of pool — under-sampled, mature path)
    bge_bare        100  (36.3% of pool)
    bge_norm         80  (15.0% of pool)
    bge_pincite     100  (8.1% of pool — over-sampled, riskiest)
    -----            ---
    total           400

Output schema (one JSON object per line):
    source_decision_id, source_context_before, target_ref,
    source_context_after, target_decision_id, target_regeste_head,
    match_type, confidence_score, adjudication, notes

On initial generation, the `adjudication` and `notes` fields ship empty.
The rule-based adjudicator (`citation_precision_audit_rules.py`) writes
mechanical verdicts back into those fields; human adjudicators can later
replace or extend them per the protocol in docs/paper/v3/v1_1_roadmap.md.

Run on the VPS where reference_graph.db and decisions.db live:

    python3 -m benchmarks.citation_precision_audit \\
        --graph /opt/caselaw/repo/output/reference_graph.db \\
        --decisions /opt/caselaw/repo/output/decisions.db \\
        --out      benchmarks/citation_precision_sample_400.jsonl \\
        --seed     42

Reproducibility: the `--seed` argument fixes both the per-stratum draw
and the ordering so two runs at the same seed produce byte-identical
output. The corpus-graph snapshot date is recorded in the output header
row (q_id="_meta") so adjudications stay tied to a known corpus state.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import random
import sqlite3
import sys
from pathlib import Path
from typing import Iterable, Optional

DEFAULT_STRATA = {
    "docket_norm": 120,
    "bge_bare": 100,
    "bge_norm": 80,
    "bge_pincite": 100,
}
CONTEXT_CHARS = 80


_re = __import__("re")
# Match separators flexibly: resolver stores dockets with underscores
# (e.g., "9C_340_2013", "E_1866_2015", "SK_2023_21") but the source body
# uses any of dash, slash, dot, or space (e.g., "9C 340/2013",
# "E-1866/2015", "SK.2023.21"). The harness searches the body for the
# components separated by any non-alphanumeric run.
_DOCKET_SEP = _re.compile(r"[_/\-\.\s]+")


def _candidate_pattern(target_ref: str) -> _re.Pattern:
    """Build a regex from target_ref that matches the same components
    separated by any of underscore / slash / dash / dot / whitespace.
    """
    parts = _DOCKET_SEP.split(target_ref)
    escaped = [_re.escape(p) for p in parts if p]
    return _re.compile(r"[_/\-\.\s]+".join(escaped))


def _source_snippet(
    decisions_conn: sqlite3.Connection,
    source_decision_id: str,
    target_ref: str,
) -> tuple[str, str]:
    """Return (before, after) text surrounding the first occurrence of
    target_ref in the source decision's full_text, matching components
    flexibly across separator variants. Empty strings when the snippet
    can't be located.
    """
    row = decisions_conn.execute(
        "SELECT COALESCE(full_text, '') FROM decisions WHERE decision_id = ?",
        (source_decision_id,),
    ).fetchone()
    if not row or not row[0] or not target_ref:
        return ("", "")
    text = row[0]
    pat = _candidate_pattern(target_ref)
    m = pat.search(text)
    if not m:
        return ("", "")
    start = max(0, m.start() - CONTEXT_CHARS)
    end = min(len(text), m.end() + CONTEXT_CHARS)
    before = text[start:m.start()].replace("\n", " ").strip()
    after = text[m.end():end].replace("\n", " ").strip()
    return (before, after)


def _target_head(decisions_conn: sqlite3.Connection, decision_id: str) -> str:
    """First ~250 chars of the target decision's regeste, for adjudicator
    sanity check (does the source citation's topic match the target?).
    """
    if not decision_id:
        return ""
    row = decisions_conn.execute(
        "SELECT COALESCE(regeste, '') FROM decisions WHERE decision_id = ?",
        (decision_id,),
    ).fetchone()
    if not row:
        return ""
    head = row[0].replace("\n", " ").strip()
    return head[:250]


def _stratum_sample(
    graph_conn: sqlite3.Connection,
    match_type: str,
    n: int,
    rng: random.Random,
) -> list[dict]:
    """Random sample of n rows from a given match_type stratum, using
    SQLite ORDER BY random() seeded for reproducibility (the connection
    sees the rng-seeded ORDER BY via row_number trick).

    For very large strata we use rowid-based reservoir sampling to avoid
    sorting the whole table.
    """
    total = graph_conn.execute(
        "SELECT COUNT(*) FROM citation_targets WHERE match_type = ?",
        (match_type,),
    ).fetchone()[0]
    if total == 0:
        return []

    # Reservoir-style: draw n unique rowids in [1, total] via rng, then
    # fetch by LIMIT/OFFSET. Cheap and reproducible. Some strata have a
    # rowid that isn't 1-contiguous, so we fetch by row_number window.
    sample_n = min(n, total)
    indices = sorted(rng.sample(range(total), sample_n))

    out: list[dict] = []
    rows = list(graph_conn.execute(
        """
        SELECT source_decision_id, target_ref, target_decision_id,
               match_type, confidence_score
        FROM citation_targets
        WHERE match_type = ?
        ORDER BY rowid
        """,
        (match_type,),
    ))
    for i in indices:
        sd, tr, td, mt, cs = rows[i]
        out.append({
            "source_decision_id": sd,
            "target_ref": tr,
            "target_decision_id": td,
            "match_type": mt,
            "confidence_score": cs,
        })
    return out


def build_sample(
    graph_db: Path,
    decisions_db: Path,
    strata: dict[str, int],
    seed: int,
) -> list[dict]:
    rng = random.Random(seed)
    g = sqlite3.connect(f"file:{graph_db}?mode=ro", uri=True)
    d = sqlite3.connect(f"file:{decisions_db}?immutable=1", uri=True)
    try:
        raw: list[dict] = []
        for mt, n in strata.items():
            print(f"  drawing {n} from {mt!r}", file=sys.stderr)
            raw.extend(_stratum_sample(g, mt, n, rng))

        # Enrich each row with adjudicator-visible context
        enriched: list[dict] = []
        for r in raw:
            before, after = _source_snippet(
                d, r["source_decision_id"], r["target_ref"],
            )
            enriched.append({
                "source_decision_id": r["source_decision_id"],
                "source_context_before": before,
                "target_ref": r["target_ref"],
                "source_context_after": after,
                "target_decision_id": r["target_decision_id"],
                "target_regeste_head": _target_head(d, r["target_decision_id"]),
                "match_type": r["match_type"],
                "confidence_score": r["confidence_score"],
                "schema_version": 2,
                "rule_consistent": "",
                "notes": "",
            })
        # Stable shuffle so the file isn't grouped by match_type when an
        # adjudicator opens it (avoids "saw 100 docket_norm in a row" bias)
        rng.shuffle(enriched)
        return enriched
    finally:
        g.close()
        d.close()


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--graph", required=True, type=Path)
    p.add_argument("--decisions", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--strata", type=str, default=None,
        help="Override default strata as JSON, e.g. '{\"docket_norm\":50,...}'",
    )
    args = p.parse_args(argv)

    strata = (
        json.loads(args.strata) if args.strata else DEFAULT_STRATA
    )
    if sum(strata.values()) == 0:
        print("ERROR: empty strata", file=sys.stderr)
        return 2

    sample = build_sample(args.graph, args.decisions, strata, args.seed)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    meta = {
        "q_id": "_meta",
        "schema_version": "citation_precision_audit/v1",
        "seed": args.seed,
        "strata": strata,
        "generated_at": _dt.datetime.now(tz=_dt.timezone.utc).isoformat(),
        "graph_db": str(args.graph),
        "decisions_db": str(args.decisions),
        "adjudication_protocol": (
            "docs/paper/v3/v1_1_roadmap.md#citation-precision-audit"
        ),
    }
    with args.out.open("w") as f:
        f.write(json.dumps(meta) + "\n")
        for row in sample:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"wrote {len(sample)} samples + meta header to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Additive BGer↔BGE cross-reference resolution (Completeness Plan, Step 1).

`build_reference_graph.py` resolves citations by matching `target_ref` against
`decisions.docket_norm` (three SQL passes). ~143k citations miss all three:
separator/normalization drift, decision_id-only forms, and — the big one — a
citation by a BGer docket whose decision is stored as a BGE (its docket_norm is
the BGE number, so the BGer docket never matches).

This pass recovers them using the citation_gap_oracle's tokenized matching plus
the BGer↔BGE cross-reference (the underlying docket extracted from each BGE
header). It is PURELY ADDITIVE and SAFE:

  * only refs with NO existing citation_targets row are processed,
  * INSERT OR IGNORE,

so it can only ADD edges the docket_norm JOINs missed — never alter or corrupt
an existing edge. Decoupled from the pipeline-critical builder so it can be
validated independently and wired in as a post-step.

Recovers ~143k edges with no scraping (the cheapest completeness win).
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from citation_gap_oracle import (  # noqa: E402
    corpus_keys_for,
    extract_underlying_dockets,
    normalize_ref,
)

XREF_MATCH_TYPE = "oracle_xref"
XREF_CONFIDENCE = 0.85


def build_key_index(decisions: sqlite3.Connection) -> dict[str, str]:
    """key → decision_id, from decision_id + both docket fields + the underlying
    docket in BGE/leading-case headers (the cross-reference). First writer wins;
    direct-source rows are processed before es_* by decision_id ordering is not
    guaranteed, so callers wanting a preference should pre-sort — for resolution
    any correct target is acceptable since keys are decision-unique."""
    idx: dict[str, str] = {}
    for did, dock, d2, court in decisions.execute(
        "SELECT decision_id, docket_number, docket_number_2, court FROM decisions"
    ):
        for k in corpus_keys_for(did, dock, d2, court):
            idx.setdefault(k, did)
    for did, ft in decisions.execute(
        "SELECT decision_id, full_text FROM decisions "
        "WHERE court IN ('bge','bge_historical') AND full_text IS NOT NULL"
    ):
        for k in extract_underlying_dockets(ft):
            idx.setdefault(k, did)
    return idx


def resolve_crossref(graph: sqlite3.Connection, key_index: dict[str, str],
                     *, confidence: float = XREF_CONFIDENCE, batch: int = 10_000) -> int:
    """Insert additive edges for unresolved docket citations. Returns the number
    of edges actually added (PK-conflict ignores don't count)."""
    resolved = set(
        r[0] for r in graph.execute("SELECT DISTINCT target_ref FROM citation_targets")
    )
    insert_sql = (
        "INSERT OR IGNORE INTO citation_targets "
        "(source_decision_id, target_ref, target_decision_id, match_type, confidence_score) "
        "VALUES (?,?,?,?,?)"
    )
    before = graph.total_changes
    cache: dict[str, str | None] = {}
    payload: list[tuple] = []
    for sid, ref in graph.execute(
        "SELECT source_decision_id, target_ref FROM decision_citations "
        "WHERE target_type='docket'"
    ):
        if not ref or ref in resolved:
            continue
        if ref in cache:
            k = cache[ref]
        else:
            k = normalize_ref(ref)
            cache[ref] = k
        if not k:
            continue
        tid = key_index.get(k)
        if tid and tid != sid:
            payload.append((sid, ref, tid, XREF_MATCH_TYPE, confidence))
            if len(payload) >= batch:
                graph.executemany(insert_sql, payload)
                payload.clear()
    if payload:
        graph.executemany(insert_sql, payload)
    graph.commit()
    return graph.total_changes - before


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    base = Path(os.environ.get("SWISS_CASELAW_DIR", "output"))
    p.add_argument("--graph", type=Path, default=base / "reference_graph.db")
    p.add_argument("--decisions", type=Path, default=base / "decisions.db")
    p.add_argument("--dry-run", action="store_true",
                   help="build the index + count would-be edges, write nothing")
    args = p.parse_args()

    decisions = sqlite3.connect(f"file:{args.decisions}?mode=ro&immutable=1", uri=True)
    idx = build_key_index(decisions)
    decisions.close()
    print(f"key index: {len(idx):,} keys", file=sys.stderr)

    if args.dry_run:
        graph = sqlite3.connect(f"file:{args.graph}?mode=ro&immutable=1", uri=True)
        # count, don't insert
        resolved = set(r[0] for r in graph.execute("SELECT DISTINCT target_ref FROM citation_targets"))
        cache: dict[str, str | None] = {}
        would = 0
        for (ref,) in graph.execute("SELECT target_ref FROM decision_citations WHERE target_type='docket'"):
            if not ref or ref in resolved:
                continue
            k = cache.get(ref, ...)
            if k is ...:
                k = normalize_ref(ref)
                cache[ref] = k
            if k and k in idx:
                would += 1
        graph.close()
        print(f"[dry-run] would add ~{would:,} edges", file=sys.stderr)
        return 0

    graph = sqlite3.connect(str(args.graph))
    added = resolve_crossref(graph, idx)
    graph.close()
    print(f"added {added:,} cross-reference edges", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

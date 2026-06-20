"""Unresolved-citation gap oracle (Completeness Plan, Pillar 1a).

Decisions in the corpus cite ~9.2M times; ~8.6M resolve to a corpus decision and
the rest are discarded at graph-build time. Those unresolved references are the
best "what are we missing, weighted by importance" signal we have. This script
recovers them, then CLASSIFIES each so the signal is actionable:

  * noise          — not a parseable decision reference (extraction garbage like
                     'URK_ 2' or 'COO_2207_105'). Ignore / fix extraction.
  * resolution_bug — a normalized form DOES match a corpus decision; the decision
                     exists, the resolver missed it (e.g. bare BGE 'vol div page'
                     like '123 V 419' with no 'BGE' prefix). Fix the resolver —
                     one fix recovers many edges.
  * missing        — a well-formed decision reference with no corpus match: a
                     genuine gap. Ranked by citation count → backfill priority.

Read-only against reference_graph.db + decisions.db; writes output/citation_gaps.db.
No change to the pipeline-critical graph builder.
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

DECISION_TYPES = {"docket", "bge", "decision", "bger", "bvger", "bstger"}

# Leading tokens that name a court/source, not part of the docket. Dropped so
# 'es_bger_2C_590_2013', 'BGE 123 V 419' and '2C_590/2013' all key the same.
_COURT_PREFIX_TOKENS = {"bge", "atf", "dtf", "bger", "tf", "es", "urteil",
                        "arret", "arrêt", "sentenza"}
_ROMAN = {"i", "ii", "iii", "iv", "v", "vi", "ia", "ib"}
# A BGer-style docket embedded in BGE/leading-case full text, e.g. '4A_576/2024'.
_UNDERLYING_DOCKET_RE = re.compile(r"\b(\d{1,2}[A-Za-z]{1,2}[._]\d{1,4}/\d{4})\b")


def _tokens(ref: str) -> list[str]:
    s = ref.replace("\n", " ").strip().lower()
    toks = [t for t in re.split(r"[\s._/\-]+", s) if t]
    while toks and toks[0] in _COURT_PREFIX_TOKENS:
        toks.pop(0)
    return toks


def normalize_ref(ref: str) -> str | None:
    """Separator-agnostic namespaced match key for a decision reference, or None
    if it is not a parseable decision reference (→ noise). Tokenizing on any
    separator means '5D 78/2017', '5D_78_2017' and '5D.78.2017' all collapse to
    one key, and the SAME keying applied to corpus dockets/ids means a key
    collision == 'the corpus has it'."""
    if not ref:
        return None
    toks = _tokens(ref)
    if len(toks) != 3:
        return None
    a, b, c = toks
    # BGE: volume (num) division (roman) page (num)
    if a.isdigit() and b in _ROMAN and c.isdigit():
        return f"bge:{int(a)}:{b}:{int(c)}"
    # BGer: chamber (digit + optional letters) num year(4)
    if re.fullmatch(r"\d{1,2}[a-z]{0,2}", a) and b.isdigit() and re.fullmatch(r"\d{4}", c):
        return f"d:{a}_{b}_{c}"
    # BVGer: single A–F letter num year(4)
    if re.fullmatch(r"[a-f]", a) and b.isdigit() and re.fullmatch(r"\d{4}", c):
        return f"d:{a}_{b}_{c}"
    # BStGer: two letters year(4) num
    if re.fullmatch(r"[a-z]{2}", a) and re.fullmatch(r"\d{4}", b) and c.isdigit():
        return f"d:{a}_{b}_{c}"
    # EVG social-insurance: single letter num year(2-4)
    if re.fullmatch(r"[a-z]", a) and b.isdigit() and re.fullmatch(r"\d{2,4}", c):
        return f"d:{a}_{b}_{c}"
    return None


def corpus_keys_for(decision_id: str | None = None, docket_number: str | None = None,
                    docket_number_2: str | None = None, court: str | None = None) -> set[str]:
    """All match keys a corpus decision contributes — from its docket(s) AND its
    decision_id (more uniform), so a decision present under any of those forms
    is found."""
    keys: set[str] = set()
    for v in (docket_number, docket_number_2, decision_id):
        if v:
            k = normalize_ref(v)
            if k:
                keys.add(k)
    return keys


def extract_underlying_dockets(full_text: str | None) -> list[str]:
    """Keys for the BGer docket(s) embedded in a BGE/leading-case header (the
    Urteilskopf names the underlying docket, e.g. '… 4A_576/2024 vom …'). This
    is the BGer↔BGE cross-reference: a citation by BGer docket then resolves to
    the BGE entry."""
    if not full_text:
        return []
    out: list[str] = []
    for m in _UNDERLYING_DOCKET_RE.findall(full_text[:600]):
        k = normalize_ref(m)
        if k:
            out.append(k)
    return out


def classify_ref(ref: str, target_type: str | None, corpus_keys: set[str]) -> str:
    """One of: 'noise' | 'resolution_bug' | 'missing'."""
    if target_type and target_type.lower() not in DECISION_TYPES:
        return "noise"
    key = normalize_ref(ref)
    if key is None:
        return "noise"
    if key in corpus_keys:
        return "resolution_bug"
    return "missing"


def load_resolved_refs(graph: sqlite3.Connection) -> set[str]:
    return set(
        r[0] for r in graph.execute(
            "SELECT DISTINCT target_ref FROM citation_targets "
            "WHERE target_decision_id IS NOT NULL AND target_decision_id<>''"
        )
    )


def load_corpus_keys(decisions: sqlite3.Connection) -> set[str]:
    keys: set[str] = set()
    # Identifier pass over ALL decisions (decision_id + both docket fields).
    for did, docket, d2, court in decisions.execute(
        "SELECT decision_id, docket_number, docket_number_2, court FROM decisions"
    ):
        keys |= corpus_keys_for(did, docket, d2, court)
    # BGer↔BGE cross-reference pass: extract underlying dockets from leading-case
    # headers so a citation by BGer docket resolves to its BGE entry.
    for (ft,) in decisions.execute(
        "SELECT full_text FROM decisions WHERE court IN ('bge','bge_historical') "
        "AND full_text IS NOT NULL"
    ):
        keys.update(extract_underlying_dockets(ft))
    return keys


def aggregate_unresolved(graph: sqlite3.Connection, resolved: set[str]):
    """{ target_ref: (citation_count, distinct_sources, target_type) } for refs
    that never resolved."""
    cnt: dict[str, int] = defaultdict(int)
    srcs: dict[str, set] = defaultdict(set)
    ttype: dict[str, str] = {}
    for sid, ref, tt in graph.execute(
        "SELECT source_decision_id, target_ref, target_type FROM decision_citations"
    ):
        if not ref or ref in resolved:
            continue
        cnt[ref] += 1
        if len(srcs[ref]) < 5000:
            srcs[ref].add(sid)
        ttype.setdefault(ref, tt)
    return {r: (cnt[r], len(srcs[r]), ttype.get(r)) for r in cnt}


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS citation_gaps (
    target_ref       TEXT PRIMARY KEY,
    target_type      TEXT,
    citation_count   INTEGER,
    distinct_sources INTEGER,
    classification   TEXT,        -- 'missing' | 'resolution_bug' | 'noise'
    normalized_key   TEXT
);
CREATE INDEX IF NOT EXISTS citation_gaps_class_idx ON citation_gaps(classification);
CREATE INDEX IF NOT EXISTS citation_gaps_count_idx ON citation_gaps(citation_count DESC);
"""


def build_gap_table(graph: sqlite3.Connection, decisions: sqlite3.Connection,
                    out: sqlite3.Connection) -> dict:
    out.executescript(SCHEMA_SQL)
    resolved = load_resolved_refs(graph)
    corpus = load_corpus_keys(decisions)
    agg = aggregate_unresolved(graph, resolved)

    summary = defaultdict(int)
    out.execute("DELETE FROM citation_gaps")
    for ref, (count, nsrc, tt) in agg.items():
        cls = classify_ref(ref, tt, corpus)
        summary[cls] += 1
        out.execute(
            "INSERT OR REPLACE INTO citation_gaps "
            "(target_ref, target_type, citation_count, distinct_sources, "
            " classification, normalized_key) VALUES (?,?,?,?,?,?)",
            (ref, tt, count, nsrc, cls, normalize_ref(ref)),
        )
    out.commit()
    return dict(summary)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    base = Path(os.environ.get("SWISS_CASELAW_DIR", "output"))
    p.add_argument("--graph", type=Path, default=base / "reference_graph.db")
    p.add_argument("--decisions", type=Path, default=base / "decisions.db")
    p.add_argument("--out", type=Path, default=base / "citation_gaps.db")
    p.add_argument("--top", type=int, default=30)
    args = p.parse_args()

    graph = sqlite3.connect(f"file:{args.graph}?mode=ro&immutable=1", uri=True)
    decisions = sqlite3.connect(f"file:{args.decisions}?mode=ro&immutable=1", uri=True)
    out = sqlite3.connect(str(args.out))
    try:
        summary = build_gap_table(graph, decisions, out)
        print("=== citation gap oracle ===", file=sys.stderr)
        for cls in ("missing", "resolution_bug", "noise"):
            print(f"  {cls:15} {summary.get(cls, 0):,}", file=sys.stderr)
        print(f"\n=== top {args.top} MISSING (by citation count) ===")
        for ref, c, tt in out.execute(
            "SELECT target_ref, citation_count, target_type FROM citation_gaps "
            "WHERE classification='missing' ORDER BY citation_count DESC LIMIT ?",
            (args.top,),
        ):
            print(f"  {c:>6,}  [{tt}]  {ref}")
    finally:
        graph.close()
        decisions.close()
        out.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

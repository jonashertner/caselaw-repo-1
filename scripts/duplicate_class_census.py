"""Duplicate-class census: how many stored records are the same decision?

Wave-1 section 4b. The recomputed canonical grouping (canonical_regrouping.py)
finds ~15k duplicate records, while the public README implies ~141k. The gap
is a class that court+docket+date grouping structurally cannot see: a Federal
Supreme Court decision published in the official reports is stored BOTH as a
bger record and as a bge record, under different courts and different docket
conventions.

This census quantifies each class separately, because the entity count depends
entirely on which classes are collapsed — the paper must report the taxonomy,
not a single "unique decisions" number.

  A  reporter identifier-form duality   bge_BGE_X vs bge_X
  B  bger <-> bge publication pairs     via the BGE header cross-reference
  C  cross-portal duplicates            same docket+date, different record

Read-only; writes one JSON. Uses the same cross-reference extractor the
oracle_xref resolution stratum uses, so class B is measured with the
mechanism already shipped rather than a new heuristic.

CORRECTION 2026-08-09: the first run keyed class C on citation_gap_oracle
.normalize_ref, which is a FEDERAL-CITATION-GRAMMAR normaliser and returns
None for anything outside it — by design, documented in its docstring.
Cantonal dockets (WBE.2025.125, ARGVP_1988_1001) are correctly rejected by
it, so 475,925 records fell into per-court None buckets and produced
fictitious groups of up to 1,170 unrelated decisions (reported figure
374,842 — an artifact, now discarded). Only 26 records in the whole corpus
actually lack a docket. Class C now uses the general docket normaliser
from the graph builder, and is reported both with and without the court
component, because genuine cross-portal duplicates carry DIFFERENT court
codes (direct scrape vs entscheidsuche adapter) and a court-keyed grouping
cannot see them at all.
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
OUT_DIR = os.environ.get("OCL_OUTPUT_DIR", "/mnt/HC_Volume_104655575/output")

_BGE_SHAPE = re.compile(
    r"^(?:BGE|ATF|DTF)?[\s_]*(\d{1,3})[\s_]+([IVX]{1,4}[ab]?)[\s_]+(\d{1,4})$",
    re.IGNORECASE)


def _log(m):
    print(f"[{datetime.datetime.now(datetime.UTC).strftime('%H:%M:%S')}] {m}",
          file=sys.stderr, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    from citation_gap_oracle import extract_underlying_dockets, normalize_ref

    dec = sqlite3.connect(
        f"file:{os.path.join(OUT_DIR, 'decisions.db')}?mode=ro&immutable=1",
        uri=True)
    dec.execute("PRAGMA busy_timeout=60000")

    _log("index: non-reporter records by normalised docket")
    by_docket: dict[str, list] = defaultdict(list)
    n = 0
    for did, court, dk in dec.execute(
            "SELECT decision_id, court, docket_number FROM decisions "
            "WHERE court NOT IN ('bge','bge_historical')"):
        n += 1
        k = normalize_ref(dk or "")
        if k:
            by_docket[k].append(did)
    _log(f"  {n:,} non-reporter records, {len(by_docket):,} distinct dockets")

    _log("class A: reporter identifier-form duality")
    ref_groups: dict[str, list] = defaultdict(list)
    for did, dk in dec.execute(
            "SELECT decision_id, docket_number FROM decisions "
            "WHERE court IN ('bge','bge_historical')"):
        m = _BGE_SHAPE.match((dk or "").strip())
        if m:
            ref_groups["{}|{}|{}".format(m.group(1), m.group(2).upper(),
                                         m.group(3))].append(did)
    class_a = {k: v for k, v in ref_groups.items() if len(v) > 1}
    _log(f"  {len(class_a):,} references with >1 record")

    _log("class B: bger <-> bge publication pairs (header cross-reference)")
    paired = 0
    pair_examples = []
    seen_bge = 0
    for did, ft in dec.execute(
            "SELECT decision_id, full_text FROM decisions "
            "WHERE court IN ('bge','bge_historical') AND full_text IS NOT NULL"):
        seen_bge += 1
        if seen_bge % 10000 == 0:
            _log(f"  {seen_bge:,} reporter records scanned")
        hit = None
        for k in extract_underlying_dockets(ft):
            if k in by_docket:
                hit = (k, by_docket[k][0])
                break
        if hit:
            paired += 1
            if len(pair_examples) < 12:
                pair_examples.append({"reporter_record": did,
                                      "docket_key": hit[0],
                                      "court_record": hit[1]})
    _log(f"  {paired:,} reporter records pair to a court record")

    _log("class C: cross-portal duplicates (general docket normaliser)")
    # _docket_norm is the shipped general normaliser (uppercase, unify
    # separators) — the right tool for grouping arbitrary dockets, unlike
    # normalize_ref which is federal-citation-grammar only. See the
    # CORRECTION note in the module docstring.
    sys.path.insert(0, os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))
    from search_stack.build_reference_graph import _docket_norm

    with_court: dict[str, list] = defaultdict(list)
    without_court: dict[str, list] = defaultdict(list)
    skipped_no_docket = 0
    for did, court, dk, date in dec.execute(
            "SELECT decision_id, court, docket_number, decision_date "
            "FROM decisions WHERE court NOT IN ('bge','bge_historical')"):
        k = _docket_norm(dk)
        if not k:
            skipped_no_docket += 1
            continue          # 26 records corpus-wide; never bucket them
        with_court["{}|{}|{}".format(court, k, date or "?")].append(did)
        without_court["{}|{}".format(k, date or "?")].append(did)
    class_c = {k: v for k, v in with_court.items() if len(v) > 1}
    class_c_xportal = {k: v for k, v in without_court.items() if len(v) > 1}
    c_sizes = Counter(len(v) for v in class_c_xportal.values())

    total = dec.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    out = {
        "_status": "rule-derived, pending human validation of each class",
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "records_total": total,
        "class_A_reporter_identifier_duality": {
            "groups": len(class_a),
            "records_involved": sum(len(v) for v in class_a.values()),
            "overcount": sum(len(v) for v in class_a.values()) - len(class_a),
        },
        "class_B_reporter_to_court_publication_pairs": {
            "reporter_records_scanned": seen_bge,
            "reporter_records_with_a_court_counterpart": paired,
            "overcount_if_collapsed": paired,
            "examples": pair_examples,
            "method": ("BGE header cross-reference, the same extractor the "
                       "oracle_xref resolution stratum uses"),
        },
        "class_C_same_court_docket_date": {
            "groups": len(class_c),
            "records_involved": sum(len(v) for v in class_c.values()),
            "overcount": sum(len(v) for v in class_c.values()) - len(class_c),
            "note": "same court code: within-portal duplicates only",
        },
        "class_C_cross_portal_docket_date": {
            "groups": len(class_c_xportal),
            "records_involved": sum(len(v) for v in class_c_xportal.values()),
            "overcount": (sum(len(v) for v in class_c_xportal.values())
                          - len(class_c_xportal)),
            "group_size_distribution": dict(sorted(c_sizes.items())[:10]),
            "records_without_a_docket_excluded": skipped_no_docket,
            "note": ("court code dropped, so direct-scrape and "
                     "entscheidsuche-adapter records of the same decision "
                     "can meet. Large groups here would indicate docket "
                     "reuse across courts and must be inspected, not "
                     "collapsed."),
        },
        "note": ("classes overlap: a record may belong to more than one. The "
                 "entity count depends on which classes are collapsed, which "
                 "is why the paper reports the taxonomy rather than a single "
                 "'unique decisions' figure."),
    }
    with open(a.out, "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    _log(f"wrote {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

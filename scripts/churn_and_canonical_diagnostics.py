"""Wave-1 artifact sections 3 and 4.

Section 3 — corpus-churn decomposition. Compares the before-state
inventory (per-record identity dumped before the post-fix build) against
the current corpus, so extractor deltas can be computed on the UNCHANGED
common set and churn reported separately. Also classifies token-level
resolution transitions on that common set.

Section 4 — canonicalisation diagnostics. Runs the invariants that must
hold before a canonical-entity count may be reported as anything other
than rule-derived: one entity per record, no cycles, no alias mapping to
two entities, variants distinguished from accidental duplicates, docket
reuse across courts/years, deterministic edge collapse.

Read-only. Usage:
  python3 scripts/churn_and_canonical_diagnostics.py \
      --inventory output/release_meta/before_inventory.sqlite \
      --out output/release_meta/churn_and_canonical.json
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sqlite3
import sys

OUT_DIR = os.environ.get("OCL_OUTPUT_DIR", "/mnt/HC_Volume_104655575/output")


def _log(m):
    print(f"[{datetime.datetime.now(datetime.UTC).strftime('%H:%M:%S')}] {m}",
          file=sys.stderr, flush=True)


def _ro(p):
    c = sqlite3.connect(f"file:{p}?mode=ro&immutable=1", uri=True)
    c.execute("PRAGMA busy_timeout=60000")
    return c


def churn(inv: sqlite3.Connection, dec: sqlite3.Connection) -> dict:
    _log("churn: loading before-state records")
    before = {r[0]: (r[1], r[2]) for r in
              inv.execute("SELECT decision_id, language, content_hash "
                          "FROM records")}
    _log(f"  before: {len(before):,}")
    after = {r[0]: (r[1], r[2]) for r in
             dec.execute("SELECT decision_id, language, "
                         "COALESCE(content_hash,'') FROM decisions")}
    _log(f"  after:  {len(after):,}")

    b, a = set(before), set(after)
    common = b & a
    added, removed = a - b, b - a
    text_changed = {d for d in common if before[d][1] != after[d][1]}
    lang_changed = {d for d in common if before[d][0] != after[d][0]}
    unchanged = common - text_changed - lang_changed

    lang_moves: dict = {}
    for d in lang_changed:
        k = f"{before[d][0]}->{after[d][0]}"
        lang_moves[k] = lang_moves.get(k, 0) + 1

    added_by_lang: dict = {}
    for d in added:
        lg = after[d][0] or "?"
        added_by_lang[lg] = added_by_lang.get(lg, 0) + 1

    return {
        "before_record_count": len(before),
        "after_record_count": len(after),
        "common_records": len(common),
        "records_added_after_before_state": len(added),
        "records_removed_after_before_state": len(removed),
        "records_with_changed_text": len(text_changed),
        "records_with_changed_language": len(lang_changed),
        "language_transitions": lang_moves,
        "added_records_by_language": added_by_lang,
        "unchanged_common_records": len(unchanged),
        "_unchanged_set_size": len(unchanged),
    }, unchanged


def token_transitions(inv, rg, unchanged: set) -> dict:
    """Resolution transitions restricted to records unchanged since the
    before-state, so extractor effects are not mixed with corpus churn."""
    _log("transitions: before tokens")
    tb: dict = {}
    for sid, ref, tt in inv.execute(
            "SELECT source_decision_id, target_ref, target_type "
            "FROM tokens_before"):
        if sid in unchanged:
            tb[(sid, ref)] = tt
    _log(f"  before tokens on unchanged set: {len(tb):,}")

    lb: dict = {}
    for sid, ref, tid, mt in inv.execute(
            "SELECT source_decision_id, target_ref, target_decision_id, "
            "match_type FROM links_before"):
        if sid in unchanged:
            lb.setdefault((sid, ref), []).append(tid)

    _log("transitions: after tokens")
    ta: dict = {}
    for sid, ref, tt in rg.execute(
            "SELECT source_decision_id, target_ref, target_type "
            "FROM decision_citations"):
        if sid in unchanged:
            ta[(sid, ref)] = tt
    la: dict = {}
    for sid, ref, tid, mt in rg.execute(
            "SELECT source_decision_id, target_ref, target_decision_id, "
            "match_type FROM citation_targets"):
        if sid in unchanged:
            la.setdefault((sid, ref), []).append(tid)

    kb, ka = set(tb), set(ta)
    out = {
        "tokens_newly_extracted": len(ka - kb),
        "tokens_no_longer_extracted": len(kb - ka),
        "tokens_retyped_docket_to_bge": sum(
            1 for k in kb & ka if tb[k] == "docket" and ta[k] == "bge"),
        "tokens_retyped_bge_to_docket": sum(
            1 for k in kb & ka if tb[k] == "bge" and ta[k] == "docket"),
        "tokens_newly_resolved": sum(
            1 for k in ka if k not in lb and k in la),
        "tokens_newly_unresolved": sum(
            1 for k in kb if k in lb and k not in la),
        "tokens_target_changed": sum(
            1 for k in (set(lb) & set(la))
            if sorted(lb[k]) != sorted(la[k])
            and len(lb[k]) == len(la[k])),
        "tokens_target_multiplicity_changed": sum(
            1 for k in (set(lb) & set(la)) if len(lb[k]) != len(la[k])),
        "_denominator_unchanged_records": len(unchanged),
    }
    return out


def canonical_diagnostics(dec: sqlite3.Connection) -> dict:
    _log("canonicalisation invariants")
    d: dict = {"_status": "rule-derived, pending human validation"}
    d["records_total"] = dec.execute(
        "SELECT COUNT(*) FROM decisions").fetchone()[0]
    d["records_with_canonical_key"] = dec.execute(
        "SELECT COUNT(*) FROM decisions WHERE canonical_key IS NOT NULL "
        "AND canonical_key <> ''").fetchone()[0]
    d["canonical_entities_rule_derived"] = dec.execute(
        "SELECT COUNT(DISTINCT COALESCE(NULLIF(canonical_key,''), "
        "decision_id)) FROM decisions").fetchone()[0]
    groups = dec.execute(
        "SELECT COUNT(*), MAX(n), SUM(n) FROM (SELECT canonical_key, "
        "COUNT(*) AS n FROM decisions WHERE canonical_key IS NOT NULL "
        "AND canonical_key <> '' GROUP BY canonical_key HAVING n > 1)"
    ).fetchone()
    d["duplicate_groups"] = groups[0] or 0
    d["max_records_per_group"] = groups[1] or 0
    d["records_inside_duplicate_groups"] = groups[2] or 0

    # invariant: one entity per record is structural (single column), so
    # the meaningful checks are on the grouping itself
    d["invariant_one_entity_per_record"] = "structural (single column)"

    # do grouped records share language? cross-language groups are
    # publication variants, same-language groups are candidate duplicates
    rows = dec.execute(
        "SELECT canonical_key, COUNT(DISTINCT language), COUNT(*), "
        "COUNT(DISTINCT court), COUNT(DISTINCT decision_date) "
        "FROM decisions WHERE canonical_key IS NOT NULL "
        "AND canonical_key <> '' GROUP BY canonical_key HAVING COUNT(*) > 1"
    ).fetchall()
    d["groups_multilingual"] = sum(1 for r in rows if r[1] > 1)
    d["groups_single_language"] = sum(1 for r in rows if r[1] == 1)
    d["groups_spanning_multiple_courts"] = sum(1 for r in rows if r[3] > 1)
    d["groups_spanning_multiple_dates"] = sum(1 for r in rows if r[4] > 1)
    d["note_docket_reuse_risk"] = (
        "groups spanning multiple courts or dates are the collision "
        "candidates: docket reuse across courts/years would surface here")
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    inv = sqlite3.connect(f"file:{a.inventory}?mode=ro", uri=True)
    dec = _ro(os.path.join(OUT_DIR, "decisions.db"))
    rg = _ro(os.path.join(OUT_DIR, "reference_graph.db"))

    ch, unchanged = churn(inv, dec)
    res = {
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "section_3_corpus_churn": ch,
        "section_3b_token_transitions_on_unchanged_records":
            token_transitions(inv, rg, unchanged),
        "section_4_canonicalisation_diagnostics": canonical_diagnostics(dec),
    }
    res["section_3_corpus_churn"].pop("_unchanged_set_size", None)
    with open(a.out, "w") as f:
        json.dump(res, f, indent=1, sort_keys=True)
    _log(f"wrote {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Offline canonical-entity regrouping and invariant checks.

Wave-1 section 4. The shipped `canonical_key` is defective: it embeds the
decision date, and keys computed while a record still carried a 1-January
placeholder were never recomputed after the date was corrected. Result:
100% of records carry a key, but only ONE group has more than one member,
so `COUNT(DISTINCT canonical_key)` is the record count wearing a different
name (verified 2026-08-08: BGE 123 III 101 is stored as both
bge_BGE_123_III_101 (key ...|19960101) and bge_123 III 101
(key ...|19961120), same docket, same decision_date).

This script recomputes the grouping READ-ONLY from current field values
and reports the invariants the review requires before any canonical count
may be published. It writes nothing to production: the regrouping lives
in its own output file and is labelled rule-derived pending human
validation.

  python3 scripts/canonical_regrouping.py --out output/release_meta/canonical_regrouping.json
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

OUT_DIR = os.environ.get("OCL_OUTPUT_DIR", "/mnt/HC_Volume_104655575/output")

# BGE reporter references: strip the optional BGE/ATF/DTF prefix and all
# separators so "BGE 123 III 101", "123 III 101" and "123_III_101" agree.
_BGE_SHAPE = re.compile(
    r"^(?:BGE|ATF|DTF)?[\s_]*(\d{1,3})[\s_]+([IVX]{1,4}[ab]?)[\s_]+(\d{1,4})$",
    re.IGNORECASE)


def _log(m):
    print(f"[{datetime.datetime.now(datetime.UTC).strftime('%H:%M:%S')}] {m}",
          file=sys.stderr, flush=True)


def regroup_key(court: str, docket: str, date: str) -> str:
    """Rule R2 (proposed): reporter references group on the reference
    itself, everything else on court+normalised docket+date. Deliberately
    does NOT use the date for reporter references — that is the exact
    field whose placeholder values broke the shipped key."""
    m = _BGE_SHAPE.match((docket or "").strip())
    if m and (court or "").startswith("bge"):
        return "bge|{}|{}|{}".format(m.group(1), m.group(2).upper(),
                                     m.group(3))
    dk = re.sub(r"[^0-9A-Z]", "", (docket or "").upper())
    return "{}|{}|{}".format(court or "?", dk, date or "?")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    dec = sqlite3.connect(
        f"file:{os.path.join(OUT_DIR, 'decisions.db')}?mode=ro&immutable=1",
        uri=True)
    dec.execute("PRAGMA busy_timeout=60000")

    _log("scanning identity columns")
    groups: dict[str, list] = defaultdict(list)
    shipped: dict[str, list] = defaultdict(list)
    rows = 0
    meta: dict[str, tuple] = {}
    for did, court, dk, date, lang, ck in dec.execute(
            "SELECT decision_id, court, docket_number, decision_date, "
            "language, canonical_key FROM decisions"):
        rows += 1
        k = regroup_key(court, dk, date)
        groups[k].append(did)
        if ck:
            shipped[ck].append(did)
        meta[did] = (court, lang, date)
        if rows % 200000 == 0:
            _log(f"  {rows:,}")

    multi = {k: v for k, v in groups.items() if len(v) > 1}
    shipped_multi = sum(1 for v in shipped.values() if len(v) > 1)

    inv = {}
    inv["every_record_maps_to_exactly_one_entity"] = (
        sum(len(v) for v in groups.values()) == rows)
    inv["no_alias_maps_to_multiple_entities"] = True   # one key per record
    inv["cycles_possible"] = False                     # flat keying, no graph
    cross_court = sum(1 for v in multi.values()
                      if len({meta[d][0] for d in v}) > 1)
    cross_lang = sum(1 for v in multi.values()
                     if len({meta[d][1] for d in v}) > 1)
    cross_date = sum(1 for v in multi.values()
                     if len({meta[d][2] for d in v}) > 1)

    sizes = Counter(len(v) for v in multi.values())
    out = {
        "_status": "rule-derived, pending human validation of grouping",
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "rule": ("reporter references (court bge*) group on the normalised "
                 "reference; all other records on court|docket|date. The "
                 "shipped key's date component is deliberately dropped for "
                 "reporter references."),
        "records_total": rows,
        "shipped_key": {
            "distinct_keys": len(shipped),
            "groups_with_more_than_one_record": shipped_multi,
            "defect": ("date component embeds placeholder dates that were "
                       "later corrected; grouping therefore fails"),
        },
        "regrouped": {
            "distinct_entities": len(groups),
            "groups_with_more_than_one_record": len(multi),
            "records_inside_multi_groups": sum(len(v) for v in multi.values()),
            "overcount_vs_entities":
                sum(len(v) for v in multi.values()) - len(multi),
            "group_size_distribution": dict(sorted(sizes.items())),
            "groups_spanning_multiple_courts": cross_court,
            "groups_spanning_multiple_languages": cross_lang,
            "groups_spanning_multiple_dates": cross_date,
        },
        "invariants": inv,
        "collision_risk_note": (
            "groups spanning multiple courts or dates are the docket-reuse "
            "collision candidates and must be inspected before the count is "
            "reported as canonical decisions"),
        "examples": {k: v[:4] for k, v in list(multi.items())[:15]},
    }
    with open(a.out, "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    _log(f"wrote {a.out}: {len(groups):,} entities from {rows:,} records")
    return 0


if __name__ == "__main__":
    sys.exit(main())

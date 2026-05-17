"""Automated precision proxies for the citation graph.

Computes empirical precision *proxies* for citation resolution without
manual adjudication. These are not a substitute for the per-stratum
manual audit (see citation_precision_audit.py + roadmap item 1, deferred
to v2.0), but they bound the worst case: every counted violation is a
guaranteed false positive.

Proxies computed (per match_type and overall):

  1. **Date sanity** — a source decision cannot cite a target decided
     *after* the source. Reported as the % of resolved pairs that
     pass the constraint when both dates are known. Failures are
     logical impossibilities, so the violation count is a hard floor
     on false-positive count.
  2. **Self-citation** — a decision shouldn't cite itself. Reported as
     count + % of pairs where source_decision_id == target_decision_id.
  3. **Date coverage** — % of pairs where source AND target decision
     dates are both known (i.e. the date-sanity check could even run).
  4. **Confidence distribution** — min / p10 / p25 / p50 / p75 / p90 /
     max of the resolver's confidence_score per match_type.

Output: a single JSON written to --out, plus a human-readable summary
to stdout.

Run on a host with both DBs:

    python3 -m benchmarks.citation_precision_proxies \\
        --graph     /opt/caselaw/repo/output/reference_graph.db \\
        --decisions /opt/caselaw/repo/output/decisions.db \\
        --out       benchmarks/citation_precision_proxies.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional


PERCENTILES = (0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 1.0)


def percentiles_of(values: list[float]) -> dict:
    if not values:
        return {f"p{int(p * 100)}": None for p in PERCENTILES}
    values = sorted(values)
    n = len(values)
    out = {}
    for p in PERCENTILES:
        idx = min(int(p * n), n - 1)
        out[f"p{int(p * 100)}"] = round(values[idx], 4)
    return out


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--graph", required=True, type=Path)
    p.add_argument("--decisions", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    args = p.parse_args(argv)

    # Load all decision dates once
    print("  loading decision dates...", file=sys.stderr)
    d = sqlite3.connect(f"file:{args.decisions}?immutable=1", uri=True)
    dates: dict[str, Optional[str]] = {}
    for did, dt in d.execute(
        "SELECT decision_id, decision_date FROM decisions"
    ):
        # Treat empty string as missing
        dates[did] = dt if dt else None
    d.close()
    print(f"  loaded {len(dates):,} dates", file=sys.stderr)

    # Stream citation_targets
    print("  streaming citation_targets...", file=sys.stderr)
    g = sqlite3.connect(f"file:{args.graph}?mode=ro", uri=True)
    cursor = g.execute(
        """
        SELECT source_decision_id, target_decision_id, match_type,
               confidence_score
        FROM citation_targets
        WHERE target_decision_id IS NOT NULL
        """
    )

    stats = defaultdict(lambda: {
        "total": 0,
        "date_violations": 0,         # target_date > source_date
        "self_citations": 0,          # src == tgt
        "both_dates_known": 0,
        "missing_source_date": 0,
        "missing_target_date": 0,
        "confidence_values": [],      # cap per stratum to 250k for memory
    })

    CONF_CAP = 250_000

    for src, tgt, mt, conf in cursor:
        s = stats[mt]
        s["total"] += 1
        if src == tgt:
            s["self_citations"] += 1
        sd = dates.get(src)
        td = dates.get(tgt)
        if not sd:
            s["missing_source_date"] += 1
        if not td:
            s["missing_target_date"] += 1
        if sd and td:
            s["both_dates_known"] += 1
            # String compare works for ISO YYYY-MM-DD dates
            if td > sd:
                s["date_violations"] += 1
        if conf is not None and len(s["confidence_values"]) < CONF_CAP:
            s["confidence_values"].append(float(conf))
    g.close()

    # Build output
    out: dict = {"by_match_type": {}, "overall": {}}
    overall_total = 0
    overall_date_viol = 0
    overall_self = 0
    overall_both_dates = 0

    for mt, s in stats.items():
        tot = s["total"]
        both = s["both_dates_known"]
        out["by_match_type"][mt] = {
            "total": tot,
            "date_coverage_pct": round(100 * both / tot, 2) if tot else None,
            "date_sanity_pass_pct": (
                round(100 * (1 - s["date_violations"] / both), 4)
                if both else None
            ),
            "date_violations": s["date_violations"],
            "self_citations": s["self_citations"],
            "self_cite_violation_pct": (
                round(100 * s["self_citations"] / tot, 6) if tot else None
            ),
            "missing_source_date": s["missing_source_date"],
            "missing_target_date": s["missing_target_date"],
            "confidence_percentiles": percentiles_of(s["confidence_values"]),
        }
        overall_total += tot
        overall_date_viol += s["date_violations"]
        overall_self += s["self_citations"]
        overall_both_dates += both

    out["overall"] = {
        "total": overall_total,
        "date_coverage_pct": (
            round(100 * overall_both_dates / overall_total, 2)
            if overall_total else None
        ),
        "date_sanity_pass_pct": (
            round(100 * (1 - overall_date_viol / overall_both_dates), 4)
            if overall_both_dates else None
        ),
        "date_violations": overall_date_viol,
        "self_citations": overall_self,
        "self_cite_violation_pct": (
            round(100 * overall_self / overall_total, 6)
            if overall_total else None
        ),
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=2))

    # Summary
    print()
    print(f"  total citation pairs analysed: {overall_total:,}")
    print(f"  date coverage (both known):    {out['overall']['date_coverage_pct']}%")
    print(f"  date sanity pass:              {out['overall']['date_sanity_pass_pct']}%")
    print(f"  self-citation violations:      {overall_self:,} "
          f"({out['overall']['self_cite_violation_pct']}%)")
    print()
    print(f"  {'match_type':<15s}  {'n':>10s}  {'date_pass':>10s}  "
          f"{'date_cov':>10s}  {'self':>6s}  {'conf_p50':>10s}")
    for mt in sorted(out["by_match_type"].keys()):
        s = out["by_match_type"][mt]
        p50 = s["confidence_percentiles"].get("p50", "—")
        print(
            f"  {mt:<15s}  {s['total']:>10,}  "
            f"{s['date_sanity_pass_pct']:>9.4f}%  "
            f"{s['date_coverage_pct']:>9.2f}%  "
            f"{s['self_citations']:>6}  "
            f"{p50!s:>10s}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Pinpoint resolver benchmark using Regeste E-hints as ground truth.

Swiss Federal Court Regestes routinely cite the exact Erwägung that
addresses each rule, in the form "(E. 3.1)", "(consid. 4.2-4.5)",
"(E. 2 ff.)", etc. These hints are the court's OWN attribution of
which paragraph supports which proposition — the cleanest weak label
we can get without manual annotation.

For each BGE with at least one Regeste E-hint:
  • ground truth = set of E-numbers extracted from the Regeste
  • claim = Regeste with the E-hint parentheticals stripped
  • run _compute_pinpoint(decision_id, claim)
  • score: HIT if returned e_number ∈ ground truth, MISS otherwise
  • partial: HIT_PARENT if e_number is a prefix of any ground-truth
    hint (e.g., resolver returned E. 3 when truth is E. 3.1)

Aggregates: hit rate, partial-hit rate, no-match rate, by language,
by confidence label, by source (lexical vs semantic).

Usage:
  python3 -m benchmarks.pinpoint_regeste_bench --limit 100 --court bge

Designed to run on the VPS where the live DBs are local (no SSH
overhead per call). Output: JSON to stdout, optionally to --output.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

# Allow run from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger("pinpoint_regeste_bench")


# Patterns for extracting E-hints from Regeste prose.
#
# Swiss formatting quirks captured here:
#   • "E. 3" / "E. 3.1" / "E. 3.1.2"            (DE)
#   • "consid. 3" / "consid. 3.1"              (FR / IT)
#   • Ranges: "E. 3.1-3.5" / "consid. 2-4"
#   • Lists: "E. 3.1, 4.2" or "(E. 3.1 und 4.2)"
#   • Suffixes: "E. 3 ff." / "E. 3 sqq."
#
# Non-greedy and fragment-tolerant — under-extracting is safer than
# over-extracting (a false ground-truth label inflates apparent
# resolver errors).
_HINT_PATTERNS = [
    # Permissive parenthetical: captures everything between '(E.' / '(consid.'
    # and ')' so lists like '(E. 3.1 und 4.2)' or '(consid. 2 et 3-5)' work.
    # Post-processing splits on commas + conjunctions and discards anything
    # that doesn't full-match the digits-and-dots E-number pattern, so the
    # loose capture can't fabricate hints.
    re.compile(r"\(E\.?\s*([^)]+?)(?:\s*ff\.?|sqq\.?)?\s*\)", re.I),
    re.compile(r"\(consid\.\s*([^)]+?)(?:\s*ff\.?|sqq\.?)?\s*\)", re.I),
    # Inline (no parens) — e.g. "consid. 3.1." mid-sentence
    re.compile(r"\bconsid\.\s*(\d+(?:\.\d+){0,3})", re.I),
    re.compile(r"\bE\.\s*(\d+(?:\.\d+){0,3})", re.I),
]


def _extract_e_numbers(regeste: str) -> set[str]:
    """Return the set of E-numbers cited in the Regeste.

    Expands ranges (3.1-3.3 → {3.1, 3.2, 3.3}) and lists
    (3.1, 4.2 → {3.1, 4.2}). Returns empty set when no hint found.
    """
    if not regeste:
        return set()
    found: set[str] = set()
    for pat in _HINT_PATTERNS:
        for m in pat.finditer(regeste):
            chunk = m.group(1).strip().rstrip(".")
            # Split on commas + 'und' / 'and' / 'et' / 'e' for lists
            for part in re.split(r"[,]|\s+(?:und|and|et|e)\s+", chunk):
                part = part.strip()
                if not part:
                    continue
                if "-" in part:
                    # Range like "3.1-3.5": expand inclusive integer end-segment
                    try:
                        a, b = part.split("-", 1)
                        a, b = a.strip(), b.strip()
                        # Common pattern: "3.1-3.5" — preserve prefix, vary last
                        a_parts = a.split(".")
                        b_parts = b.split(".")
                        if len(a_parts) == len(b_parts) and a_parts[:-1] == b_parts[:-1]:
                            try:
                                lo = int(a_parts[-1])
                                hi = int(b_parts[-1])
                                if 0 <= hi - lo <= 20:
                                    for i in range(lo, hi + 1):
                                        found.add(".".join(a_parts[:-1] + [str(i)]))
                                    continue
                            except ValueError:
                                pass
                        # Fallback: just add both endpoints
                        found.add(a)
                        found.add(b)
                    except ValueError:
                        found.add(part)
                else:
                    found.add(part)
    # Cleanup: keep only well-formed E-numbers (digits + dots)
    return {e for e in found if re.fullmatch(r"\d+(\.\d+){0,3}", e)}


def _strip_e_hints(regeste: str) -> str:
    """Remove the (E. X.Y) / (consid. X.Y) parentheticals so the
    claim text doesn't leak the ground-truth answer to the resolver."""
    if not regeste:
        return regeste
    cleaned = regeste
    for pat in _HINT_PATTERNS:
        cleaned = pat.sub(" ", cleaned)
    # Collapse multiple spaces
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _is_partial_match(predicted: str, truth_set: set[str]) -> bool:
    """True if `predicted` is a prefix of any truth e_number, or vice versa."""
    for t in truth_set:
        if predicted == t:
            return True
        if t.startswith(predicted + "."):
            return True
        if predicted.startswith(t + "."):
            return True
    return False


def run_bench(decisions_db: Path, limit: int, court: str | None,
              language: str | None) -> dict:
    """Sample BGEs with Regeste E-hints, run resolver, return summary."""
    import mcp_server

    src = sqlite3.connect(f"file:{decisions_db}?immutable=1", uri=True)
    src.row_factory = sqlite3.Row

    # Pull candidate decisions: Regeste contains '(E.' or '(consid.'
    where_court = f"AND court = '{court}'" if court else ""
    where_lang = f"AND language = '{language}'" if language else ""
    sql = f"""
        SELECT decision_id, court, language, regeste
        FROM decisions
        WHERE regeste IS NOT NULL
          AND length(regeste) > 100
          AND (regeste LIKE '%(E.%' OR regeste LIKE '%(consid.%')
          {where_court}
          {where_lang}
        ORDER BY decision_date DESC
        LIMIT ?
    """
    # Ordering note: originally `citation_count DESC` (prefer leading cases),
    # but that column no longer exists in decisions.db — citation counts live
    # in reference_graph.db. Recency-only changes the candidate set; no
    # historical runs exist to stay comparable with (the citation_count run
    # crashed before producing output, 2026-08-23). If leading-case sampling
    # matters later, ATTACH the graph DB rather than resurrecting the column.
    candidates = src.execute(sql, (limit * 3,)).fetchall()
    src.close()

    # Filter to those with at least one well-formed E-hint
    cases = []
    for row in candidates:
        truth = _extract_e_numbers(row["regeste"])
        if truth:
            cases.append((row, truth))
        if len(cases) >= limit:
            break

    logger.info("Bench: %d cases (after filtering for valid E-hints)", len(cases))

    # Run resolver
    counts = {
        "total": len(cases),
        "hit_exact": 0,
        "hit_partial": 0,
        "miss_pinpointed": 0,  # resolver returned a pinpoint but wrong e
        "no_pinpoint": 0,      # resolver returned None
    }
    by_confidence: Counter = Counter()
    by_source: Counter = Counter()
    by_lang: defaultdict = defaultdict(lambda: {"hit": 0, "total": 0})

    started = time.monotonic()
    for row, truth in cases:
        claim = _strip_e_hints(row["regeste"])
        try:
            pp = mcp_server._compute_pinpoint(row["decision_id"], claim)
        except Exception as e:
            logger.warning("resolver error on %s: %s", row["decision_id"], e)
            pp = None

        lang = row["language"] or "?"
        by_lang[lang]["total"] += 1

        if pp is None:
            counts["no_pinpoint"] += 1
        else:
            by_confidence[pp.get("confidence", "?")] += 1
            by_source[pp.get("source", "?")] += 1
            predicted = pp.get("e_number", "")
            if predicted in truth:
                counts["hit_exact"] += 1
                by_lang[lang]["hit"] += 1
            elif _is_partial_match(predicted, truth):
                counts["hit_partial"] += 1
                by_lang[lang]["hit"] += 1
            else:
                counts["miss_pinpointed"] += 1

    elapsed = time.monotonic() - started

    return {
        "n_cases": counts["total"],
        "hit_exact": counts["hit_exact"],
        "hit_partial": counts["hit_partial"],
        "hit_total": counts["hit_exact"] + counts["hit_partial"],
        "hit_rate_exact": counts["hit_exact"] / max(counts["total"], 1),
        "hit_rate_total": (counts["hit_exact"] + counts["hit_partial"]) / max(counts["total"], 1),
        "miss_pinpointed": counts["miss_pinpointed"],
        "no_pinpoint": counts["no_pinpoint"],
        "by_confidence": dict(by_confidence),
        "by_source": dict(by_source),
        "by_lang": {k: v for k, v in by_lang.items()},
        "elapsed_sec": round(elapsed, 1),
        "rate_per_sec": round(counts["total"] / max(elapsed, 1e-3), 2),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--decisions-db", default=os.environ.get(
        "SWISS_CASELAW_DB",
        "/opt/caselaw/repo/output/decisions.db"))
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--court", default="bge",
                    help="Filter by court (default: bge — has the richest "
                         "Regeste E-hints; '' = all courts)")
    ap.add_argument("--language", default=None,
                    help="Filter by language (de/fr/it); default: all")
    ap.add_argument("--output", help="Write JSON summary to this path")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    )

    summary = run_bench(
        Path(args.decisions_db), args.limit,
        args.court or None, args.language,
    )
    out = json.dumps(summary, indent=2)
    print(out)
    if args.output:
        Path(args.output).write_text(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

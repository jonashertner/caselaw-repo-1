#!/usr/bin/env python3
"""
audit_pre2007_bger_gap.py — quantify the GENUINE pre-2007 BGer completeness
gap (read-only analysis; NOT pipeline-gated).

Enumerates the entscheidsuche CH_BGer autoindex, isolates decisions whose
docket year is <= --max-year (the EVG / pre-BGG era), normalizes dockets
separator-agnostically, and diffs against the published corpus
(decisions.db, opened mode=ro&immutable=1). Reports the count of ES dockets
with NO normalized match in the corpus — the genuine-absence estimate —
broken down by docket prefix and year, with a sample and an optional JSONL
of recoverable dockets for a later (pure-official) recovery scrape.

Why this gap exists (root cause): scrapers/bger.py::_extract_docket requires
a leading digit (DOCKET_RE=\\d{1,2}[A-Z]..., DOCKET_OLD_RE=\\d[A-Z]\\....), so
single-letter EVG dockets (the social-insurance court pre-2007: I/U/C/H/K/B/
P/M 123/05) are unmatchable and were never discovered by the direct scraper.

This is a DIFFERENT gap from audit_bger_coverage.py (which targets the
late-publication/Neuheiten class). Cross-referenced so the two aren't
confused.

READ-ONLY: one GET to the public ES autoindex + a read of decisions.db. No
writes to any production DB / state / pipeline.

Usage:
    python3 scripts/audit_pre2007_bger_gap.py \
        --db /opt/caselaw/repo/output/decisions.db \
        --out-jsonl /tmp/pre2007_genuine_absences.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import urllib.request
from collections import Counter
from pathlib import Path

ES_INDEX = "https://entscheidsuche.ch/docs/CH_BGer/"
UA = "CaselawBot/2.0 (legal research; swiss-caselaw project)"
# CH_BGer_<NNN>_<DOCKET>_<YYYY-MM-DD>.json — docket uses hyphens in place of
# '.', '/', and spaces (e.g. 1A-1-2000 == "1A.1/2000", I-123-2005 == "I 123/2005").
FNAME_RE = re.compile(r'(CH_BGer_\d+_(.+?)_(\d{4})-\d{2}-\d{2}\.json)')


def norm(d: str) -> str:
    """Separator-agnostic docket key: uppercase, strip non-alphanumeric.
    '1A.1/2000' / '1A-1-2000' / '1A_1/2000' all collapse to '1A12000'."""
    return re.sub(r"[^A-Z0-9]", "", d.upper())


def docket_year(docket_hyphen: str, filename_year: int) -> int:
    """Case year = the last 4-digit year embedded in the docket; fall back
    to the filename's leading date year if the docket has none."""
    yrs = re.findall(r"(?:19\d{2}|20\d{2})", docket_hyphen)
    return int(yrs[-1]) if yrs else filename_year


def enumerate_es(timeout: int = 180) -> dict[str, tuple[str, int]]:
    req = urllib.request.Request(ES_INDEX, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        html = r.read().decode("utf-8", errors="replace")
    seen: dict[str, tuple[str, int]] = {}
    for fname, docket, fyear in FNAME_RE.findall(html):
        if fname not in seen:
            seen[fname] = (docket, int(fyear))
    return seen


def corpus_norm_set(db: str) -> set[str]:
    conn = sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True, timeout=30)
    s: set[str] = set()
    for (dk,) in conn.execute(
        "SELECT docket_number FROM decisions "
        "WHERE court IN ('bger','bge','bge_historical') AND docket_number IS NOT NULL"
    ):
        if dk:
            s.add(norm(dk))
    conn.close()
    return s


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    default_db = os.path.join(os.environ.get("SWISS_CASELAW_DIR", "output"), "decisions.db")
    ap.add_argument("--db", default=default_db, help="path to decisions.db")
    ap.add_argument("--max-year", type=int, default=2007, help="case (docket) year ceiling")
    ap.add_argument("--out-jsonl", default=None, help="write genuine-absence dockets here")
    ap.add_argument("--sample", type=int, default=30)
    a = ap.parse_args()

    print(f"Enumerating {ES_INDEX} ...", file=sys.stderr)
    es = enumerate_es()
    print(f"  {len(es)} unique CH_BGer files", file=sys.stderr)
    corpus = corpus_norm_set(a.db)
    print(f"  corpus normalized bger/bge dockets: {len(corpus)}", file=sys.stderr)

    genuine: list[tuple[str, str, int]] = []
    by_prefix: Counter[str] = Counter()
    by_year: Counter[int] = Counter()
    pre_total = 0
    for fname, (docket, fyear) in es.items():
        dy = docket_year(docket, fyear)
        if dy > a.max_year:
            continue
        pre_total += 1
        if norm(docket) in corpus:
            continue
        genuine.append((fname, docket, dy))
        m = re.match(r"([0-9]*[A-Za-z]+)", docket)
        by_prefix[m.group(1).upper() if m else "?"] += 1
        by_year[dy] += 1

    out = {
        "es_total_files": len(es),
        "corpus_norm_dockets": len(corpus),
        "max_year": a.max_year,
        "pre_year_es_total": pre_total,
        "genuine_absent_total": len(genuine),
        "genuine_absent_pct_of_pre": round(100 * len(genuine) / pre_total, 1) if pre_total else 0,
        "by_prefix": dict(by_prefix.most_common()),
        "by_year": dict(sorted(by_year.items())),
        "sample": [{"file": f, "docket": d, "year": y} for f, d, y in genuine[: a.sample]],
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))

    if a.out_jsonl:
        with open(a.out_jsonl, "w", encoding="utf-8") as fh:
            for f, d, y in genuine:
                fh.write(json.dumps({"es_file": f, "docket_hyphen": d, "year": y,
                                     "es_json_url": ES_INDEX + f}, ensure_ascii=False) + "\n")
        print(f"wrote {len(genuine)} genuine-absence rows -> {a.out_jsonl}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

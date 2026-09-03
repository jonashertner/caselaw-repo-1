#!/usr/bin/env python3
"""Measure the attest_response quote rail's false-positive rate on statutes.

For every article in statutes.db, quote each paragraph of 60-400 characters
AS PRINTED on Fedlex and run it through the real rail (_audit_quotes over
_statute_source_pool, whose haystack is the stored text). A correct, as-printed
quotation that the rail flags is a false positive. "As printed" is obtained by
re-parsing the row's raw Akoma Ntoso fragment (the `xml` column) with the
parser currently in search_stack/build_statutes_db.py: with the old parser the
quote equals the stored text and only rail defects show; with the fixed parser
(footnotes routed out, inline markup joined) the quote is what a lawyer copies
from fedlex.admin.ch and every row the old build corrupted counts as a false
positive until statutes.db is rebuilt. `--stored` quotes the stored text instead. The 2026-09 gap-report review
measured 21.3 % on the 5-law dev slice (18 points from the old 600-char pool
slice, the rest from footnotes spliced into bodies and space-split article
references), so this is the gate before `audit_quotes` defaults to on.

Manual, not collected by `make test`. Reads the DB read-only. Point it at a
copy of production statutes.db with SWISS_CASELAW_STATUTES_DB or --db.

    .venv/bin/python scripts/measure_quote_rail_fp.py --db /path/statutes.db
    .venv/bin/python scripts/measure_quote_rail_fp.py --max-fp 0.02   # exit 1 above

Buckets explain each false positive by comparing the stored paragraph with
the as-printed one:
    footnote     the stored paragraph carries amendment-note text that the
                 as-printed one does not (issue B)
    split_ref    the stored paragraph carries a space-split article reference
                 or ordinal (issue C)
    spacing      the two differ only in whitespace / joined words (issue C)
    other        none of the above (rail defect, or a parser difference worth a look)
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", help="statutes.db path (default: SWISS_CASELAW_STATUTES_DB or output/statutes.db)")
    ap.add_argument("--langs", default="de,fr,it")
    ap.add_argument("--sr", nargs="*", help="restrict to these SR numbers")
    ap.add_argument("--limit", type=int, default=0, help="stop after N articles (0 = all)")
    ap.add_argument("--max-fp", type=float, default=None, help="exit 1 if the overall FP rate exceeds this")
    ap.add_argument("--show", type=int, default=10, help="print this many example false positives")
    ap.add_argument("--stored", action="store_true",
                    help="quote the stored text instead of re-parsing the xml column (measures the rail alone)")
    args = ap.parse_args()

    db = Path(args.db or os.environ.get("SWISS_CASELAW_STATUTES_DB") or REPO / "output" / "statutes.db")
    if not db.exists():
        print(f"statutes.db not found: {db}", file=sys.stderr)
        return 2
    # The server reads the path at import time.
    os.environ["SWISS_CASELAW_STATUTES_DB"] = str(db)
    import mcp_server as m  # noqa: E402
    from search_stack.build_statutes_db import parse_article  # noqa: E402
    import xml.etree.ElementTree as ET  # noqa: E402

    langs = [x.strip() for x in args.langs.split(",") if x.strip()]
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    abbr_col = {"de": "abbr_de", "fr": "abbr_fr", "it": "abbr_it"}
    laws = {r["sr_number"]: dict(r) for r in conn.execute("SELECT * FROM laws")}
    cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)")}
    has_section = "section" in cols
    reparse = not args.stored and "xml" in cols
    where = ["lang IN (%s)" % ",".join("?" * len(langs))]
    params: list = list(langs)
    if args.sr:
        where.append("sr_number IN (%s)" % ",".join("?" * len(args.sr)))
        params += args.sr
    if has_section:
        where.append("section = ''")
    xml_col = ", xml" if reparse else ""
    sql = (f"SELECT sr_number, article_num, lang, text{xml_col} FROM articles "
           f"WHERE {' AND '.join(where)} ORDER BY sr_number, lang, id")

    footnote_re = re.compile(r"Fassung gemäss|Eingefügt durch|Aufgehoben durch|Nouvelle teneur|Introduit par|"
                             r"Abrogé par|Nuovo testo|Introdotto dal|Abrogato dal|\b(?:AS|RO|RU) \d{4}\b|\b(?:BBl|FF) \d{4}\b")
    split_re = re.compile(r"\b\d+ (?:bis|ter|quater|quinquies|sexies|septies|octies|novies|decies)\b|"
                          r"\b[a-z] (?:bis|ter|quater|quinquies|sexies)\b|"
                          r"\b(?:Art\.?|Artikel|art\.|articolo)\s*\d+ [a-z]\b")
    open_q = {"de": "„", "fr": "«", "it": "«"}
    close_q = {"de": "“", "fr": "»", "it": "»"}

    quoted = flagged = 0
    unresolvable = Counter()
    by_lang = defaultdict(lambda: [0, 0])
    buckets = Counter()
    examples: list[str] = []
    seen_articles = 0
    for row in conn.execute(sql, params):
        sr, num, lang, text = row["sr_number"], row["article_num"], row["lang"], row["text"] or ""
        stored_paras = text.split("\n")
        if reparse and row["xml"]:
            try:
                text = parse_article(ET.fromstring(row["xml"]))[2] or text
            except ET.ParseError:
                pass
        abbr = (laws.get(sr) or {}).get(abbr_col[lang]) or (laws.get(sr) or {}).get("abbr_de")
        if not abbr or not re.fullmatch(r"[A-Z][A-Za-zÄÖÜ0-9]{1,11}", abbr):
            unresolvable[f"{sr} ({abbr})"] += 1
            continue
        seen_articles += 1
        if args.limit and seen_articles > args.limit:
            break
        if len(m._statute_text_cache) > 3000:
            m._statute_text_cache.clear()
        for idx, para in enumerate(text.split("\n")):
            stored = stored_paras[idx] if idx < len(stored_paras) else ""
            if not 60 <= len(para) <= 400 or '"' in para or "„" in para or "«" in para:
                continue
            draft = f"Art. {num} {abbr}: {open_q[lang]}{para}{close_q[lang]}"
            pool = m._statute_source_pool(draft)
            if not pool:
                unresolvable[f"{sr} ({abbr}) Art. {num}"] += 1
                break
            quoted += 1
            by_lang[lang][0] += 1
            issues = [i for i in m._audit_quotes(draft, pool) if i.get("category") == "quote"]
            if not issues:
                continue
            flagged += 1
            by_lang[lang][1] += 1
            if footnote_re.search(stored) and not footnote_re.search(para):
                buckets["footnote"] += 1
            elif split_re.search(stored):
                buckets["split_ref"] += 1
            elif re.sub(r"\s+", "", stored) == re.sub(r"\s+", "", para):
                buckets["spacing"] += 1
            else:
                buckets["other"] += 1
            if len(examples) < args.show:
                examples.append(f"{sr} {lang} Art. {num} para {idx + 1}: {para[:120]}…")

    rate = flagged / quoted if quoted else 0.0
    print(f"db: {db}   quoting: {'as printed (re-parsed xml)' if reparse else 'stored text'}")
    print(f"articles considered: {seen_articles}   quoted paragraphs: {quoted}   flagged: {flagged}   FP rate: {rate:.1%}")
    for lang in langs:
        q, f = by_lang[lang]
        print(f"  {lang}: {f}/{q} = {(f / q if q else 0):.1%}")
    print("causes:", dict(buckets))
    if unresolvable:
        print(f"skipped (abbreviation not audit-shaped or unresolvable): {sum(unresolvable.values())} rows across "
              f"{len(unresolvable)} keys, e.g. {list(unresolvable)[:5]}")
    for ex in examples:
        print("  FP:", ex)
    if args.max_fp is not None and rate > args.max_fp:
        print(f"FAIL: FP rate {rate:.1%} exceeds --max-fp {args.max_fp:.1%}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

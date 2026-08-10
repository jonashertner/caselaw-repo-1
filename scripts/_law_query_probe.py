#!/usr/bin/env python3
"""Issue #31 analysis probe: compare AND-form vs OR-form FTS results for a law
query against the live federal statutes index. Read-only.

    python3 scripts/_law_query_probe.py "<query terms>" [lang] [target_sr]

Prints, for the space-joined (AND) and OR-joined forms: the hit count, whether
an optional target SR number appears in the top 10, and the top article labels.
"""
import sqlite3
import sys

q = sys.argv[1] if len(sys.argv) > 1 else ""
lang = sys.argv[2] if len(sys.argv) > 2 else "de"
target = sys.argv[3] if len(sys.argv) > 3 else None
terms = [t for t in q.split() if t]
forms = {"AND": " ".join(terms), "OR": " OR ".join(terms)}

c = sqlite3.connect(
    "file:/opt/caselaw/repo/output/statutes.db?mode=ro&immutable=1", uri=True
)


def run(match):
    try:
        return c.execute(
            "SELECT a.sr_number, a.article_num, l.abbr_de FROM articles_fts f "
            "JOIN articles a ON a.id=f.rowid LEFT JOIN laws l ON a.sr_number=l.sr_number "
            "WHERE articles_fts MATCH ? AND a.lang=? ORDER BY f.rank LIMIT 10",
            (match, lang),
        ).fetchall()
    except Exception as e:  # invalid FTS etc.
        return [("ERR", str(e)[:70], "")]


print("QUERY: %r  lang=%s%s" % (q, lang, ("  target=SR " + target) if target else ""))
for name, match in forms.items():
    rows = run(match)
    labels = ["%s %s" % (r[2] or r[0], r[1]) for r in rows]
    hit = ""
    if target:
        hit = " [TARGET FOUND]" if any(r[0] == target for r in rows) else " [target NOT in top10]"
    print("  %-3s (%2d)%s: %s" % (name, len(rows), hit, labels))

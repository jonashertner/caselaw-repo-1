"""Build the public law-code reference table (issue #74).

The cross-language abbreviation pairing is currently re-encoded by hand in
at least three places — quality/checks/statute_graph.py TOP_FEDERAL_LAWS,
benchmarks/build_canonical_top_statutes.py, and the SR mapping the statute
tools assume. Adding a law code today means remembering three edit sites,
and forgetting one degrades a QC threshold or a benchmark silently.

This derives the table from statutes.db instead of curating it: `laws`
carries sr_number plus abbr_de/abbr_fr/abbr_it per act, so the alias
groups ARE the data. Cantonal codes come from cantonal_laws.db where
available.

Consumer problem this solves (reported by @Tomagori): the extractor emits
language-specific codes by design, so a downstream tool that does not know
OR == CO silently loses the majority of French statute references — our own
QC comment measures OR alone at 125,592 edges against 359,195 for OR+CO.
Without the table that is a wrong answer with no error.

  python3 scripts/build_law_code_table.py \
      --statutes output/statutes.db \
      --cantonal output/cantonal_laws.db \
      --out docs/data/law_codes.json --csv docs/data/law_codes.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path


def _norm(a: str | None) -> str | None:
    a = (a or "").strip()
    return a or None


def federal(statutes_db: str) -> list[dict]:
    con = sqlite3.connect(f"file:{statutes_db}?mode=ro&immutable=1", uri=True)
    con.row_factory = sqlite3.Row
    out = []
    for r in con.execute(
            "SELECT sr_number, abbr_de, abbr_fr, abbr_it, "
            "title_de, title_fr, title_it FROM laws ORDER BY sr_number"):
        de, fr, it = _norm(r["abbr_de"]), _norm(r["abbr_fr"]), _norm(r["abbr_it"])
        if not (de or fr or it):
            continue
        aliases = sorted({a.upper() for a in (de, fr, it) if a})
        out.append({
            "level": "federal",
            "sr_number": r["sr_number"],
            "abbr_de": de, "abbr_fr": fr, "abbr_it": it,
            "aliases": aliases,
            "title_de": r["title_de"], "title_fr": r["title_fr"],
            "title_it": r["title_it"],
        })
    con.close()
    return out


def cantonal(db: str | None) -> list[dict]:
    """Cantonal acts keyed (canton, systematic number), titles per language.

    NOTE for consumers: cantonal_laws.laws carries canton, sr_number,
    title and language but NO abbreviation column — we do not hold a
    curated cantonal abbreviation list, so none is published here rather
    than one being invented. What is published is the (canton, number,
    title) triple per language, which is the part that has no other
    machine-readable source.
    """
    if not db or not Path(db).exists():
        return []
    con = sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True)
    con.row_factory = sqlite3.Row
    cols = {r[1] for r in con.execute("PRAGMA table_info(laws)")}
    if not {"canton", "sr_number"} <= cols:
        con.close()
        return []
    grouped: dict[tuple, dict] = {}
    for r in con.execute(
            "SELECT canton, sr_number, language, title, is_active, "
            "original_url FROM laws"):
        num = _norm(r["sr_number"])
        canton = _norm(r["canton"])
        if not (canton and num):
            continue
        key = (canton, num)
        row = grouped.setdefault(key, {
            "level": "cantonal", "canton": canton,
            "systematic_number": num, "titles": {},
            "is_active": bool(r["is_active"]) if r["is_active"] is not None
            else None,
            "url": r["original_url"],
        })
        lang = (r["language"] or "").lower()[:2]
        if lang and r["title"]:
            row["titles"][lang] = r["title"]
    con.close()
    return sorted(grouped.values(),
                  key=lambda x: (x["canton"], x["systematic_number"]))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--statutes", required=True)
    ap.add_argument("--cantonal")
    ap.add_argument("--out", required=True)
    ap.add_argument("--csv")
    a = ap.parse_args()

    fed = federal(a.statutes)
    can = cantonal(a.cantonal)

    # alias -> SR, the lookup a consumer of the extractor actually needs
    alias_index: dict[str, list[str]] = {}
    for row in fed:
        for al in row["aliases"]:
            alias_index.setdefault(al, [])
            if row["sr_number"] not in alias_index[al]:
                alias_index[al].append(row["sr_number"])

    multilingual = [r for r in fed if len(r["aliases"]) > 1]
    payload = {
        "_": "OpenCaseLaw law-code reference table. Derived from the "
             "published corpus, not hand-curated; regenerate with "
             "scripts/build_law_code_table.py.",
        "_why": "The citation extractor emits language-specific law codes. "
                "A consumer that does not pair them (OR==CO, ZGB==CC) "
                "silently loses most French-language statute references.",
        "generated": date.today().isoformat(),
        "license": "CC0-1.0",
        "counts": {
            "federal_acts": len(fed),
            "federal_acts_with_multilingual_abbr": len(multilingual),
            "distinct_aliases": len(alias_index),
            "cantonal_codes": len(can),
        },
        "alias_to_sr": {k: (v[0] if len(v) == 1 else v)
                        for k, v in sorted(alias_index.items())},
        "federal": fed,
        "cantonal": can,
    }
    outp = Path(a.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(payload, indent=1, ensure_ascii=False))
    print(f"wrote {outp}: {len(fed):,} federal acts "
          f"({len(multilingual):,} multilingual), {len(alias_index):,} "
          f"aliases, {len(can):,} cantonal codes")

    if a.csv:
        cp = Path(a.csv)
        with open(cp, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["level", "canton", "number", "abbr_de", "abbr_fr",
                        "abbr_it", "aliases", "title_de"])
            for r in fed:
                w.writerow(["federal", "", r["sr_number"], r["abbr_de"] or "",
                            r["abbr_fr"] or "", r["abbr_it"] or "",
                            "|".join(r["aliases"]), r["title_de"] or ""])
            for r in can:
                t = r.get("titles") or {}
                w.writerow(["cantonal", r["canton"],
                            r["systematic_number"], "", "", "", "",
                            t.get("de") or t.get("fr") or t.get("it") or ""])
        print(f"wrote {cp}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

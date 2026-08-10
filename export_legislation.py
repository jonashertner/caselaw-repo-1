"""export_legislation — Fedlex + cantonal law DBs -> Parquet shards for a
separate (private) HF dataset ``voilaj/swiss-legislation``.

Read-only from statutes.db (federal) + cantonal_laws.db (cantonal). Article-
level rows (the useful granularity). Federal is one shard; cantonal is one
shard PER CANTON (looped to completion). Provenance is preserved: cantonal
rows carry ``text_source`` (direct-portal scrape vs LexFind fallback) so the
19-direct / 7-fallback split stays visible.

Usage:
  python export_legislation.py --statutes-db output/statutes.db \
      --cantonal-db output/cantonal_laws.db --out /mnt/.../legislation_export
Then upload with upload_legislation_hf.py (separate, gated on verification).
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

FEDERAL_SCHEMA = pa.schema([
    ("sr_number", pa.string()), ("law_title", pa.string()),
    ("abbreviation", pa.string()), ("article_num", pa.string()),
    ("heading", pa.string()), ("text", pa.string()),
    ("language", pa.string()), ("consolidation_date", pa.string()),
    ("work_uri", pa.string()), ("source", pa.string()),
])
CANTONAL_SCHEMA = pa.schema([
    ("canton", pa.string()), ("sr_number", pa.string()),
    ("law_title", pa.string()), ("category", pa.string()),
    ("article_num", pa.string()), ("seq", pa.int64()),
    ("heading", pa.string()), ("text", pa.string()),
    ("language", pa.string()), ("is_active", pa.int64()),
    ("text_source", pa.string()), ("original_url", pa.string()),
    ("version_active_since", pa.string()), ("lexfind_id", pa.string()),
    ("source", pa.string()),
])


def _coerce(val, patype):
    """SQLite is dynamically typed; coerce each value to the parquet column
    type (str vs int) so a stray int in a string column can't abort the write."""
    if val is None:
        return None
    if pa.types.is_string(patype):
        return str(val)
    if pa.types.is_integer(patype):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None
    return val


def _write(rows: list[dict], schema: pa.Schema, path: Path) -> int:
    if not rows:
        return 0
    cols = {f.name: [_coerce(r.get(f.name), f.type) for r in rows] for f in schema}
    tbl = pa.table(cols, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(tbl, tmp, compression="zstd")
    tmp.replace(path)
    return len(rows)


def export_federal(db: Path, out: Path) -> int:
    c = sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True)
    c.row_factory = sqlite3.Row
    rows = []
    q = """SELECT a.sr_number, a.article_num, a.heading, a.text, a.lang,
                  l.title_de, l.title_fr, l.title_it, l.abbr_de, l.abbr_fr,
                  l.abbr_it, l.consolidation_date, l.work_uri
           FROM articles a LEFT JOIN laws l USING (sr_number)
           WHERE a.text IS NOT NULL AND a.text != ''"""
    for r in c.execute(q):
        lang = r["lang"] or "de"
        title = {"de": r["title_de"], "fr": r["title_fr"],
                 "it": r["title_it"]}.get(lang) or r["title_de"]
        abbr = {"de": r["abbr_de"], "fr": r["abbr_fr"],
                "it": r["abbr_it"]}.get(lang) or r["abbr_de"]
        rows.append({
            "sr_number": r["sr_number"], "law_title": title,
            "abbreviation": abbr, "article_num": r["article_num"],
            "heading": r["heading"], "text": r["text"], "language": lang,
            "consolidation_date": r["consolidation_date"],
            "work_uri": r["work_uri"], "source": "fedlex",
        })
    n = _write(rows, FEDERAL_SCHEMA, out / "federal" / "fedlex.parquet")
    print(f"  federal/fedlex.parquet: {n:,} article rows", flush=True)
    return n


def export_cantonal(db: Path, out: Path) -> int:
    c = sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True)
    c.row_factory = sqlite3.Row
    cantons = [r[0] for r in c.execute(
        "SELECT DISTINCT canton FROM articles WHERE canton IS NOT NULL ORDER BY canton")]
    total = 0
    for canton in cantons:  # loop over every canton to completion
        rows = []
        q = """SELECT a.canton, a.article_num, a.seq, a.heading, a.text,
                      a.language, a.lexfind_id, l.sr_number, l.title,
                      l.category, l.is_active, l.text_source, l.original_url,
                      l.version_active_since
               FROM articles a LEFT JOIN laws l
                    ON a.lexfind_id = l.lexfind_id AND a.language = l.language
               WHERE a.canton = ? AND a.text IS NOT NULL AND a.text != ''"""
        for r in c.execute(q, (canton,)):
            rows.append({
                "canton": r["canton"], "sr_number": r["sr_number"],
                "law_title": r["title"], "category": r["category"],
                "article_num": r["article_num"], "seq": r["seq"],
                "heading": r["heading"], "text": r["text"],
                "language": r["language"],
                "is_active": r["is_active"], "text_source": r["text_source"],
                "original_url": r["original_url"],
                "version_active_since": r["version_active_since"],
                "lexfind_id": r["lexfind_id"], "source": "cantonal",
            })
        n = _write(rows, CANTONAL_SCHEMA,
                   out / "cantonal" / f"{canton.lower()}.parquet")
        total += n
        print(f"  cantonal/{canton.lower()}.parquet: {n:,} rows", flush=True)
    print(f"  cantonal TOTAL: {total:,} rows across {len(cantons)} cantons", flush=True)
    return total


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--statutes-db", type=Path, default=Path("output/statutes.db"))
    ap.add_argument("--cantonal-db", type=Path, default=Path("output/cantonal_laws.db"))
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    print("== Fedlex ==", flush=True)
    f = export_federal(args.statutes_db, args.out)
    print("== Cantonal (per canton) ==", flush=True)
    ca = export_cantonal(args.cantonal_db, args.out)
    print(f"DONE: federal {f:,} + cantonal {ca:,} = {f+ca:,} article rows -> {args.out}", flush=True)


if __name__ == "__main__":
    main()

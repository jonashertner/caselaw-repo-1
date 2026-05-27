#!/usr/bin/env python3
"""Build the unified Swiss OA legal scholarship SQLite DB.

Inputs:
  - JSONL files from `scrapers/scholarship/*` (one per source)
  - ok_commentaries.db (re-export of OnlineKommentar + OpenLegalCommentary)

Output:
  - output/legal_scholarship.db (FTS5)

Schema (one row per publication):
  publications(pub_id, source, pub_type, title, authors, abstract, language,
               publication_date, year, journal, volume, issue, pages,
               publisher, institution, doi, isbn, issn, url, pdf_url,
               full_text, has_full_text, license, license_url, keywords,
               subjects, ingested_at, source_record_id, raw_metadata)

  publications_fts (FTS5 over title, authors, abstract, full_text, journal,
                    keywords, subjects)

  pub_citations_decisions(pub_id, decision_id, snippet)
  pub_citations_statutes(pub_id, sr_number, article, snippet)
  pub_citations_pubs(src_pub_id, dst_pub_id, snippet)

Idempotent: re-running rebuilds from scratch (atomic swap via .db.tmp).

Usage:
    python -m search_stack.build_legal_scholarship
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("build_legal_scholarship")

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO_ROOT / "output" / "legal_scholarship.db"
DEFAULT_JSONL_DIR = REPO_ROOT / "output" / "legal_scholarship"
DEFAULT_OK_DB = REPO_ROOT / "output" / "ok_commentaries.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS publications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pub_id TEXT UNIQUE NOT NULL,
    source TEXT NOT NULL,
    pub_type TEXT NOT NULL,
    title TEXT NOT NULL,
    authors TEXT,
    abstract TEXT,
    language TEXT,
    publication_date TEXT,
    year INTEGER,
    journal TEXT,
    volume TEXT,
    issue TEXT,
    pages TEXT,
    publisher TEXT,
    institution TEXT,
    doi TEXT,
    isbn TEXT,
    issn TEXT,
    url TEXT,
    pdf_url TEXT,
    full_text TEXT,
    has_full_text INTEGER NOT NULL DEFAULT 0,
    license TEXT,
    license_url TEXT,
    keywords TEXT,
    subjects TEXT,
    ingested_at TEXT NOT NULL,
    source_record_id TEXT,
    raw_metadata TEXT
);

CREATE INDEX IF NOT EXISTS idx_pub_source       ON publications(source);
CREATE INDEX IF NOT EXISTS idx_pub_type         ON publications(pub_type);
CREATE INDEX IF NOT EXISTS idx_pub_doi          ON publications(doi);
CREATE INDEX IF NOT EXISTS idx_pub_year         ON publications(year);
CREATE INDEX IF NOT EXISTS idx_pub_journal      ON publications(journal);
CREATE INDEX IF NOT EXISTS idx_pub_language     ON publications(language);

CREATE VIRTUAL TABLE IF NOT EXISTS publications_fts USING fts5(
    title, authors, abstract, full_text, journal, keywords, subjects,
    content='publications', content_rowid='id',
    tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER IF NOT EXISTS publications_ai AFTER INSERT ON publications BEGIN
    INSERT INTO publications_fts(rowid, title, authors, abstract,
                                 full_text, journal, keywords, subjects)
    VALUES (new.id, new.title, new.authors, new.abstract,
            new.full_text, new.journal, new.keywords, new.subjects);
END;

CREATE TABLE IF NOT EXISTS pub_citations_decisions (
    pub_id INTEGER NOT NULL,
    decision_id TEXT NOT NULL,
    snippet TEXT,
    PRIMARY KEY (pub_id, decision_id),
    FOREIGN KEY (pub_id) REFERENCES publications(id)
);
CREATE INDEX IF NOT EXISTS idx_pcd_decision ON pub_citations_decisions(decision_id);

CREATE TABLE IF NOT EXISTS pub_citations_statutes (
    pub_id INTEGER NOT NULL,
    sr_number TEXT NOT NULL,
    article TEXT NOT NULL DEFAULT '',
    snippet TEXT,
    PRIMARY KEY (pub_id, sr_number, article),
    FOREIGN KEY (pub_id) REFERENCES publications(id)
);
CREATE INDEX IF NOT EXISTS idx_pcs_sr ON pub_citations_statutes(sr_number, article);

CREATE TABLE IF NOT EXISTS pub_citations_pubs (
    src_pub_id INTEGER NOT NULL,
    dst_pub_id INTEGER NOT NULL,
    snippet TEXT,
    PRIMARY KEY (src_pub_id, dst_pub_id),
    FOREIGN KEY (src_pub_id) REFERENCES publications(id),
    FOREIGN KEY (dst_pub_id) REFERENCES publications(id)
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


def _pub_id(source: str, source_record_id: str) -> str:
    """Stable canonical pub id, prefixed by source."""
    rid = source_record_id.strip()
    # OAI identifiers like 'oai:www.hope.uzh.ch:article/1382' -> 'article-1382'
    if rid.startswith("oai:"):
        parts = rid.split(":", 2)
        if len(parts) == 3:
            rid = parts[2].replace("/", "-")
    rid = "".join(c if c.isalnum() or c in "-_.:" else "_" for c in rid)
    return f"{source}:{rid}"


def _join(values, sep="; "):
    if not values:
        return None
    if isinstance(values, str):
        return values
    return sep.join(str(v) for v in values if v is not None and str(v).strip())


def ingest_jsonl(conn: sqlite3.Connection, path: Path) -> int:
    """Load one source's JSONL into publications. Returns row count inserted."""
    n = 0
    cur = conn.cursor()
    now_iso = datetime.now(timezone.utc).isoformat()
    with path.open(encoding="utf-8") as fh:
        for line_num, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError as e:
                log.warning("bad JSON in %s line %d: %s", path.name, line_num, e)
                continue
            source = d.get("source") or path.stem
            src_id = d.get("source_record_id") or d.get("doi") or d.get("url") or ""
            if not src_id:
                continue
            pub_id = _pub_id(source, src_id)
            try:
                cur.execute(
                    """INSERT OR IGNORE INTO publications
                       (pub_id, source, pub_type, title, authors, abstract,
                        language, publication_date, year, journal, publisher,
                        doi, issn, url, license, license_url, keywords,
                        subjects, ingested_at, source_record_id, raw_metadata,
                        has_full_text, full_text)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                               ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        pub_id,
                        source,
                        d.get("pub_type") or "article",
                        d.get("title") or "(untitled)",
                        _join(d.get("authors")),
                        d.get("abstract"),
                        d.get("language"),
                        d.get("publication_date"),
                        d.get("year"),
                        (d.get("sources_raw") or [None])[0]
                        if d.get("sources_raw") else d.get("publisher"),
                        d.get("publisher"),
                        d.get("doi"),
                        d.get("issn"),
                        d.get("url"),
                        d.get("license"),
                        d.get("license_url"),
                        _join(d.get("subjects")),  # subjects are also our keywords source
                        _join(d.get("subjects")),
                        now_iso,
                        d.get("source_record_id"),
                        json.dumps(d, ensure_ascii=False),
                        1 if d.get("full_text") else 0,
                        d.get("full_text"),
                    ),
                )
                if cur.rowcount > 0:
                    n += 1
            except sqlite3.Error as e:
                log.warning("insert failed (%s, src_id=%s): %s", source, src_id, e)
    conn.commit()
    return n


def ingest_ok_commentaries(conn: sqlite3.Connection, ok_db_path: Path) -> int:
    """Re-export OnlineKommentar + OpenLegalCommentary into publications.

    Each commentary becomes a `pub_type='commentary'` row keyed by ok_uuid.
    Cross-referenced to its statute article so it can be found via
    find_scholarship_citing_statute too.
    """
    if not ok_db_path.exists():
        log.info("OK DB not found, skipping commentaries: %s", ok_db_path)
        return 0
    n = 0
    ok = sqlite3.connect(f"file:{ok_db_path}?mode=ro", uri=True)
    ok.row_factory = sqlite3.Row
    cur = conn.cursor()
    now_iso = datetime.now(timezone.utc).isoformat()

    rows = ok.execute(
        """SELECT ok_uuid, sr_number, abbr, article_num, title, language,
                  date, authors, editors, suggested_citation, html_link,
                  pdf_link, content_text
           FROM commentaries"""
    )
    for r in rows:
        source = "openlegalcommentary" if (r["ok_uuid"] or "").startswith("olc_") else "onlinekommentar"
        pub_id = f"{source}:{r['ok_uuid']}"
        title = f"{r['abbr'] or ''} Art. {r['article_num']} — {r['title']}".strip(" —")
        cur.execute(
            """INSERT OR IGNORE INTO publications
               (pub_id, source, pub_type, title, authors, language,
                publication_date, year, journal, doi, url, pdf_url,
                full_text, has_full_text, license, license_url,
                ingested_at, source_record_id, raw_metadata)
               VALUES (?, ?, 'commentary', ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, 1,
                       ?, ?, ?, ?, ?)""",
            (
                pub_id, source, title,
                r["authors"], r["language"],
                r["date"], int((r["date"] or "0000")[:4]) if (r["date"] or "")[:4].isdigit() else None,
                "OnlineKommentar.ch" if source == "onlinekommentar" else "OpenLegalCommentary.ch",
                r["html_link"], r["pdf_link"],
                r["content_text"],
                "CC-BY-4.0" if source == "onlinekommentar" else "CC-BY-SA-4.0",
                "https://creativecommons.org/licenses/by/4.0/" if source == "onlinekommentar"
                else "https://creativecommons.org/licenses/by-sa/4.0/",
                now_iso, r["ok_uuid"],
                json.dumps({
                    "ok_uuid": r["ok_uuid"], "sr_number": r["sr_number"],
                    "abbr": r["abbr"], "article_num": r["article_num"],
                    "suggested_citation": r["suggested_citation"],
                    "editors": r["editors"],
                }, ensure_ascii=False),
            ),
        )
        if cur.rowcount > 0:
            n += 1
            # Cross-link the commentary to its statute article so
            # find_scholarship_citing_statute(sr_number, article) returns it.
            pub_row_id = cur.execute(
                "SELECT id FROM publications WHERE pub_id=?", (pub_id,)
            ).fetchone()
            if pub_row_id and r["sr_number"]:
                cur.execute(
                    """INSERT OR IGNORE INTO pub_citations_statutes
                       (pub_id, sr_number, article, snippet)
                       VALUES (?, ?, ?, NULL)""",
                    (pub_row_id[0], r["sr_number"], r["article_num"] or ""),
                )
    conn.commit()
    ok.close()
    return n


def build(
    db_path: Path = DEFAULT_DB,
    jsonl_dir: Path = DEFAULT_JSONL_DIR,
    ok_db_path: Path = DEFAULT_OK_DB,
) -> dict:
    """Build the legal_scholarship DB from scratch. Atomic swap."""
    tmp_path = db_path.with_suffix(".db.tmp")
    for aux in (
        tmp_path,
        tmp_path.with_name(tmp_path.name + "-wal"),
        tmp_path.with_name(tmp_path.name + "-shm"),
        tmp_path.with_name(tmp_path.name + "-journal"),
    ):
        if aux.exists():
            aux.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(tmp_path))
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA_SQL)

    summary: dict = {"by_source": {}}

    # 1. JSONL sources
    if jsonl_dir.exists():
        for jsonl in sorted(jsonl_dir.glob("*.jsonl")):
            t0 = time.time()
            n = ingest_jsonl(conn, jsonl)
            log.info("ingested %d rows from %s in %.1fs", n, jsonl.name, time.time() - t0)
            summary["by_source"][jsonl.stem] = n
    else:
        log.info("No JSONL dir at %s — skipping source harvests", jsonl_dir)

    # 2. OK commentaries re-export
    t0 = time.time()
    n = ingest_ok_commentaries(conn, ok_db_path)
    log.info("ingested %d commentaries from %s in %.1fs",
             n, ok_db_path, time.time() - t0)
    summary["by_source"]["onlinekommentar+openlegalcommentary"] = n

    # 3. Stats + meta
    total = conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
    by_type = dict(conn.execute(
        "SELECT pub_type, COUNT(*) FROM publications GROUP BY pub_type"
    ).fetchall())
    by_lang = dict(conn.execute(
        "SELECT language, COUNT(*) FROM publications GROUP BY language"
    ).fetchall())
    summary["total_publications"] = total
    summary["by_type"] = by_type
    summary["by_language"] = by_lang

    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('built_at', ?)",
        (datetime.now(timezone.utc).isoformat(),),
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('summary', ?)",
        (json.dumps(summary, ensure_ascii=False),),
    )

    # FTS5 optimize
    conn.execute("INSERT INTO publications_fts(publications_fts) VALUES ('optimize')")
    conn.commit()
    conn.close()

    os.replace(str(tmp_path), str(db_path))
    log.info("DB built: %s rows=%d", db_path, total)
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--jsonl-dir", type=Path, default=DEFAULT_JSONL_DIR)
    p.add_argument("--ok-db", type=Path, default=DEFAULT_OK_DB)
    args = p.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    summary = build(args.db, args.jsonl_dir, args.ok_db)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

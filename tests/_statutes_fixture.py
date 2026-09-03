"""Shared in-memory statutes.db fixture built with the real schema.

Tests that hand-write a `CREATE TABLE articles (...)` drift from the builder
the moment a column is added (2026-09: `section`, `section_heading`, `eid`).
This helper calls the real search_stack.build_statutes_db.create_schema on
a :memory: connection and fills articles_fts the way build_db does, so a
fixture row is always a valid production row.

    conn = make_statutes_conn([
        {"sr_number": "220", "article_num": "1", "text": "Main body."},
        {"sr_number": "220", "article_num": "1", "text": "Transitional.",
         "section": "disp_u2", "section_heading": "Schlussbestimmungen ..."},
    ])

Row keys: sr_number, article_num, heading, footnote, text, xml, lang,
section, section_heading, eid. Missing keys default to the values below;
`eid` is derived from section + article_num when absent. `laws` is an
optional list of dicts with the `laws` table's columns; every SR referenced
by a row gets a law row anyway (from _KNOWN_LAWS when the SR is one of the
dev-slice acts, else a bare one), so JOINs and abbreviation lookups work.
"""
from __future__ import annotations

import sqlite3

from search_stack.build_statutes_db import create_schema

_ARTICLE_DEFAULTS = {
    "sr_number": "220",
    "article_num": "1",
    "heading": None,
    "footnote": None,
    "text": "",
    "xml": None,
    "lang": "de",
    "section": "",
    "section_heading": None,
    "eid": None,
}

_LAW_COLUMNS = ("sr_number", "title_de", "title_fr", "title_it",
                "abbr_de", "abbr_fr", "abbr_it", "consolidation_date", "work_uri")

# The five acts of the local dev slice (output/statutes.db).
_KNOWN_LAWS = {
    "101": {"title_de": "Bundesverfassung der Schweizerischen Eidgenossenschaft vom 18. April 1999",
            "title_fr": "Constitution fédérale de la Confédération suisse du 18 avril 1999",
            "title_it": "Costituzione federale della Confederazione Svizzera del 18 aprile 1999",
            "abbr_de": "BV", "abbr_fr": "Cst.", "abbr_it": "Cost.",
            "work_uri": "https://fedlex.data.admin.ch/eli/cc/1999/404"},
    "173.110": {"title_de": "Bundesgesetz über das Bundesgericht",
                "title_fr": "Loi sur le Tribunal fédéral",
                "title_it": "Legge sul Tribunale federale",
                "abbr_de": "BGG", "abbr_fr": "LTF", "abbr_it": "LTF",
                "work_uri": "https://fedlex.data.admin.ch/eli/cc/2006/218"},
    "210": {"title_de": "Schweizerisches Zivilgesetzbuch vom 10. Dezember 1907",
            "title_fr": "Code civil suisse du 10 décembre 1907",
            "title_it": "Codice civile svizzero del 10 dicembre 1907",
            "abbr_de": "ZGB", "abbr_fr": "CC", "abbr_it": "CC",
            "work_uri": "https://fedlex.data.admin.ch/eli/cc/24/233_245_233"},
    "220": {"title_de": "Bundesgesetz betreffend die Ergänzung des Schweizerischen Zivilgesetzbuches "
                        "(Fünfter Teil: Obligationenrecht)",
            "title_fr": "Loi fédérale complétant le Code civil suisse (Livre cinquième: Droit des obligations)",
            "title_it": "Legge federale di complemento del Codice civile svizzero "
                        "(Libro quinto: Diritto delle obbligazioni)",
            "abbr_de": "OR", "abbr_fr": "CO", "abbr_it": "CO",
            "work_uri": "https://fedlex.data.admin.ch/eli/cc/27/317_321_377"},
    "311.0": {"title_de": "Schweizerisches Strafgesetzbuch vom 21. Dezember 1937",
              "title_fr": "Code pénal suisse du 21 décembre 1937",
              "title_it": "Codice penale svizzero del 21 dicembre 1937",
              "abbr_de": "StGB", "abbr_fr": "CP", "abbr_it": "CP",
              "work_uri": "https://fedlex.data.admin.ch/eli/cc/54/757_781_799"},
}


def make_statutes_conn(rows: list[dict], laws: list[dict] | None = None) -> sqlite3.Connection:
    """In-memory statutes.db with the production schema, `rows` inserted into
    `articles` (see module docstring for keys and defaults), law rows for
    every SR involved, and articles_fts populated."""
    conn = sqlite3.connect(":memory:")
    create_schema(conn)

    srs: set[str] = set()
    for row in rows:
        unknown = set(row) - set(_ARTICLE_DEFAULTS)
        if unknown:
            raise KeyError(f"unknown articles column(s): {sorted(unknown)}")
        r = {**_ARTICLE_DEFAULTS, **row}
        if r["eid"] is None:
            prefix = f"{r['section']}/" if r["section"] else ""
            r["eid"] = f"{prefix}art_{r['article_num']}"
        conn.execute(
            """INSERT INTO articles (sr_number, article_num, heading, footnote, text, xml,
                                     lang, section, section_heading, eid)
               VALUES (:sr_number, :article_num, :heading, :footnote, :text, :xml,
                       :lang, :section, :section_heading, :eid)""",
            r,
        )
        srs.add(r["sr_number"])

    law_rows = {law["sr_number"]: dict(law) for law in (laws or [])}
    for sr in sorted(srs - set(law_rows)):
        law_rows[sr] = {"sr_number": sr, **_KNOWN_LAWS.get(sr, {})}
    for law in law_rows.values():
        unknown = set(law) - set(_LAW_COLUMNS)
        if unknown:
            raise KeyError(f"unknown laws column(s): {sorted(unknown)}")
        full = {col: law.get(col) for col in _LAW_COLUMNS}
        conn.execute(
            f"INSERT OR REPLACE INTO laws ({', '.join(_LAW_COLUMNS)}) "
            f"VALUES ({', '.join(':' + c for c in _LAW_COLUMNS)})",
            full,
        )

    conn.execute("""
        INSERT INTO articles_fts(rowid, sr_number, article_num, heading, text, lang)
        SELECT id, sr_number, article_num, heading, text, lang FROM articles
    """)
    conn.commit()
    return conn

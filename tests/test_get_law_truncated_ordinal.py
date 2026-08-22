"""GitHub #87: get_law(abbreviation="StGB", article="322decies") returned
"No articles found" while still emitting a correct Fedlex anchor.

The article was never missing. statutes.db was built with a suffix regex whose
ordinal list stopped at "novies", so Art. 322decies and Art. 179decies StGB were
stored under "322d" and "179d". search_stack/build_statutes_db.py parses them
correctly now, but the shipped mirror predates that fix, so get_law aliases
those two exact pairs.

The regression guard that matters here is the OR case. Art. 322d OR is a real
provision (Gratifikation), so a general "ordinal -> first letter" retry would
answer a query for OR Art. 322decies, which does not exist, with the text of a
different article under the requested number. That is worse than an empty
result, and these tests pin it shut.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

STGB_322D = "1 Keine nicht gebuehrenden Vorteile sind: a. dienstrechtlich ..."
STGB_179D = "Wer die Identitaet einer anderen Person ohne deren Einwilligung ..."
OR_322D = "1 Richtet der Arbeitgeber neben dem Lohn bei bestimmten Anlaessen ..."


def _conn(extra_rows: list[tuple] | None = None):
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        CREATE TABLE laws (sr_number TEXT PRIMARY KEY, title_de TEXT, title_fr TEXT,
            title_it TEXT, abbr_de TEXT, abbr_fr TEXT, abbr_it TEXT, consolidation_date TEXT);
        CREATE TABLE articles (sr_number TEXT, article_num TEXT, heading TEXT, text TEXT, lang TEXT);
        """
    )
    con.execute("INSERT INTO laws VALUES ('311.0','StGB de','CP fr','CP it','StGB','CP','CP','2026-06-12')")
    con.execute("INSERT INTO laws VALUES ('220','OR de','CO fr','CO it','OR','CO','CO','2026-06-12')")
    # The corpus as currently shipped: decies stored under its truncated number.
    con.execute("INSERT INTO articles VALUES ('311.0','322d',NULL,?,'de')", (STGB_322D,))
    con.execute("INSERT INTO articles VALUES ('311.0','179d',NULL,?,'de')", (STGB_179D,))
    con.execute("INSERT INTO articles VALUES ('311.0','322novies',NULL,'Wer als Arbeitnehmer ...','de')")
    # OR Art. 322d is a genuine article, not a truncation.
    con.execute("INSERT INTO articles VALUES ('220','322d',NULL,?,'de')", (OR_322D,))
    for row in extra_rows or []:
        con.execute("INSERT INTO articles VALUES (?,?,?,?,?)", row)
    con.commit()
    return con


def test_stgb_322decies_resolves_and_is_relabelled(monkeypatch):
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: _conn())
    r = m.get_law(sr_number="311.0", article="322decies")
    arts = r.get("articles") or []
    assert arts, "Art. 322decies StGB must resolve, not return an empty list"
    assert arts[0]["text"] == STGB_322D
    # Relabelled to what the caller asked for: the text really is 322decies, and
    # the Fedlex anchor in the same response already points at #art_322decies.
    assert arts[0]["article_num"] == "322decies"
    alias = r.get("article_number_alias")
    assert alias and alias["stored_as"] == "322d"


def test_stgb_179decies_resolves(monkeypatch):
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: _conn())
    arts = m.get_law(sr_number="311.0", article="179decies").get("articles") or []
    assert arts and arts[0]["text"] == STGB_179D


def test_or_322decies_still_returns_nothing(monkeypatch):
    # The whole reason the alias table is keyed on (sr_number, article).
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: _conn())
    r = m.get_law(sr_number="220", article="322decies")
    assert not (r.get("articles") or []), (
        "OR has no Art. 322decies; returning OR Art. 322d (Gratifikation) here "
        "would be real text under the wrong number"
    )


def test_or_322d_still_resolves_normally(monkeypatch):
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: _conn())
    arts = m.get_law(sr_number="220", article="322d").get("articles") or []
    assert arts and arts[0]["text"] == OR_322D
    assert arts[0]["article_num"] == "322d"


def test_novies_unaffected(monkeypatch):
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: _conn())
    r = m.get_law(sr_number="311.0", article="322novies")
    assert (r.get("articles") or [])
    assert r.get("article_number_alias") is None


def test_alias_stops_firing_once_the_corpus_is_fixed(monkeypatch):
    # After a statutes.db rebuild the real number is present, the exact-match
    # lookup wins, and the alias table is dead code that can be deleted.
    fixed = [("311.0", "322decies", None, "REBUILT TEXT", "de")]
    monkeypatch.setattr(m, "_get_statutes_conn", lambda: _conn(fixed))
    r = m.get_law(sr_number="311.0", article="322decies")
    arts = r.get("articles") or []
    assert arts and arts[0]["text"] == "REBUILT TEXT"
    assert r.get("article_number_alias") is None

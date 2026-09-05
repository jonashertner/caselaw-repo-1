"""attest_response quote rail against statute text (2026-09 gap report).

Before: the statute source pool held the first 600 characters of the
German article only. A verbatim French or Italian statute quote was
flagged as a hallucination every time, and so was any German quote from
the tail of a long article. The rail was also advertised unconditionally
while being opt-in and hidden from the MCP schema.

Offline: an in-memory statutes fixture is injected through
_get_statutes_conn (pattern of tests/test_get_law_lang_fallback.py); the
drafts carry no case citation, so decisions.db is never touched. Every
positive test has a negative control so a rail that silently skips the
quote cannot pass.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

ART41_DE = ("1 Wer einem andern widerrechtlich Schaden zufügt, sei es mit Absicht, sei es aus "
            "Fahrlässigkeit, wird ihm zum Ersatze verpflichtet.\n"
            "2 Ebenso ist zum Ersatze verpflichtet, wer einem andern in einer gegen die guten "
            "Sitten verstossenden Weise absichtlich Schaden zufügt.")
ART41_FR = ("1 Celui qui cause, d'une manière illicite, un dommage à autrui, soit intentionnellement, "
            "soit par négligence ou imprudence, est tenu de le réparer.\n"
            "2 Celui qui cause intentionnellement un dommage à autrui par des faits contraires aux "
            "moeurs est également tenu de le réparer.")
ART41_IT = ("1 Chiunque è tenuto a riparare il danno illecitamente cagionato ad altri sia con "
            "intenzione, sia per negligenza od imprudenza.\n"
            "2 Parimente è chiamato a risarcire il danno chi lo cagiona intenzionalmente con atti "
            "contrari ai buoni costumi.")
P1 = ("1 Nach Ablauf der Probezeit darf der Arbeitgeber das Arbeitsverhältnis nicht kündigen: "
      "a. während die andere Partei schweizerischen obligatorischen Militär- oder Schutzdienst oder "
      "schweizerischen Zivildienst leistet, sowie, sofern die Dienstleistung mehr als elf Tage dauert, "
      "während vier Wochen vorher und nachher; b. während der Arbeitnehmer ohne eigenes Verschulden "
      "durch Krankheit oder durch Unfall ganz oder teilweise an der Arbeitsleistung verhindert ist, "
      "und zwar im ersten Dienstjahr während 30 Tagen, ab zweitem bis und mit fünftem Dienstjahr "
      "während 90 Tagen und ab sechstem Dienstjahr während 180 Tagen;")
P2 = ("2 Die Kündigung, die während einer der in Absatz 1 festgesetzten Sperrfristen erklärt wird, "
      "ist nichtig; ist dagegen die Kündigung vor Beginn einer solchen Frist erfolgt, aber die "
      "Kündigungsfrist bis dahin noch nicht abgelaufen, so wird deren Ablauf unterbrochen und erst "
      "nach Beendigung der Sperrfrist fortgesetzt.")
P3 = ("3 Gilt für die Beendigung des Arbeitsverhältnisses ein Endtermin, wie das Ende eines Monats "
      "oder einer Arbeitswoche, und fällt dieser nicht mit dem Ende der fortgesetzten Kündigungsfrist "
      "zusammen, so verlängert sich diese bis zum nächstfolgenden Endtermin.")
ART336C_DE = "\n".join((P1, P2, P3))
assert ART336C_DE.index(P3) > 600, "fixture must place paragraph 3 beyond the old 600-char slice"


def _make_conn() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript("""
        CREATE TABLE laws (sr_number TEXT PRIMARY KEY, title_de TEXT, title_fr TEXT,
            title_it TEXT, abbr_de TEXT, abbr_fr TEXT, abbr_it TEXT, consolidation_date TEXT);
        CREATE TABLE articles (sr_number TEXT, article_num TEXT, heading TEXT, text TEXT, lang TEXT);
    """)
    con.execute("INSERT INTO laws VALUES ('220','Obligationenrecht','Code des obligations',"
                "'Codice delle obbligazioni','OR','CO','CO','2026-01-01')")
    con.executemany("INSERT INTO articles VALUES (?,?,?,?,?)", [
        ("220", "41", None, ART41_DE, "de"),
        ("220", "41", None, ART41_FR, "fr"),
        ("220", "41", None, ART41_IT, "it"),
        ("220", "336c", None, ART336C_DE, "de"),
    ])
    return con


@pytest.fixture
def statutes(monkeypatch, tmp_path):
    opens = {"n": 0}

    def conn():
        opens["n"] += 1
        return _make_conn()

    fake_db = tmp_path / "statutes.db"
    fake_db.touch()
    monkeypatch.setattr(m, "STATUTES_DB_PATH", fake_db)
    monkeypatch.setattr(m, "_get_statutes_conn", conn)
    monkeypatch.setattr(m, "_statute_text_cache", {})
    return opens


def _quote_issues(draft: str) -> int:
    res = m._handle_attest_response(draft_text=draft, audit_quotes=True)
    return res["issues_by_category"]["quote"]


def test_correct_french_statute_quote_passes(statutes):
    good = ("Nach art. 41 al. 1 CO gilt: «Celui qui cause, d'une manière illicite, un dommage à "
            "autrui, soit intentionnellement, soit par négligence ou imprudence, est tenu de le réparer.»")
    assert _quote_issues(good) == 0
    bad = good.replace("un dommage à autrui", "un préjudice à autrui")
    assert _quote_issues(bad) == 1


def test_french_code_des_obligations_is_audited_not_blacklisted(statutes):
    """'CO' was listed among the paragraph markers (Italian 'co.'), so every
    FR/IT reference to the Code des obligations was skipped by both the
    statute audit and the quote pool."""
    assert "CO" not in m._STATUTE_AUDIT_INVALID_LAWS
    assert [p["lang"] for p in m._statute_source_pool("art. 41 al. 1 CO")] == ["de", "fr", "it"]
    assert m._audit_statutes("Selon l'art. 41 al. 1 CO, le dommage doit être réparé.") == []
    # A wrong article number in a CO reference is now caught.
    problems = [i["problem"] for i in m._audit_statutes("Selon l'art. 999 CO, le dommage doit être réparé.")]
    assert problems == ["article_not_in_law"]
    # Lowercase Italian "co." stays a paragraph marker, never a law.
    hit = m._STATUTE_AUDIT_PATTERN.search("art. 41 co. 1 CO")
    assert hit.group("article") == "41" and hit.group("law") == "CO"


def test_correct_italian_statute_quote_passes(statutes):
    good = ("Secondo l'art. 41 cpv. 1 CO: «Chiunque è tenuto a riparare il danno illecitamente "
            "cagionato ad altri sia con intenzione, sia per negligenza od imprudenza.»")
    assert _quote_issues(good) == 0
    assert _quote_issues(good.replace("per negligenza", "per colpa")) == 1


def test_german_quote_beyond_the_old_600_char_slice_passes(statutes):
    good = f"Art. 336c Abs. 3 OR: „{P3}“"
    assert _quote_issues(good) == 0
    assert _quote_issues(good.replace("Endtermin", "Endzeitpunkt")) == 1


def test_source_pool_holds_every_language_once(statutes):
    pool = m._statute_source_pool("Art. 41 OR")
    assert [p["lang"] for p in pool] == ["de", "fr", "it"]
    assert all(p["decision_id"] == "statute:OR_41" for p in pool)
    assert pool[1]["regeste"] == ART41_FR
    # A German-only article is pooled once, not three times via the fallback.
    pool = m._statute_source_pool("Art. 336c OR")
    assert [p["lang"] for p in pool] == ["de"]
    assert pool[0]["regeste"] == ART336C_DE  # untruncated


def test_fetch_statute_text_serves_language_and_full_text(statutes):
    fr = m._fetch_statute_text(law_code="CO", article="41", lang="fr")
    assert fr["lang_served"] == "fr" and fr["text"] == ART41_FR and "text_de" not in fr
    de = m._fetch_statute_text(law_code="OR", article="336c")
    assert de["lang_served"] == "de"
    assert len(de["text"]) == 600 and de["text_de"] == ART336C_DE[:600]
    full = m._fetch_statute_text(law_code="OR", article="336c", full=True)
    assert full["text"] == ART336C_DE and len(full["text_de"]) == 600
    # Missing translation falls back to German and says so.
    it = m._fetch_statute_text(law_code="OR", article="336c", lang="it")
    assert it["lang_served"] == "de" and it["text_de"]


def test_fetch_statute_text_cache_key_includes_lang_and_full(statutes):
    m._fetch_statute_text(law_code="OR", article="41")
    m._fetch_statute_text(law_code="OR", article="41", lang="fr")
    m._fetch_statute_text(law_code="OR", article="41", full=True)
    assert statutes["n"] == 3
    m._fetch_statute_text(law_code="OR", article="41")
    m._fetch_statute_text(law_code="OR", article="41", lang="fr")
    assert statutes["n"] == 3


def test_audit_statutes_accepts_an_article_served_in_another_language(statutes, monkeypatch):
    monkeypatch.setattr(m, "_fetch_statute_text",
                        lambda **kw: {"sr_number": "220", "lang_served": "fr", "text": "x"})
    assert [i for i in m._audit_statutes("Art. 41 CO") if i["problem"] == "article_not_in_law"] == []


def test_space_split_letter_suffix_is_detected_and_normalised():
    """Our own corpus printed 'Art. 85a' as 'Art. 85 a' until the 2026-09
    parser fix; a draft copied from get_law must still be audited."""
    hit = m._STATUTE_AUDIT_PATTERN.search("vgl. Art. 85 a BV zur Nationalstrassenabgabe")
    assert hit and hit.group("law") == "BV"
    assert re.sub(r"\s+", "", hit.group("article")) == "85a"
    # Subdivision markers and connectives never leak into the article slot.
    hit = m._STATUTE_AUDIT_PATTERN.search("selon l'art. 4 al. 1 CO")
    assert hit.group("article") == "4" and hit.group("law") == "CO"
    hit = m._STATUTE_AUDIT_PATTERN.search("Art. 32 Abs. 1 OR sind klar.")
    assert hit.group("article") == "32"
    # The connective never becomes a law and the second reference still parses
    # ("Art. 18 Abs" with law "Abs" is a pre-existing first match, filtered
    # downstream by _STATUTE_AUDIT_INVALID_LAWS).
    hits = {h.group("law"): h.group("article")
            for h in m._STATUTE_AUDIT_PATTERN.finditer("Art. 18 Abs. 1 und Art. 32 Abs. 1 OR")}
    assert hits.get("OR") == "32" and "und" not in hits


def test_audit_quotes_is_declared_default_on_and_description_is_truthful():
    # 2026-09-05: on by default for the MCP tool (0.0 % FP measured on the
    # rebuilt statutes.db); the description must say so and name the opt-out.
    tools = m._list_tools()
    attest = next(t for t in tools if t.name == "attest_response")
    prop = attest.inputSchema["properties"]["audit_quotes"]
    assert prop["type"] == "boolean" and prop["default"] is True
    assert "60-400" in prop["description"]
    assert "On by default" in prop["description"]
    assert "Pass false" in prop["description"]
    assert "audit_quotes, on by default" in attest.description
    assert "60-400" in attest.description
    assert "30+ chars" not in attest.description
    assert "with audit_quotes=true" not in attest.description

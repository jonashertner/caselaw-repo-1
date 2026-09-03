"""Transitional / final-provision articles no longer collide with the main body.

Before the 2026-09 rebuild `parse_xml` swept every `<article>` in a Fedlex
file into one namespace, so OR Art. 1 (de) had 14 rows and get_law(220, 2)
returned 13 "Art. 2" blocks. The rebuilt statutes.db tags those rows with
their eId prefix (`section`) and the block heading; these tests pin what the
read side does with that: main body first, other blocks listed, a number that
exists only in a block served with a label, list counts split, search hits
labelled, the quote rail and the "Art. N ABBR" resolver reading the main body.

The fixture is built with the real schema (tests/_statutes_fixture.py). The
old-schema path stays covered by test_get_law_lang_fallback.py, whose
hand-written table has no `section` column.
"""
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
for p in (REPO, REPO / "tests"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import mcp_server as m  # noqa: E402
from _statutes_fixture import make_statutes_conn  # noqa: E402

SB_1962 = "Schlussbestimmungen der Änderung vom 23. März 1962"
SB_1971 = "Schlussbestimmungen der Änderung vom 4. Oktober 1971"
UEB_2008 = "Übergangsbestimmungen der Änderung vom 19. Dezember 2008"

ROWS = [
    {"sr_number": "220", "article_num": "1", "heading": "A. Abschluss des Vertrages",
     "text": "1 Zum Abschlusse eines Vertrages ist die übereinstimmende gegenseitige Willensäusserung der Parteien erforderlich."},
    {"sr_number": "220", "article_num": "1", "section": "disp_u2", "section_heading": SB_1962,
     "text": "Der Schlusstitel des Zivilgesetzbuches gilt für dieses Gesetz."},
    {"sr_number": "220", "article_num": "1", "section": "disp_u3", "section_heading": SB_1971,
     "text": "Die neuen Bestimmungen gelten ab Inkrafttreten."},
    {"sr_number": "220", "article_num": "2", "heading": "Nebenpunkte",
     "text": "1 Haben sich die Parteien über alle wesentlichen Punkte geeinigt, so wird vermutet, dass der Vorbehalt von Nebenpunkten die Verbindlichkeit des Vertrages nicht hindern solle."},
    {"sr_number": "220", "article_num": "24", "heading": "Fälle des Irrtums",
     "text": "1 Der Irrtum ist namentlich in folgenden Fällen ein wesentlicher: 1. wenn der Irrende einen andern Vertrag eingehen wollte."},
    {"sr_number": "220", "article_num": "5", "section": "disp_u9", "section_heading": UEB_2008,
     "text": "Diese Nummer gibt es nur in den Übergangsbestimmungen."},
    {"sr_number": "220", "article_num": "1", "lang": "fr", "heading": "A. Conclusion du contrat",
     "text": "1 Le contrat est parfait lorsque les parties ont, réciproquement et d'une manière concordante, manifesté leur volonté."},
]


def _conn() -> sqlite3.Connection:
    conn = make_statutes_conn(ROWS)
    conn.row_factory = sqlite3.Row
    return conn


@pytest.fixture
def statutes(monkeypatch, tmp_path):
    fake = tmp_path / "statutes.db"
    fake.touch()
    monkeypatch.setattr(m, "STATUTES_DB_PATH", fake)
    monkeypatch.setattr(m, "_get_statutes_conn", _conn)
    monkeypatch.setattr(m, "_statute_text_cache", {})
    monkeypatch.setattr(m, "_FEDLEX_WORK_URI_MAP", None)
    monkeypatch.setattr(m, "_fetch_pending_changes", lambda sr: [])
    monkeypatch.setattr(m, "_get_materialien_for_doctrine", lambda *a, **k: None, raising=False)


def test_main_body_article_wins_and_other_blocks_are_listed(statutes):
    res = m.get_law(sr_number="220", article="1")
    assert len(res["articles"]) == 1
    assert res["articles"][0]["text"].startswith("1 Zum Abschlusse eines Vertrages")
    assert res["articles"][0]["section"] == ""
    assert res["also_in_sections"] == [
        {"section": "disp_u2", "section_heading": SB_1962},
        {"section": "disp_u3", "section_heading": SB_1971},
    ]
    text = m._format_get_law_response(res)
    assert text.count("### Art. 1") == 1
    assert f"Note: Art. 1 also exists in 2 transitional/final-provision block(s): {SB_1962}; {SB_1971}." in text


def test_number_that_exists_only_in_a_block_is_served_with_a_label(statutes):
    res = m.get_law(sr_number="220", article="5")
    (art,) = res["articles"]
    assert art["section"] == "disp_u9" and art["section_heading"] == UEB_2008
    assert res["article_section_note"].startswith("Art. 5 exists only in the transitional / final provisions")
    text = m._format_get_law_response(res)
    assert f"### Art. 5 ({UEB_2008}, disp_u9)" in text
    assert "Note: Art. 5 exists only in the transitional / final provisions" in text


def test_article_24_is_one_block_again(statutes):
    res = m.get_law(sr_number="220", article="24")
    assert [a["text"][:20] for a in res["articles"]] == ["1 Der Irrtum ist nam"]
    assert "also_in_sections" not in res


def test_article_list_counts_the_main_body_and_groups_the_blocks(statutes):
    res = m.get_law(sr_number="220")
    assert res["article_count"] == 3 and res["transitional_count"] == 3
    nums = [(a["article_num"], a.get("section", "")) for a in res["articles"]]
    assert nums == [("1", ""), ("2", ""), ("24", ""), ("1", "disp_u2"), ("1", "disp_u3"), ("5", "disp_u9")]
    text = m._format_get_law_response(res)
    assert "**3 articles** (+3 in transitional / final provisions)" in text
    assert f"#### {SB_1962} (disp_u2)" in text and f"#### {UEB_2008} (disp_u9)" in text
    assert text.index("- Art. 24") < text.index(f"#### {SB_1962}")


def test_french_main_body_untouched_by_german_blocks(statutes):
    res = m.get_law(sr_number="220", article="1", language="fr")
    assert len(res["articles"]) == 1 and res["articles"][0]["text"].startswith("1 Le contrat est parfait")
    assert "also_in_sections" not in res


def test_quote_rail_source_is_the_main_body(statutes):
    stat = m._fetch_statute_text(law_code="OR", article="1", full=True)
    assert stat["text"].startswith("1 Zum Abschlusse") and stat["lang_served"] == "de"


def test_article_reference_resolver_uses_the_main_body(statutes):
    hits = m._article_reference_lookup_federal("Art. 1 OR", "de", conn=_conn())
    assert len(hits) == 1
    blob = " ".join(str(v) for v in hits[0].values())
    assert "Zum Abschlusse" in blob and "Schlusstitel" not in blob


def test_search_hits_in_blocks_are_keyed_and_labelled_separately(statutes):
    hits = m._search_laws_federal('"Zivilgesetzbuches"', sr_number="220", language="de", limit=10)
    assert [(h["article_num"], h.get("section")) for h in hits] == [("1", "disp_u2")]
    assert hits[0]["section_heading"] == SB_1962
    rendered = m._format_search_laws_response(
        {"query": "Zivilgesetzbuches", "count": 1, "federal_hits": 1, "cantonal_hits": 0, "results": hits})
    assert f"**1. [CH] Art. 1 OR** (SR 220) [{SB_1962}]" in rendered
    # Main-body and block hits for the same number both survive the dedup.
    hits = m._search_laws_federal('Vertrages OR Zivilgesetzbuches', sr_number="220", language="de", limit=10)
    keys = {(h["article_num"], h.get("section", "")) for h in hits}
    assert {("1", ""), ("1", "disp_u2")} <= keys

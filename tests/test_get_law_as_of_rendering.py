"""Historical get_law answers are attributable (2026-09 statute gap report).

Before: a get_law(as_of=...) result rendered as "# SR 235.1 / Jurisdiction:
Bund (federal)" followed by article text; no title, no edition date, no link.
A repealed act served under a reused SR number was indistinguishable from
current law. pending_changes existed in the dict and never reached the text,
so system-prompt rule R5 could not be followed over MCP.

These tests drive _format_get_law_response with ready-made dicts (pure) and
pin the REST wire schema by scanning the source, as test_get_law_source_url
does. Offline.
"""
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

DSG_1992 = "https://fedlex.data.admin.ch/eli/cc/1993/1945_1945_1945"


def _hist(**over):
    base = {
        "sr_number": "235.1", "abbreviation": "DSG",
        "title": "Bundesgesetz vom 19. Juni 1992 über den Datenschutz",
        "canton": "CH", "level": "federal", "language": "de",
        "version": "historical", "as_of": "2020-01-01", "snapshot_date": "2019-03-01",
        "work_uri": DSG_1992, "work_entry_in_force": "1993-07-01",
        "work_no_longer_in_force": "2023-09-01", "work_in_force_status": "repealed",
        "fedlex_snapshot_uri": DSG_1992 + "/20190301",
        "source_url": "https://www.fedlex.admin.ch/eli/cc/1993/1945_1945_1945/20190301/de",
        "source_label": "Fedlex (Fassung vom 2019-03-01)",
        "text_source": "fedlex_xml", "structure": "articles",
        "verbatim_quotation": "verbatim", "formats_available": ["xml"],
        "articles": [{
            "article_num": "4", "heading": "Grundsätze",
            "text": "1 Personendaten dürfen nur rechtmässig bearbeitet werden.",
            "footnote": "Fassung gemäss Ziff. I des BG vom 24. März 2006 (AS 2007 4983).",
            "section": "",
        }],
    }
    base.update(over)
    return base


def test_historical_answer_names_edition_window_source_and_link():
    text = m._format_get_law_response(_hist())
    assert text.startswith("# DSG — SR 235.1\n")
    assert "**Bundesgesetz vom 19. Juni 1992 über den Datenschutz**" in text
    assert "Version: HISTORICAL — Fedlex edition of 2019-03-01, applicable on 2020-01-01" in text
    assert "Act in force: 1993-07-01 – 2023-09-01 (repealed)" in text
    assert "Text source: Fedlex Akoma Ntoso XML — per-article, verbatim" in text
    assert ("Quelle: [Fedlex (Fassung vom 2019-03-01)]"
            "(https://www.fedlex.admin.ch/eli/cc/1993/1945_1945_1945/20190301/de)") in text
    assert "### Art. 4 — Grundsätze\n\n1 Personendaten dürfen nur rechtmässig bearbeitet werden.\n\n" in text
    assert "> Footnote: Fassung gemäss Ziff. I des BG vom 24. März 2006 (AS 2007 4983)." in text
    # The amendment note is rendered as a footnote, never inside the provision.
    body = text.split("### Art. 4 — Grundsätze")[1].split("> Footnote")[0]
    assert "Fassung gemäss" not in body


def test_repealed_act_without_recorded_end_date_carries_a_caveat():
    text = m._format_get_law_response(_hist(work_no_longer_in_force=None))
    assert ("Act in force: 1993-07-01 – ? (repealed; end date not recorded on Fedlex — "
            "verify this edition applied on 2020-01-01)") in text


def test_live_act_renders_an_open_window():
    text = m._format_get_law_response(_hist(
        work_in_force_status="in_force", work_no_longer_in_force=None,
        work_entry_in_force="2023-09-01", snapshot_date="2023-09-01", as_of="2024-01-01"))
    assert "Act in force: 2023-09-01 – (open)\n" in text
    assert "(repealed)" not in text


def test_current_law_rendering_has_no_version_block():
    current = {"sr_number": "220", "abbreviation": "OR", "title": "Obligationenrecht",
               "canton": "CH", "level": "federal", "language": "de",
               "consolidation_date": "2026-01-01",
               "source_url": "https://www.fedlex.admin.ch/eli/cc/27/317_321_377/de#art_41",
               "source_label": "Fedlex",
               "articles": [{"article_num": "41", "heading": None, "text": "Wer einem andern..."}]}
    text = m._format_get_law_response(current)
    assert "Consolidation date: 2026-01-01" in text
    assert "Version:" not in text and "Act in force:" not in text and "Text source:" not in text
    assert "Footnote" not in text


def test_pending_changes_are_rendered_on_the_current_path_and_only_when_present():
    current = {"sr_number": "910.1", "abbreviation": "LwG", "title": "Landwirtschaftsgesetz",
               "canton": "CH", "level": "federal", "language": "de",
               "articles": [{"article_num": "1", "heading": None, "text": "Der Bund sorgt dafür..."}]}
    silent = m._format_get_law_response(current)
    assert "Pending changes" not in silent
    current["pending_changes"] = [{"date": "2027-07-01"}, {"date": "2028-01-01"}]
    text = m._format_get_law_response(current)
    assert ("Pending changes: a consolidation of this act enters into force on 2027-07-01 "
            "(also: 2028-01-01); it does not necessarily touch the article shown. "
            "Verify the affected articles on Fedlex.") in text
    current["pending_changes"] = [{"date": "2027-07-01"}]
    assert "on 2027-07-01; it does not necessarily touch" in m._format_get_law_response(current)


def test_prefix_match_is_announced():
    text = m._format_get_law_response(_hist(
        articles=[{"article_num": "5a", "heading": "Begriffe", "text": "In diesem Gesetz...", "section": ""}],
        article_match={"requested": "5", "matched": ["5a"], "method": "prefix"}))
    assert "Note: no exact Art. 5 in this edition; showing prefix matches 5a." in text


def test_missing_article_note_precedes_no_articles_found():
    text = m._format_get_law_response(_hist(
        articles=[], note="Art. 99 is not in this edition. Nearest article numbers: 5a, 4, 1."))
    assert "Note: Art. 99 is not in this edition. Nearest article numbers: 5a, 4, 1." in text
    assert text.rstrip().endswith("No articles found.")


def test_transitional_article_is_labelled_with_its_block():
    text = m._format_get_law_response(_hist(articles=[
        {"article_num": "1", "heading": None, "text": "Laufende Verfahren...", "section": "disp_u2"}]))
    assert "### Art. 1 (transitional/final provisions, disp_u2)\n" in text
    text = m._format_get_law_response(_hist(articles=[
        {"article_num": "1", "heading": None, "text": "...", "section": "disp_u1",
         "section_heading": "Schlusstitel: Anwendungs- und Einführungsbestimmungen"}]))
    assert "### Art. 1 (Schlusstitel: Anwendungs- und Einführungsbestimmungen, disp_u1)\n" in text


def test_error_dict_is_rendered_verbatim():
    assert m._format_get_law_response({"error": "No Fedlex act carried SR 999.9 on 2020-01-01."}) \
        == "No Fedlex act carried SR 999.9 on 2020-01-01."


def _laws_wire_schema_block() -> str:
    src = (REPO / "mcp_server.py").read_text(encoding="utf-8")
    marker = '("GET", "/laws/{abbreviation}"): {'
    start = src.find(marker)
    assert start != -1, "wire schema block for /laws/{abbreviation} not found"
    end = src.find("/laws/search", start)
    return src[start:end]


def test_wire_schema_declares_the_historical_provenance_fields():
    block = _laws_wire_schema_block()
    for field in ("version", "as_of", "snapshot_date", "work_uri", "work_entry_in_force",
                  "work_no_longer_in_force", "work_in_force_status", "fedlex_snapshot_uri",
                  "text_source", "structure", "verbatim_quotation", "formats_available",
                  "pending_changes", "footnote", "section"):
        assert f'"{field}"' in block, field


def test_format_xml_404_no_longer_blames_statutes_db_for_historical_editions():
    src = (REPO / "mcp_server.py").read_text(encoding="utf-8")
    assert "an as_of edition" in src and "carries it only when Fedlex has XML for that edition" in src
    assert "rebuild statutes.db to populate it.\"" not in src

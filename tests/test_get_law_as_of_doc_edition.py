"""get_law(as_of=...) served from a Fedlex Word edition (behind a flag).

Order of preference is XML, then the .doc when SWISS_CASELAW_HIST_DOC=1 and
a style-preserving converter exists on the host, then pdf-a. The Word path
returns per-article text flagged verbatim_quotation="best_effort"; every
other combination falls through to the PDF path or its error. Offline: the
SPARQL seam, requests.get, the converter probe and the conversion itself are
stubbed; the split runs on real textutil markup from the fixture.
"""
import sys
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

FIXTURE_HTML = (REPO / "tests" / "fixtures" / "fedlex_or2017_doc_excerpt.html").read_text(encoding="utf-8")
OR_WORK = "https://fedlex.data.admin.ch/eli/cc/27/317_321_377"
SNAP = OR_WORK + "/20170401"
FMT = "https://fedlex.data.admin.ch/vocabulary/user-format/"
IN_FORCE = "https://fedlex.data.admin.ch/vocabulary/enforcement-status/0"
EXPR = {SNAP: [
    {"fmt": FMT + "doc", "url": "https://fedlex.data.admin.ch/filestore/or-2017.doc",
     "title": "Obligationenrecht", "abbr": "OR"},
    {"fmt": FMT + "pdf-a", "url": "https://fedlex.data.admin.ch/filestore/or-2017.pdf",
     "title": "Obligationenrecht", "abbr": "OR"},
]}


def _stub(monkeypatch, *, flag=True, converter="/usr/bin/textutil", html=FIXTURE_HTML, keys=None):
    def fake(query, timeout=15):
        if "historicalLegalId" in query:
            return [{"work": OR_WORK, "eif": "1912-01-01", "status": IN_FORCE}]
        if "isMemberOf" in query:
            return [{"snapshot": SNAP, "date": "2017-04-01"}]
        if "isRealizedBy" in query:
            return EXPR.get(SNAP, []) if f"<{SNAP}>" in query else []
        raise AssertionError(query)

    class _Resp:
        def __init__(self, url):
            self.status_code = 200
            self.content = b"%PDF-1.4 x" if url.endswith(".pdf") else b"\xd0\xcf\x11\xe0 word"

    monkeypatch.setattr(m, "_fedlex_sparql_select", fake)
    monkeypatch.setattr(requests, "get", lambda url, timeout=30: _Resp(url))
    monkeypatch.setattr(m, "_HIST_DOC_ENABLED", flag)
    monkeypatch.setattr(m, "_doc_converter", lambda: converter)
    monkeypatch.setattr(m, "_convert_doc_to_html", lambda content: html)
    monkeypatch.setattr(m, "_extract_pdf_text", lambda content: ("Art. 60\n1 PDF-Fassung.\nArt. 61\nx", 1))
    monkeypatch.setattr(m, "_lexfind_cache_get", lambda k: None)
    monkeypatch.setattr(m, "_lexfind_cache_set",
                        (lambda k, v: keys.append(k)) if keys is not None else (lambda k, v: None))


def test_doc_edition_is_served_per_article_as_best_effort(monkeypatch):
    keys = []
    _stub(monkeypatch, keys=keys)
    res = m.get_law(sr_number="220", article="60", as_of="2019-06-01")
    assert res["text_source"] == "fedlex_doc"
    assert res["structure"] == "articles" and res["verbatim_quotation"] == "best_effort"
    assert res["formats_available"] == ["doc", "pdf-a"]
    (art,) = res["articles"]
    assert art["article_num"] == "60" and art["heading"] == "G. Verjährung"
    assert art["text"].startswith("1 Der Anspruch auf Schadenersatz oder Genugtuung verjährt in einem Jahre")
    assert "hist_edition:v1:220:2017-04-01:de:doc" in keys
    text = m._format_get_law_response(res)
    assert "Text source: Fedlex Word (.doc) edition" in text and "best effort" in text
    assert "### Art. 60 — G. Verjährung" in text


def test_prefix_and_missing_article_behave_like_the_xml_path(monkeypatch):
    _stub(monkeypatch)
    res = m.get_law(sr_number="220", article="5", as_of="2019-06-01")
    assert [a["article_num"] for a in res["articles"]] == ["59", "59a"]
    assert res["article_match"]["method"] == "prefix"
    res = m.get_law(sr_number="220", article="999", as_of="2019-06-01")
    assert res["articles"] == [] and res["note"].startswith("Art. 999 is not in this edition")


def test_flag_off_falls_through_to_pdf(monkeypatch):
    _stub(monkeypatch, flag=False)
    res = m.get_law(sr_number="220", article="60", as_of="2019-06-01")
    assert res["text_source"] == "fedlex_pdf" and res["verbatim_quotation"] == "not_guaranteed"


def test_no_converter_falls_through_to_pdf(monkeypatch):
    _stub(monkeypatch, converter=None)
    assert m.get_law(sr_number="220", article="60", as_of="2019-06-01")["text_source"] == "fedlex_pdf"


def test_unrecoverable_structure_falls_through_to_pdf(monkeypatch):
    _stub(monkeypatch, html="<html><body><p class=\"x\">no article styles here</p></body></html>")
    assert m.get_law(sr_number="220", article="60", as_of="2019-06-01")["text_source"] == "fedlex_pdf"


def test_system_prompt_allows_best_effort_quotes_only_with_the_edition_date():
    src = (REPO / "mcp_server.py").read_text(encoding="utf-8")
    r2 = src[src.index('"R2. NEVER write a direct quotation'):]
    r2 = r2[:r2.index('"R3. NEVER')]
    assert "best_effort" in r2 and "edition date stated" in r2

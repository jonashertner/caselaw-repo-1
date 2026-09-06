"""get_law(as_of=...) on PDF-only editions: glued footnote superscripts and
the honesty guard.

Bug (verified live 2026-09-05): `GET /api/laws/OR?article=336c&as_of=2010-01-01`
answered text_source fedlex_pdf, articles[0].text = "Art. 336c \\nb. durch den
Arbeitnehmer" — the table-of-contents line carrying the marginal note of
Art. 336d — with a success status. In the Fedlex 2010 OR PDF the body header
reads "Art. 336c147" (footnote 147 glued to the number), which neither the
article marker nor the any-article marker matched, so only the TOC line was
a candidate.

Fixture: the lines below are the pymupdf text of that edition (SR 220,
Fassung vom 2010-01-01, de), verbatim including trailing spaces, cut down
to the runs that matter: the Art. 47–50 run (a repealed article, "Art.
4817", whose glued digits also spell Art. 481), Art. 33, Art. 481, the
Art. 336–337 run with glued headers, Art. 1118 (footnote glued after the
period) and the table of contents at the end of the document. Offline:
the SPARQL seam, requests.get and the PDF extractor are stubbed.
"""
import sys
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mcp_server as m  # noqa: E402

BODY_47_50 = [
    "Art. 47 ",
    "Bei Tötung eines Menschen oder Körperverletzung kann der Richter ",
    "unter Würdigung der besonderen Umstände dem Verletzten oder den ",
    "Angehörigen des Getöteten eine angemessene Geldsumme als Genug-",
    "tuung zusprechen. ",
    "Art. 4817 ",
    "Art. 4918 ",
    "1 Wer in seiner Persönlichkeit widerrechtlich verletzt wird, hat An-",
    "spruch auf Leistung einer Geldsumme als Genugtuung, sofern die ",
    "Schwere der Verletzung es rechtfertigt und diese nicht anders wieder-",
    "gutgemacht worden ist. ",
    "2 Anstatt oder neben dieser Leistung kann der Richter auch auf eine ",
    "andere Art der Genugtuung erkennen. ",
    "Art. 50 ",
    "1 Haben mehrere den Schaden gemeinsam verschuldet, sei es als ",
    "Anstifter, Urheber oder Gehilfen, so haften sie dem Geschädigten ",
    "solidarisch. ",
]

BODY_33 = [
    "Art. 33 ",
    "1 Soweit die Ermächtigung, im Namen eines andern Rechtshandlungen ",
    "vorzunehmen, aus Verhältnissen des öffentlichen Rechtes hervorgeht, ",
    "ist sie nach den Vorschriften des öffentlichen Rechtes des Bundes und ",
    "der Kantone zu beurteilen. ",
    "2 Ist die Ermächtigung durch Rechtsgeschäft eingeräumt, so beurteilt ",
    "sich ihr Umfang nach dessen Inhalt. ",
    "Art. 34 ",
    "1 Eine durch Rechtsgeschäft erteilte Ermächtigung kann vom Vollmacht-",
    "geber jederzeit beschränkt oder widerrufen werden. ",
]

BODY_481 = [
    "Art. 481 ",
    "1 Ist Geld mit der ausdrücklichen oder stillschweigenden Vereinbarung ",
    "hinterlegt worden, dass der Aufbewahrer nicht dieselben Stücke, son-",
    "dern nur die gleiche Geldsumme zurückzuerstatten habe, so geht Nut-",
    "zen und Gefahr auf ihn über. ",
    "Art. 482 ",
    "1 Ein Lagerhalter, der sich öffentlich zur Aufbewahrung von Waren ",
    "anerbietet, kann von der zuständigen Behörde die Bewilligung erhalten. ",
]

# Page break with the two-column marginal notes bleeding in, then the
# amended labour-law articles: every body header carries its footnote.
BODY_336 = [
    "3. Konsultation  ",
    "der Arbeit-",
    "nehmer-",
    "vertretung ",
    "4. Verfahren ",
    "",
    "Obligationenrecht ",
    "120 ",
    "220 ",
    "Art. 336140 ",
    "1 Die Kündigung eines Arbeitsverhältnisses ist missbräuchlich, wenn ",
    "eine Partei sie ausspricht: ",
    "a. ",
    "wegen einer Eigenschaft, die der anderen Partei kraft ihrer Per-",
    "sönlichkeit zusteht, es sei denn, diese Eigenschaft stehe in ei-",
    "nem Zusammenhang mit dem Arbeitsverhältnis oder beein-",
    "trächtige wesentlich die Zusammenarbeit im Betrieb; ",
    "Art. 336a144 ",
    "1 Die Partei, die das Arbeitsverhältnis missbräuchlich kündigt, hat der ",
    "anderen Partei eine Entschädigung auszurichten. ",
    "2 Die Entschädigung wird vom Richter unter Würdigung aller Um-",
    "stände festgesetzt, darf aber den Betrag nicht übersteigen, der dem ",
    "Lohn des Arbeitnehmers für sechs Monate entspricht. ",
    "Art. 336b146 ",
    "1 Wer gestützt auf Artikel 336 und 336a eine Entschädigung geltend ",
    "machen will, muss gegen die Kündigung längstens bis zum Ende der ",
    "Kündigungsfrist beim Kündigenden schriftlich Einsprache erheben. ",
    "Art. 336c147 ",
    "1 Nach Ablauf der Probezeit darf der Arbeitgeber das Arbeitsverhältnis ",
    "nicht kündigen: ",
    "a.148 während die andere Partei schweizerischen obligatorischen ",
    "Militär- oder Schutzdienst oder schweizerischen Zivildienst ",
    "leistet, sowie, sofern die Dienstleistung mehr als elf Tage dauert, ",
    "während vier Wochen vorher und nachher; ",
    "2 Die Kündigung, die während einer der vorstehend genannten Sperr-",
    "fristen erklärt wird, ist nichtig. ",
    "Art. 336d150 ",
    "1 Nach Ablauf der Probezeit darf der Arbeitnehmer das Arbeitsverhält-",
    "nis nicht kündigen, wenn ein Vorgesetzter, dessen Funktionen er aus-",
    "zuüben vermag, oder der Arbeitgeber selbst unter den in Artikel 336c ",
    "Absatz 1 Buchstabe a angeführten Voraussetzungen an der Ausübung ",
    "der Tätigkeit verhindert ist und der Arbeitnehmer dessen Tätigkeit ",
    "während der Verhinderung zu übernehmen hat. ",
    "Art. 337 ",
    "1 Aus wichtigen Gründen kann der Arbeitgeber wie der Arbeitnehmer ",
    "jederzeit das Arbeitsverhältnis fristlos auflösen; er muss die fristlose ",
    "Vertragsauflösung auf Verlangen schriftlich begründen. ",
]

BODY_1118 = [
    "Art. 1118 ",
    "Die Einlieferung in eine von der Schweizerischen Nationalbank aner-",
    "kannte Abrechnungsstelle steht der Vorlegung zur Zahlung gleich.544 ",
    "Art. 1119 ",
    "1 Ein Widerruf des Checks ist erst nach Ablauf der Vorlegungsfrist ",
    "wirksam. ",
]

# The table of contents: marginal note, then the article number. The line
# after "Art. 336c" is the marginal note of Art. 336d.
TOC = [
    "Inhaltsverzeichnis ",
    "b. Umfang der Ermächtigung ",
    "Art. 33 ",
    "2. Auf Grund von Rechtsgeschäft ",
    "Art. 34 ",
    "c. Leistung von Genugtuung ",
    "Art. 47 ",
    "2. … ",
    "Art. 48 ",
    "3. Bei Verletzung der Persönlichkeit ",
    "Art. 49 ",
    "VI. Haftung mehrerer ",
    "1. Bei unerlaubter Handlung ",
    "Art. 50 ",
    "2. Bei verschiedenen Rechtsgründen ",
    "B. Die Hinterlegung vertretbarer Sachen ",
    "Art. 481 ",
    "C. Lagergeschäft ",
    "I. Berechtigung zur Ausgabe von Warenpapieren ",
    "Art. 482 ",
    "II. Aufbewahrungspflicht des Lagerhalters ",
    "rbeitnehmervertretung ",
    "Art. 335f  ",
    "4. Verfahren ",
    "Art. 335g ",
    "III. Kündigungsschutz ",
    "1. Missbräuchliche Kündigung ",
    "a. Grundsatz ",
    "Art. 336 ",
    "b. Sanktionen ",
    "Art. 336a ",
    "c. Verfahren ",
    "Art. 336b ",
    "2. Kündigung zur Unzeit ",
    "a. durch den Arbeitgeber ",
    "Art. 336c ",
    "b. durch den Arbeitnehmer ",
    "Art. 336d ",
    "IV. Fristlose Auflösung ",
    "1. Voraussetzungen ",
    "a. aus wichtigen Gründen ",
    "Art. 337 ",
    "b. wegen Lohngefährdung ",
    "Art. 337a ",
    "4. Einlieferung in eine Abrechnungsstelle ",
    "Art. 1118 ",
    "5. Widerruf ",
    "a. Im Allgemeinen ",
    "Art. 1119 ",
    "b. Bei Tod, Handlungsunfähigkeit, Konkurs ",
]

EDITION_TEXT = "\n".join(
    ["Bundesgesetz betreffend die Ergänzung des Schweizerischen Zivilgesetzbuches "]
    + BODY_33 + BODY_47_50 + BODY_336 + BODY_481 + BODY_1118 + TOC)


# ── the excerpt helper ───────────────────────────────────────────────

def _excerpt(article):
    excerpt, n = m._pdf_article_excerpt(EDITION_TEXT, article)
    return excerpt, n


def test_336c_resolves_to_the_glued_body_header_not_the_toc_line():
    excerpt, n = _excerpt("336c")
    assert n == 2                                   # TOC line + body header
    assert excerpt.startswith("Art. 336c147 \n1 Nach Ablauf der Probezeit darf der Arbeitgeber")
    assert "b. durch den Arbeitnehmer" not in excerpt   # the 336d marginal note
    assert "Art. 336d150" not in excerpt            # the window stops at the next glued header
    assert "darf der Arbeitnehmer das Arbeitsverhält" not in excerpt
    assert excerpt.endswith("fristen erklärt wird, ist nichtig.")
    assert m._pdf_excerpt_status(excerpt) == "ok"


def test_336d_336_and_336a_resolve_the_same_way():
    excerpt, n = _excerpt("336d")
    assert n == 2 and excerpt.startswith("Art. 336d150 \n1 Nach Ablauf der Probezeit darf der Arbeitnehmer")
    assert "IV. Fristlose Auflösung" not in excerpt
    assert "Art. 337" not in excerpt
    excerpt, n = _excerpt("336")
    assert n == 2 and excerpt.startswith("Art. 336140 \n1 Die Kündigung eines Arbeitsverhältnisses")
    assert "Art. 336a144" not in excerpt
    excerpt, n = _excerpt("336a")
    assert n == 2 and excerpt.startswith("Art. 336a144 \n1 Die Partei, die das Arbeitsverhältnis")


def test_a_number_does_not_match_its_longer_neighbours():
    # "Art. 33" must not pick up "Art. 336 …" (TOC) or "Art. 336140" (body).
    excerpt, n = _excerpt("33")
    assert n == 2 and excerpt.startswith("Art. 33 \n1 Soweit die Ermächtigung")
    assert "Kündigung" not in excerpt
    # "Art. 336" must not match "Art. 336c147" / "Art. 336c".
    excerpt, _ = _excerpt("336")
    assert "Nach Ablauf der Probezeit" not in excerpt
    # "Art. 4817" is Art. 48 + footnote 17, not Art. 481 + footnote 7:
    # Art. 481 has its own clean header further on.
    excerpt, n = _excerpt("481")
    assert n == 2 and excerpt.startswith("Art. 481 \n1 Ist Geld mit der ausdrücklichen")
    assert "Art. 4817" not in excerpt
    # Art. 49's glued header "Art. 4918" spells Art. 491 too; the position
    # between Art. 47 and Art. 50 settles it.
    excerpt, n = _excerpt("49")
    assert n == 2 and excerpt.startswith("Art. 4918 \n1 Wer in seiner Persönlichkeit")
    assert m._pdf_excerpt_status(excerpt) == "ok"


def test_letter_suffix_is_exact():
    text = "\n".join([
        "Art. 5 ",
        "The text of article five, the plain number, one full sentence long. ",
        "Art. 5bis ",
        "The text of article five bis, the ordinal, one full sentence long. ",
        "Art. 5bis12 ",
        "The text of the amended article five bis, footnote twelve glued to the number, ",
        "runs to a second line and is therefore the longer of the two windows. ",
        "Art. 5c ",
        "The text of article five c, one full sentence long and nothing more. ",
        "Art. 5cbis ",
        "The text of article five c bis, one full sentence long and nothing more. ",
        "Art. 6 ",
        "The text of article six, one full sentence long and nothing more. ",
    ])
    excerpt, n = m._pdf_article_excerpt(text, "5")
    assert n == 1 and excerpt == "Art. 5 \nThe text of article five, the plain number, one full sentence long."
    excerpt, n = m._pdf_article_excerpt(text, "5c")
    assert n == 1 and excerpt == "Art. 5c \nThe text of article five c, one full sentence long and nothing more."
    excerpt, n = m._pdf_article_excerpt(text, "5bis")
    assert n == 2 and excerpt.startswith("Art. 5bis12 \n")     # longest ok block
    assert m._pdf_article_excerpt(text, "5cb") == (None, 0)
    assert m._pdf_article_excerpt(text, "") == (None, 0)


def test_glued_footnote_counts_only_at_the_end_of_the_line():
    text = "\n".join([
        "Art. 41 ",
        "Wer einem andern widerrechtlich Schaden zufügt, wird ersatzpflichtig. ",
        "Art. 4112 ",
        "Text of article 41, footnote 12 glued. ",
        "Art. 41123456 ",
        "Not a footnote: too many digits. ",
        "Art. 412 Abs. 2 gilt sinngemäss. ",
        "Art. 42 ",
        "Text of article 42. ",
    ])
    excerpt, n = m._pdf_article_excerpt(text, "41")
    assert n == 2
    assert excerpt.startswith("Art. 41 \nWer einem andern")            # longest ok block
    assert m._pdf_article_excerpt(text, "4112")[1] == 1                # the clean reading stays available


def test_any_article_marker_accepts_a_glued_header():
    for line in ("Art. 336c147 ", "Art. 336140", "Art. 325bis3 ", "Art. 41 ", "  Art. 7a"):
        assert m._PDF_ANY_ARTICLE_MARKER.match(line), line
    for line in ("Art. 336c147 weiter", "Art. 336c1478 ", "Artikel 336c", "Art. 336cx"):
        assert not m._PDF_ANY_ARTICLE_MARKER.match(line), line


def test_sort_key_orders_ordinals_and_letters():
    k = m._pdf_article_sort_key
    assert k("325") < k("325bis") < k("325ter") < k("326") < k("326a") < k("326b")
    assert k("336c") < k("336d") < k("337")


# ── the honesty guard ────────────────────────────────────────────────

def test_status_of_toc_and_repealed_windows():
    assert m._pdf_excerpt_status("Art. 336c \nb. durch den Arbeitnehmer") == "heading_only"
    assert m._pdf_excerpt_status(
        "Art. 280 \nD. Pflichten des Pächters \nI. Zahlung des Pachtzinses und der Nebenkosten \n"
        "1. Im Allgemeinen") == "heading_only"
    assert m._pdf_excerpt_status("Art. 4817") == "empty"
    assert m._pdf_excerpt_status("Art. 4817 \n \n") == "empty"
    assert m._pdf_excerpt_status("Art. 226a–226m146 \nAufgehoben") == "heading_only"


def test_status_of_article_windows():
    excerpt, _ = _excerpt("1118")             # footnote glued after the full stop
    assert excerpt.endswith("zur Zahlung gleich.544")
    assert m._pdf_excerpt_status(excerpt) == "ok"
    assert m._pdf_excerpt_status("Art. 3 \nDer Bundesrat bestimmt den Zeitpunkt des Inkrafttretens. ") == "ok"
    assert m._pdf_excerpt_status("Art. 5 \nDieses Gesetz tritt am 1. Januar 1912 in Kraft.") == "ok"


def test_repealed_article_is_flagged_not_answered():
    # Art. 48: the body header "Art. 4817" has nothing under it and the TOC
    # line only the marginal note of Art. 49. Neither is statute text.
    excerpt, n = _excerpt("48")
    assert n == 2                                   # not "Art. 481" / "Art. 482"
    assert excerpt == "Art. 48 \n3. Bei Verletzung der Persönlichkeit"
    assert m._pdf_excerpt_status(excerpt) == "heading_only"


def test_glued_header_at_a_numbering_restart():
    # Final provisions restart at Art. 1 right after the last body article;
    # "Art. 15" there is Art. 1 with footnote 5, while "Art. 12" — listed in
    # the table of contents — is Art. 12 and never Art. 1.
    text = "\n".join([
        "Art. 11 ",
        "The text of article eleven, one full sentence long and nothing more. ",
        "Art. 12 ",
        "The text of article twelve, one full sentence long and nothing more. ",
        "Schlussbestimmungen ",
        "Art. 15 ",
        "The text of transitional article one, footnote five glued to the number. ",
        "Art. 2 ",
        "The text of transitional article two, one full sentence long and more. ",
        "Inhaltsverzeichnis ",
        "Art. 11 ",
        "Art. 12 ",
        "Schlussbestimmungen ",
        "Art. 1 ",
        "Art. 2 ",
    ])
    excerpt, n = m._pdf_article_excerpt(text, "1")
    assert n == 2                                   # "Art. 15" + the TOC line
    assert excerpt.startswith("Art. 15 \nThe text of transitional article one")
    excerpt, n = m._pdf_article_excerpt(text, "12")
    assert n == 2 and excerpt.startswith("Art. 12 \nThe text of article twelve")
    excerpt, n = m._pdf_article_excerpt(text, "2")
    assert n == 2 and excerpt.startswith("Art. 2 \nThe text of transitional article two")


# ── get_law wiring (SPARQL, download and PDF extraction stubbed) ─────

OR_WORK = "https://fedlex.data.admin.ch/eli/cc/27/317_321_377"
SNAP_2010 = OR_WORK + "/20100101"
IN_FORCE = "https://fedlex.data.admin.ch/vocabulary/enforcement-status/0"
FMT = "https://fedlex.data.admin.ch/vocabulary/user-format/"
PDF_URL = "https://fedlex.data.admin.ch/filestore/or-2010.pdf"
EXPR_PDF_ONLY = {SNAP_2010: [
    {"fmt": FMT + "pdf-a", "url": PDF_URL, "title": "Obligationenrecht", "abbr": "OR"},
]}


def _stub(monkeypatch):
    def fake(query, timeout=15):
        if "historicalLegalId" in query:
            return [{"work": OR_WORK, "eif": "1912-01-01", "status": IN_FORCE}]
        if "isMemberOf" in query:
            return [{"snapshot": SNAP_2010, "date": "2010-01-01"}] if f"<{OR_WORK}>" in query else []
        if "isRealizedBy" in query:
            return EXPR_PDF_ONLY.get(SNAP_2010, []) if f"<{SNAP_2010}>" in query else []
        raise AssertionError("unexpected SPARQL: " + query)

    class _Resp:
        status_code = 200
        content = b"%PDF-1.3 synthetic"

    monkeypatch.setattr(m, "_fedlex_sparql_select", fake)
    monkeypatch.setattr(requests, "get", lambda url, timeout=30: _Resp())
    monkeypatch.setattr(m, "_extract_pdf_text", lambda content: (EDITION_TEXT, 502))
    monkeypatch.setattr(m, "_lexfind_cache_get", lambda k: None)
    monkeypatch.setattr(m, "_lexfind_cache_set", lambda k, v: None)


def test_get_law_336c_as_of_2010_returns_the_article_with_text_status_ok(monkeypatch):
    _stub(monkeypatch)
    res = m.get_law(sr_number="220", article="336c", as_of="2010-01-01")
    assert "error" not in res
    assert res["text_source"] == "fedlex_pdf"
    assert res["verbatim_quotation"] == "not_guaranteed"     # still not quotable
    assert res["text_status"] == "ok"
    assert res["excerpt_candidates"] == 2
    (art,) = res["articles"]
    assert art["article_num"] == "336c" and art["excerpt"] is True
    assert art["text_status"] == "ok"
    assert art["text"].startswith("Art. 336c147 \n1 Nach Ablauf der Probezeit darf der Arbeitgeber")
    assert "b. durch den Arbeitnehmer" not in art["text"]
    assert "note" not in res
    text = m._format_get_law_response(res)
    assert '### Excerpt around "Art. 336c" (unstructured PDF text, 1 of 2 candidate positions)' in text
    assert "UNRESOLVED" not in text
    assert "verbatim quotation NOT guaranteed" in text


def test_get_law_repealed_article_is_returned_flagged_with_a_note(monkeypatch):
    _stub(monkeypatch)
    res = m.get_law(sr_number="220", article="48", as_of="2010-01-01")
    assert "error" not in res
    assert res["text_status"] in ("heading_only", "empty")
    (art,) = res["articles"]                         # returned, not dropped …
    assert art["article_num"] == "48" and art["text_status"] == res["text_status"]
    assert res["note"].startswith("UNRESOLVED: the PDF excerpt around 'Art. 48' holds ")
    assert "Treat Art. 48 as not retrieved" in res["note"]
    assert res["source_url"] in res["note"]
    text = m._format_get_law_response(res)
    assert "Note: UNRESOLVED" in text
    assert f"_UNRESOLVED (text_status: {res['text_status']})" in text


def test_get_law_toc_only_article_is_heading_only(monkeypatch):
    # Art. 337a is listed in the table of contents only (its body is outside
    # the fixture): the window holds Art. 337a's line and nothing of it.
    _stub(monkeypatch)
    res = m.get_law(sr_number="220", article="337a", as_of="2010-01-01")
    assert res["text_status"] == "heading_only"
    assert res["articles"][0]["text"] == "Art. 337a \n4. Einlieferung in eine Abrechnungsstelle"
    assert "UNRESOLVED" in res["note"]


def test_get_law_missing_marker_still_returns_no_article(monkeypatch):
    _stub(monkeypatch)
    res = m.get_law(sr_number="220", article="999", as_of="2010-01-01")
    assert res["articles"] == [] and "text_status" not in res
    assert res["note"].startswith("No 'Art. 999' marker found in the PDF text")


# ── REST outcome: HTTP 200, payload flagged, metrics count a miss ────

class _Response:
    def __init__(self):
        self.headers: dict = {}


def test_rest_route_marks_unresolved_text_as_an_empty_outcome(monkeypatch):
    """api_get_law keeps HTTP 200 for a flagged excerpt (the payload says
    text_status) but must not count it as an answered lookup."""
    _stub(monkeypatch)
    # The route body lives inside main_remote() (see
    # test_rest_outcome_routes for how the app is built offline); check the
    # decision is wired, then exercise it through the helpers it calls.
    route = (REPO / "mcp_server.py").read_text(encoding="utf-8")
    route = route[route.index("async def api_get_law("):]
    route = route[:route.index("_amendment_ref_validity")]
    assert 'result.get("text_status") in ("heading_only", "empty")' in route
    assert '_mark_outcome(response, "empty", "article_text_unresolved")' in route
    res = m.get_law(sr_number="220", article="48", as_of="2010-01-01")
    r = _Response()
    assert m._payload_outcome(res) == ("substantive", None)       # shape alone says answered …
    m._mark_outcome(r, "empty", "article_text_unresolved")          # … the route overrides it
    assert r.headers["X-OCL-Outcome"] == "empty"
    assert r.headers["X-OCL-Empty-Reason"] == "article_text_unresolved"


def test_openapi_and_prompt_document_text_status():
    src = (REPO / "mcp_server.py").read_text(encoding="utf-8")
    assert '"text_status":             {"type": "string", "nullable": True' in src
    r2 = src[src.index('"R2. NEVER write a direct quotation'):]
    r2 = r2[:r2.index('"R3. NEVER')]
    assert "text_status: heading_only" in r2
    assert "article text was recovered at all" in r2

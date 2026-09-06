"""The report says only what was established: a decision exists (and its passage was
retrieved), a quotation is judged only after a served text was compared, "not in the
corpus" is qualified by the court's coverage, citation-like strings the finder did not
read are listed, and the labels follow the draft's language."""
import json
import shutil
import sqlite3
from argparse import Namespace
from pathlib import Path

from opencaselaw_cli import cli, render, report, workflows
from opencaselaw_cli.local import LocalClient

from test_check_document import make_docx
from test_local_pack import _build_pack
from test_workflows import FakeClient

PLAIN = render.Style(False)
PASSAGE = "Selon l'art. 335 al. 1 CO, le contrat de travail conclu pour une durée indéterminée peut être résilié par chacune des parties."
COVERAGE = [{"court": "zh_obergericht", "canton": "ZH", "decision_count": 28197, "earliest": "1943-02-01", "latest": "2026-08-30", "languages": 1},
            {"court": "zh_gerichte", "canton": "ZH", "decision_count": 100, "earliest": "2000-01-01", "latest": "2020-01-01", "languages": 1},
            {"court": "bge", "canton": "CH", "decision_count": 50000, "earliest": "1875-01-01", "latest": "2026-01-01", "languages": 3}]


class Online(FakeClient):
    """The service with the list_courts tool."""

    def __init__(self, overrides=None, coverage=COVERAGE):
        super().__init__(overrides)
        self.coverage = coverage
        self.tools_called = []

    def tool_json(self, name, arguments=None):
        self.tools_called.append(name)
        return {"courts": self.coverage, "_tool": name}


def online_client(cls=Online, **kw):
    def cite(params):
        ref = params["reference"]
        table = {"BGE 136 III 513": ("bge_BGE_136_III_513", "BGE 136 III 513"), "4A_747/2012": ("bger_4A_747_2012", "BGer 4A_747/2012 vom 5. April 2013"),
                 "LA210005": ("zh_obergericht_LA210005", "Obergericht ZH LA210005 vom 15. Juni 2021"), "4A_714/2014": ("bger_4A_714_2014", "BGer 4A_714/2014 vom 22. Mai 2015")}
        if ref in table:
            return {"exists": True, "decision_id": table[ref][0], "citation_string_de": table[ref][1], "citation_string_fr": table[ref][1].replace("BGE", "ATF").replace("BGer", "TF")}
        return {"exists": False, "close_matches": []}
    records = {"bge_BGE_136_III_513": {"decision_id": "bge_BGE_136_III_513", "citation_string_de": "BGE 136 III 513", "citation_string_fr": "ATF 136 III 513", "court": "bge", "decision_date": "2010-10-07"},
               "bger_4A_747_2012": {"decision_id": "bger_4A_747_2012", "docket_number": "4A_747/2012", "court": "bger", "citation_string_de": "BGer 4A_747/2012 vom 5. April 2013", "decision_date": "2013-04-05"},
               "zh_obergericht_LA210005": {"decision_id": "zh_obergericht_LA210005", "docket_number": "LA210005", "court": "zh_obergericht", "canton": "ZH", "citation_string_de": "Obergericht ZH LA210005 vom 15. Juni 2021", "decision_date": "2021-06-15"},
               "bger_4A_714_2014": {"decision_id": "bger_4A_714_2014", "docket_number": "4A_714/2014", "court": "bger", "citation_string_de": "BGer 4A_714/2014 vom 22. Mai 2015", "decision_date": "2015-05-22"}}
    overrides = {"/api/cite": cite, "/api/lookup": {"is_case_number": True, "exact": True, "results": []},
                 "/api/erwaegung/bge_BGE_136_III_513/2.3": {"decision_id": "bge_BGE_136_III_513", "e_number": "2.3", "text": PASSAGE},
                 "/api/erwaegung/bger_4A_747_2012/3": {"decision_id": "bger_4A_747_2012", "e_number": "3", "text": "served"}}
    for did, rec in records.items():
        overrides["/api/decisions/" + did] = lambda params, rec=rec: {**rec, "full_text": PASSAGE} if params and params.get("full_text") else dict(rec)
    return cls(overrides, **kw)


def row_for(result, needle):
    """The result row whose written reference contains the needle (the finder keeps the
    court words as written, "Urteil des Obergerichts ZH ...")."""
    return next(r for r in result["results"] if needle in r["reference"])


def check(tmp_path, paragraphs, client, language="en", report_name="report.html"):
    """check_document straight from the library, with the report language chosen freely
    (the CLI's --language offers de, fr and it)."""
    draft = make_docx(tmp_path / "draft.docx", paragraphs)
    args = Namespace(draft=str(draft), language=language, report=str(tmp_path / report_name), no_report=False, jobs=1)
    result, code = workflows.check_document(args, client)
    return result, code, Path(result["report_path"]).read_text(encoding="utf-8")


def run_cli(monkeypatch, capsys, client, argv):
    if client is not None:
        monkeypatch.setattr(cli, "create_client", lambda args: client)
    monkeypatch.setenv("OCL_CONFIG", "/nonexistent/ocl-config"); monkeypatch.setenv("OCL_JOBS", "1")
    code = cli.main(argv)
    return code, capsys.readouterr().out


# ── 1. a quotation is judged only after a served text was compared ──────────

OFFLINE_DRAFT = [
    "Das Obergericht hielt fest: «Die Kündigung ist missbräuchlich, wenn sie wegen einer Eigenschaft ausgesprochen wird» (Obergericht ZH LA210005 vom 15. Juni 2021).",
    "Le Tribunal fédéral: «le contrat de travail conclu pour une durée indéterminée peut être résilié» (ATF 136 III 513 consid. 2.3).",
    "Ferner: «une phrase qui ne figure nulle part dans ce considérant ni ailleurs» (BGE 136 III 513 E. 2.3).",
]


def test_offline_quotation_without_served_text_is_unverifiable_not_not_found(tmp_path, monkeypatch, capsys):
    pack, _, _ = _build_pack(tmp_path)
    draft = make_docx(tmp_path / "memo.docx", OFFLINE_DRAFT)
    code, out = run_cli(monkeypatch, capsys, None, ["--local", str(pack), "check", str(draft), "--format", "json", "--no-report"])
    result = json.loads(out)
    rows = {r["reference"]: r for r in result["results"]}
    assert code == 4
    unverifiable = rows["Obergericht ZH LA210005 vom 15. Juni 2021"]["quote_check"]
    assert unverifiable == {"quote_status": "unverifiable", "reason": "no served text"}
    assert rows["ATF 136 III 513 consid. 2.3"]["quote_check"]["quote_status"] == "exact"
    compared = rows["BGE 136 III 513 E. 2.3"]["quote_check"]
    assert compared["quote_status"] == "not_found" and compared["served"] and compared["found_in"] == "E. 2.3"
    # every not_found carries the served text it was compared with
    assert all(r["quote_check"].get("served") for r in result["results"] if r.get("quote_check", {}).get("quote_status") == "not_found")
    assert result["report_language"] == "de"   # the CLI's default; the terminal renderer follows it
    result["report_language"] = "en"
    for text in (report.render_html(result, "memo.docx", result["found"], "en"), report.render_markdown(result, "memo.docx", result["found"], "en"),
                 render.render_check(result, PLAIN, 100)):
        assert "quotation not checked" in text and "The decision text is not available in this mode; check against the decision." in text
        assert text.count("quotation not found") == 1   # the real comparison only
    assert report.status_key(rows["Obergericht ZH LA210005 vom 15. Juni 2021"]) == "quote_unverifiable"


def test_quotes_check_offline_without_pinpoint_is_unverifiable(tmp_path, monkeypatch, capsys):
    pack, _, _ = _build_pack(tmp_path)
    code, out = run_cli(monkeypatch, capsys, None, ["--local", str(pack), "quotes", "check", "Obergericht ZH LA210005 vom 15. Juni 2021",
                                                    "--quote", "Die Kündigung ist missbräuchlich, wenn sie wegen einer Eigenschaft ausgesprochen wird", "--format", "json"])
    result = json.loads(out)
    row = result["results"][0]
    assert code == 4 and row["quote_status"] == "unverifiable" and row["status"] == "quote_unverifiable" and row["reason"] == "no served text"
    assert "ratio" not in row and result["counts"] == {"unverifiable": 1}
    text = render.render_quotes(result, PLAIN, 100)
    assert "unverifiable" in text and "no served text" in text


def test_empty_served_text_is_unverifiable_and_an_empty_quotation_stays_not_found():
    assert workflows.match_quote("le contrat de travail", "   \n ") == {"quote_status": "unverifiable", "reason": "no served text"}
    assert workflows.match_quote("", PASSAGE)["quote_status"] == "not_found"


# ── 2. wording: exists / passage retrieved, scope above the results ─────────

MEMO = [
    "Nach ständiger Rechtsprechung ist die Kündigung missbräuchlich (BGE 136 III 513 E. 2.3; vgl. auch BGer 4A_747/2012 vom 5. April 2013, E. 3).",
    "Das Obergericht hat dies bestätigt (Obergericht ZH LA210005 vom 15. Juni 2021).",
    "Unzutreffend zitiert wird BGE 999 III 1 sowie BGer 4A_714/2014 vom 22. Mai 2016.",
]


def test_report_says_exists_and_passage_retrieved_never_verified(tmp_path):
    result, code, html = check(tmp_path, MEMO, online_client(), "en")
    md = report.render_markdown(result, "draft.docx", result["found"], "en")
    text = render.render_check(result, PLAIN, 100)
    assert code == 4 and result["summary"]["exists"] == 3 and result["summary"]["passages_retrieved"] == 2 and result["summary"]["attention"] == 2
    for page in (html, md, text):
        assert "verified" not in page.lower()
    assert "exists, passage retrieved (E. 2.3)" in html and "exists, passage retrieved (E. 3)" in html and "<td class=\"ok\">exists</td>" in html
    assert "| exists, passage retrieved (E. 2.3) |" in md and "| exists |" in md
    # the service's string is shown as served; the client appends no pinpoint to it
    assert "| BGE 136 III 513 E. 2.3 | BGE 136 III 513 | exists, passage retrieved (E. 2.3) |" in md
    assert "3 exist (2 with the cited passage retrieved), 2 need attention" in text
    assert "3 exist (2 with the cited passage retrieved)" in html and "2 need attention" in html


def test_scope_statement_stands_above_the_results_in_every_rendering(tmp_path):
    result, _, html = check(tmp_path, MEMO, online_client(), "en")
    md = report.render_markdown(result, "draft.docx", result["found"], "en")
    text = render.render_check(result, PLAIN, 100)
    scope = "Not whether a decision supports the argument or is still good law."
    assert html.index(scope) < html.index("Needs attention") < html.index("BGE 999 III 1")
    assert md.index(scope) < md.index("## Needs attention")
    lines = text.splitlines()
    assert "Existence, identity and wording only; not whether a decision supports the argument or is still good law." == lines[1]
    assert lines.index(next(l for l in lines if "BGE 999 III 1" in l)) > 1


def test_exit_codes_are_kept(tmp_path):
    _, code, _ = check(tmp_path, MEMO[:2] + ["Vgl. ZR 110 Nr. 23."], online_client(), "en")
    assert code == 0
    _, code, _ = check(tmp_path, MEMO, online_client(), "en")
    assert code == 4


# ── 3. "not in the corpus" is qualified by the court's coverage ─────────────

MISSING = ["Unzutreffend zitiert wird BGE 999 III 1.", "Ebenso das Urteil des Obergerichts ZH LB990001 vom 1. Januar 1999.",
           "Und ein Urteil des Handelsgerichts ZH HG200001 vom 2. Februar 2020."]


def test_missing_reference_carries_the_corpus_coverage_of_its_court_online(tmp_path):
    client = online_client()
    result, code, html = check(tmp_path, MISSING, client, "en")
    assert code == 4 and client.tools_called == ["list_courts"]
    zh = row_for(result, "LB990001")
    assert zh["status"] == "missing" and zh["reference"] == "Urteil des Obergerichts ZH LB990001 vom 1. Januar 1999"
    assert zh["coverage"]["inferred"] == {"label": "LB990001", "court_word": "Obergericht", "canton": "ZH", "courts": [], "stem": "obergericht"}
    assert zh["coverage"]["courts"] == ["zh_obergericht"] and zh["coverage"]["decisions"] == 28197
    assert zh["coverage"]["first_year"] == "1943" and zh["coverage"]["last_year"] == "2026" and zh["coverage"]["source"] == "list_courts"
    assert zh["coverage"]["name"] == "Obergericht ZH"
    bge = row_for(result, "BGE 999 III 1")["coverage"]
    assert bge["inferred"]["courts"] == ["bge"] and bge["decisions"] == 50000 and bge["name"] == "BGE"
    # no zh_handelsgericht in the coverage: the canton's collections, said so
    hg = row_for(result, "HG200001")["coverage"]
    assert hg["canton_wide"] is True and hg["name"] == "canton ZH" and hg["decisions"] == 28297
    assert "Obergericht ZH: 28,197 decisions 1943–2026 in the corpus; unpublished decisions and the decision under appeal are expected to be absent" in html
    assert "BGE: 50,000 decisions 1875–2026 in the corpus" in html
    assert "read as Handelsgericht ZH, label HG200001; canton ZH: 28,297 decisions" in html
    assert "Check the citation. If the decision is unpublished or is the decision under appeal, it cannot be in any corpus." in html
    assert "not in the corpus" in html and "not found" not in html.lower()
    text = render.render_check(result, PLAIN, 120)
    assert "not in the corpus" in text and "28,197 decisions 1943–2026 in the corpus" in text


def test_missing_reference_coverage_offline_from_the_pack_and_tolerant_without_it(tmp_path):
    pack, meta, _ = _build_pack(tmp_path)
    assert meta["schema_version"] == "2"
    con = sqlite3.connect(f"file:{pack}?mode=ro&immutable=1", uri=True)
    courts = {r[0]: r[1:] for r in con.execute("SELECT court, canton, decisions, first_year, last_year FROM courts ORDER BY court")}
    con.close()
    assert courts == {"bge": ("CH", 1, "2010", "2010"), "bger": ("CH", 2, "2006", "2013"), "zh_obergericht": ("ZH", 1, "2021", "2021")}
    client = LocalClient(pack)
    result, code, html = check(tmp_path, MISSING[:2], client, "en")
    zh = row_for(result, "LB990001")["coverage"]
    assert code == 4 and zh["source"] == "pack" and zh["decisions"] == 1 and zh["first_year"] == "2021" and zh["last_year"] == "2021"
    assert "Obergericht ZH: 1 decisions 2021–2021 in the corpus" in html
    assert row_for(result, "BGE 999 III 1")["coverage"]["decisions"] == 1
    # a schema-1 pack (no courts table): the inference stands, the figure is absent, nothing fails
    old = tmp_path / "old.sqlite"
    shutil.copy(pack, old)
    con = sqlite3.connect(old); con.execute("DROP TABLE courts"); con.commit(); con.close()
    result, code, html = check(tmp_path, MISSING[:2], LocalClient(old), "en", "old.html")
    zh = row_for(result, "LB990001")["coverage"]
    assert code == 4 and zh["source"] is None and zh["decisions"] is None and zh["inferred"]["canton"] == "ZH"
    assert "Obergericht ZH: no coverage figure available in this mode" in html
    assert "If the decision is unpublished or is the decision under appeal, it cannot be in any corpus." in html


def test_missing_reference_without_any_coverage_source(tmp_path):
    result, code, html = check(tmp_path, MISSING[:1], online_client(FakeClient), "en")   # a client without tool_json
    coverage = result["results"][0]["coverage"]
    assert code == 4 and coverage["source"] is None and coverage["decisions"] is None and coverage["inferred"]["courts"] == ["bge"]
    assert "BGE: no coverage figure available in this mode" in html


def test_infer_court_reads_the_reference_not_the_corpus():
    assert workflows.infer_court("BGer 4A_714/2014 vom 22. Mai 2016")["courts"] == ["bger"]
    assert workflows.infer_court("4A_714/2014")["court_word"] == "BGer"
    assert workflows.infer_court("A-4843/2020")["courts"] == ["bvger"]
    vd = workflows.infer_court("arrêt du Tribunal cantonal vaudois HC/2020/38")
    assert vd["canton"] == "VD" and vd["court_word"] == "Tribunal cantonal" and vd["stem"] == "kantonsgericht" and vd["label"] == "HC/2020/38"
    bl = workflows.infer_court("Kantonsgericht BL 810 16 9")
    assert bl["canton"] == "BL" and bl["label"] == "810 16 9"
    rows = workflows._coverage_rows({"courts": [{"court": "bl_gerichte", "canton": "bl", "decisions": "7", "first_year": 2001, "last_year": 2024}]})
    assert rows == [{"court": "bl_gerichte", "canton": "BL", "decisions": 7, "first_year": "2001", "last_year": "2024"}]
    coverage = workflows.coverage_for("Kantonsgericht BL 810 16 9", rows, "pack")
    assert coverage["courts"] == ["bl_gerichte"] and coverage["canton_wide"] is True and coverage["decisions"] == 7


# ── 4. silent recall: citation-like strings that were not checked ──────────

RECALL = [
    "Nach Rechtsprechung (BGE 136 III 513 E. 2.3; vgl. ZR 110 Nr. 23 und Pra 2015 Nr. 45).",
    "Das Urteil vom 15. Juni 2021, LA210005, sowie SK 2019/12 und 810 16 9. Vgl. auch JdT 2019 II 45 und SJZ 112/2016 S. 123.",
    "Bestätigt in Obergericht ZH LA210005 vom 15. Juni 2021. Es kostet Fr. 100 000.",
]


def test_unparsed_citation_like_strings_are_listed_not_counted(tmp_path):
    result, code, html = check(tmp_path, RECALL, online_client(), "en")
    assert [u["text"] for u in result["unparsed"]] == ["ZR 110 Nr. 23", "Pra 2015 Nr. 45", "SK 2019/12", "810 16 9", "JdT 2019 II 45", "SJZ 112/2016 S. 123"]
    assert result["unparsed"][2]["paragraph"] == 2 and "SK 2019/12" in result["unparsed"][2]["context"]
    assert result["summary"] == {**result["summary"], "checked": 2, "exists": 2, "attention": 0, "unparsed": 6}
    assert code == 0   # not attention items
    assert "Possibly citations, not checked" in html and "SK 2019/12" in html and "810 16 9" in html
    assert "6 possibly citations, not checked" in html
    md = report.render_markdown(result, "draft.docx", result["found"], "en")
    assert "## Possibly citations, not checked" in md and "- JdT 2019 II 45 (§2):" in md
    text = render.render_check(result, PLAIN, 100)
    assert "possibly citations, not checked (6):" in text and "  ZR 110 Nr. 23  §1" in text
    assert "2 exist (1 with the cited passage retrieved), 0 need attention, 6 possibly citations, not checked" in text


def test_unparsed_are_listed_even_when_nothing_was_found(tmp_path):
    result, code, html = check(tmp_path, ["Vgl. GVP 2018 Nr. 12 und RBOG 2008 Nr. 10."], online_client(), "en")
    assert code == 0 and result["results"] == [] and [u["text"] for u in result["unparsed"]] == ["GVP 2018 Nr. 12", "RBOG 2008 Nr. 10"]
    assert "0 citations found" in html and "2 possibly citations, not checked" in html
    text = render.render_check(result, PLAIN, 100)
    assert "no citations found in the document" in text and "possibly citations, not checked (2)" in text


# ── 5. the report language ──────────────────────────────────────────────────

def test_language_drives_labels_and_advice(tmp_path, monkeypatch, capsys):
    draft = make_docx(tmp_path / "memo.docx", MEMO + ["Vgl. ZR 110 Nr. 23."])
    expected = {
        "de": ("# Zitatprüfung: memo.docx", "## Zu prüfen", "| nicht im Korpus |", "Zitat prüfen. Ist der Entscheid unpubliziert oder der angefochtene Entscheid, kann er in keinem Korpus sein.",
               "vorhanden, Erwägung abgerufen (E. 2.3)", "## Möglicherweise Zitate, nicht geprüft", "BGE: 50'000 Entscheide 1875–2026 im Korpus"),
        "fr": ("# Vérification des citations: memo.docx", "## À examiner", "| pas dans le corpus |", "Vérifier la citation. Si la décision est non publiée ou est la décision attaquée, elle ne peut figurer dans aucun corpus.",
               "existe, considérant récupéré (E. 2.3)", "## Citations possibles, non vérifiées", "BGE: 50 000 décisions 1875–2026 dans le corpus"),
        "it": ("# Verifica delle citazioni: memo.docx", "## Da esaminare", "| non nel corpus |", "Verificare la citazione. Se la decisione non è pubblicata o è la decisione impugnata, non può trovarsi in alcun corpus.",
               "esiste, considerando recuperato (E. 2.3)", "## Possibili citazioni, non verificate", "BGE: 50'000 decisioni 1875–2026 nel corpus"),
    }
    for lang, phrases in expected.items():
        code, out = run_cli(monkeypatch, capsys, online_client(), ["check", str(draft), "--report", str(tmp_path / f"memo-{lang}.md"), "--format", "json", "--language", lang])
        result = json.loads(out)
        md = (tmp_path / f"memo-{lang}.md").read_text(encoding="utf-8")
        assert code == 4 and result["report_language"] == lang and result["summary"]["language"] == lang
        for phrase in phrases:
            assert phrase in md, (lang, phrase)
        assert "verified" not in md.lower() and "Needs attention" not in md
        assert md.index("## " + phrases[1][3:]) > md.index(phrases[0])
        text = " ".join(render.render_check(result, PLAIN, 100).split())
        assert phrases[2].strip("| ") in text and phrases[3] in text
    # the French report shows the service's French string
    assert "| BGE 136 III 513 E. 2.3 | ATF 136 III 513 |" in (tmp_path / "memo-fr.md").read_text(encoding="utf-8")
    # English for en and for anything unknown
    assert report.language_of("en") == "en" and report.language_of("xx") == "en" and report.language_of(None) == "en"
    assert report.t("xx", "h_attention") == "Needs attention" and report.label({"status": "missing"}, "rm")[0] == "not in the corpus"
    assert report.fmt_int("fr", 1234567) == "1 234 567" and report.fmt_int("de", 1234) == "1'234" and report.fmt_int("en", 1234) == "1,234"


def test_terminal_and_html_follow_the_language(tmp_path, monkeypatch, capsys):
    draft = make_docx(tmp_path / "memo.docx", MEMO)
    code, out = run_cli(monkeypatch, capsys, online_client(), ["check", str(draft), "--format", "text", "--color", "never", "--language", "fr"])
    assert code == 4
    assert "Existence, identité et libellé seulement; non pas si une décision soutient l'argument ou fait encore jurisprudence." in out
    assert "pas dans le corpus" in out and "indication erronée" in out and "3 existent (2 avec le considérant cité récupéré), 2 à examiner" in out
    html = (tmp_path / "memo.check.html").read_text(encoding="utf-8")
    assert html.startswith("<!doctype html><html lang=\"fr\">") and "<title>Vérification des citations: memo.docx</title>" in html
    assert "<h2>À examiner</h2>" in html and "<h2>Existent</h2>" in html and "existe, considérant récupéré (E. 2.3)" in html
    assert html.index("Existence, identité et libellé seulement") < html.index("<h2>À examiner</h2>")

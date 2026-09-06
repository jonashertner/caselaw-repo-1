"""`ocl check DRAFT`: a Word file in, a report out."""
import json
import zipfile
from pathlib import Path

from opencaselaw_cli import cli
from opencaselaw_cli.documents import find_citations, read_document

from test_workflows import FakeClient

W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"


def make_docx(path: Path, paragraphs: list[str], footnotes: list[str] = ()) -> Path:
    def body(items, root, extra=""):
        ps = "".join(f'<w:p><w:r><w:t xml:space="preserve">{p}</w:t></w:r></w:p>' for p in items)
        return f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:{root} xmlns:w="{W}">{extra}{ps}</w:{root}>'
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="xml" ContentType="application/xml"/></Types>')
        zf.writestr("word/document.xml", body(paragraphs, "document").replace("<w:document", "<w:document").replace(f'<w:document xmlns:w="{W}">', f'<w:document xmlns:w="{W}"><w:body>').replace("</w:document>", "</w:body></w:document>"))
        if footnotes:
            fns = "".join(f'<w:footnote w:id="{i+1}"><w:p><w:r><w:t xml:space="preserve">{t}</w:t></w:r></w:p></w:footnote>' for i, t in enumerate(footnotes))
            zf.writestr("word/footnotes.xml", f'<?xml version="1.0"?><w:footnotes xmlns:w="{W}">{fns}</w:footnotes>')
    return path


MEMO = [
    "Nach ständiger Rechtsprechung ist die Kündigung missbräuchlich, wenn sie wegen einer Eigenschaft ausgesprochen wird (BGE 136 III 513 E. 2.3; vgl. auch BGer 4A_747/2012 vom 5. April 2013, E. 3).",
    "Das Obergericht hat dies bestätigt (Obergericht ZH LA210005 vom 15. Juni 2021).",
    "Das Bundesgericht hielt fest: «le contrat de travail conclu pour une durée déterminée peut être résilié par chacune des parties» (ATF 136 III 513 consid. 2.3).",
    "Unzutreffend zitiert wird BGE 999 III 1 sowie BGer 4A_714/2014 vom 22. Mai 2016.",
]


def test_docx_is_read_with_footnotes_and_citations_are_found(tmp_path):
    path = make_docx(tmp_path / "memo.docx", MEMO, footnotes=["Siehe auch BGE 140 III 86 E. 2.3."])
    paragraphs = read_document(path)
    assert len(paragraphs) == 5 and paragraphs[-1].startswith("Siehe auch")
    found = find_citations(paragraphs)
    refs = [f["reference"] for f in found]
    assert refs == ["BGE 136 III 513 E. 2.3", "BGer 4A_747/2012 vom 5. April 2013, E. 3", "Obergericht ZH LA210005 vom 15. Juni 2021",
                    "ATF 136 III 513 consid. 2.3", "BGE 999 III 1", "BGer 4A_714/2014 vom 22. Mai 2016", "BGE 140 III 86 E. 2.3"]
    quoted = [f for f in found if f.get("quote")]
    assert len(quoted) == 1 and quoted[0]["reference"] == "ATF 136 III 513 consid. 2.3" and quoted[0]["quote"].startswith("le contrat de travail")
    assert found[-1]["paragraph"] == 5
    md = tmp_path / "memo.md"; md.write_text("\n\n".join(MEMO), encoding="utf-8")
    assert [f["reference"] for f in find_citations(read_document(md))][:2] == refs[:2]


def test_check_writes_a_report_and_exits_4_when_something_needs_attention(tmp_path, monkeypatch, capsys):
    path = make_docx(tmp_path / "memo.docx", MEMO)
    passage = "Selon l'art. 335 al. 1 CO, le contrat de travail conclu pour une durée indéterminée peut être résilié par chacune des parties."
    def cite(params):
        ref = params["reference"]
        table = {"BGE 136 III 513": ("bge_BGE_136_III_513", "BGE 136 III 513"), "4A_747/2012": ("bger_4A_747_2012", "BGer 4A_747/2012 vom 5. April 2013"),
                 "LA210005": ("zh_obergericht_LA210005", "Obergericht ZH LA210005 vom 15. Juni 2021"), "4A_714/2014": ("bger_4A_714_2014", "BGer 4A_714/2014 vom 22. Mai 2015")}
        if ref in table:
            return {"exists": True, "decision_id": table[ref][0], "citation_string_de": table[ref][1]}
        return {"exists": False, "close_matches": []}
    records = {"bge_BGE_136_III_513": {"decision_id": "bge_BGE_136_III_513", "citation_string_de": "BGE 136 III 513", "court": "bge", "decision_date": "2010-10-07"},
               "bger_4A_747_2012": {"decision_id": "bger_4A_747_2012", "docket_number": "4A_747/2012", "court": "bger", "citation_string_de": "BGer 4A_747/2012 vom 5. April 2013", "decision_date": "2013-04-05"},
               "zh_obergericht_LA210005": {"decision_id": "zh_obergericht_LA210005", "docket_number": "LA210005", "court": "zh_obergericht", "canton": "ZH", "citation_string_de": "Obergericht ZH LA210005 vom 15. Juni 2021", "decision_date": "2021-06-15"},
               "bger_4A_714_2014": {"decision_id": "bger_4A_714_2014", "docket_number": "4A_714/2014", "court": "bger", "citation_string_de": "BGer 4A_714/2014 vom 22. Mai 2015", "decision_date": "2015-05-22"}}
    overrides = {"/api/cite": cite, "/api/lookup": {"is_case_number": True, "exact": True, "results": []},
                 "/api/erwaegung/bge_BGE_136_III_513/2.3": {"decision_id": "bge_BGE_136_III_513", "e_number": "2.3", "text": passage},
                 "/api/erwaegung/bger_4A_747_2012/3": {"decision_id": "bger_4A_747_2012", "e_number": "3", "text": "served"}}
    for did, rec in records.items():
        overrides["/api/decisions/" + did] = lambda params, rec=rec: {**rec, "full_text": passage} if params and params.get("full_text") else dict(rec)
    client = FakeClient(overrides)
    monkeypatch.setattr(cli, "create_client", lambda args: client)
    monkeypatch.setenv("OCL_CONFIG", "/nonexistent/ocl-config"); monkeypatch.setenv("OCL_JOBS", "1")
    code = cli.main(["check", str(path), "--format", "json"])
    out = json.loads(capsys.readouterr().out)
    assert code == 4 and out["summary"]["checked"] == 6 and out["summary"]["attention"] == 3
    statuses = {r["reference"]: r["status"] for r in out["results"]}
    assert statuses["BGE 999 III 1"] == "missing" and statuses["BGer 4A_714/2014 vom 22. Mai 2016"] == "discrepancy"
    quoted = next(r for r in out["results"] if r["reference"] == "ATF 136 III 513 consid. 2.3")
    assert quoted["quote_check"]["quote_status"] == "near"
    report = Path(out["report_path"])
    assert report.name == "memo.check.html" and "Needs attention" in report.read_text(encoding="utf-8") and "BGE 999 III 1" in report.read_text(encoding="utf-8")
    code = cli.main(["check", str(path), "--report", str(tmp_path / "memo-check.md"), "--format", "text", "--color", "never"])
    text = capsys.readouterr().out
    assert code == 4 and "not found" in text and "detail wrong" in text and "quotation differs" in text and (tmp_path / "memo-check.md").read_text(encoding="utf-8").startswith("# Citation check: memo.docx")

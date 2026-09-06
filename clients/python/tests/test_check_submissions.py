"""`ocl check` on party submissions: PDF input, batches with an index, submission wording."""
import json
from pathlib import Path

import pytest

from opencaselaw_cli import cli, documents

from test_check_document import MEMO, make_docx
from test_workflows import FakeClient


def write_pdf(path: Path, lines: list[str]) -> Path:
    """A one-page PDF written by hand (Helvetica, WinAnsi), one text line per entry."""
    def escape(text: str) -> bytes:
        return text.encode("cp1252").replace(b"\\", b"\\\\").replace(b"(", b"\\(").replace(b")", b"\\)")
    content = b"BT /F1 11 Tf 14 TL 56 780 Td " + b" T* ".join(b"(" + escape(p) + b") Tj" for p in lines) + b" ET"
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
               b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
               b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
               b"<< /Length " + str(len(content)).encode() + b" >>\nstream\n" + content + b"\nendstream"]
    out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"); offsets = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out)); out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode() + b"0000000000 65535 f \n" + b"".join(f"{o:010d} 00000 n \n".encode() for o in offsets)
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    path.write_bytes(bytes(out)); return path


pypdf = pytest.importorskip("pypdf")


def test_pdf_pages_and_paragraphs(tmp_path):
    pdf = write_pdf(tmp_path / "eingabe.pdf", ["Die Beschwerde stuetzt sich auf BGE 136 III 513 E. 2.3.", "", "Zweiter Absatz mit Kuendi-", "gung im Umbruch."])
    pairs = documents.read_paragraphs(pdf)
    assert pairs and all(page == 1 for _, page in pairs)
    text = " ".join(p for p, _ in pairs)
    assert "BGE 136 III 513 E. 2.3" in text
    assert documents.read_document(pdf) == [p for p, _ in pairs]


def test_scan_without_text_layer_is_refused(tmp_path):
    scan = write_pdf(tmp_path / "scan.pdf", ["x"])
    with pytest.raises(ValueError, match="no text layer"):
        documents.read_paragraphs(scan)


def test_pdf_paragraphs_join_hyphenated_breaks():
    assert documents._pdf_paragraphs("Die Kündi-\ngung ist\nmissbräuchlich.\n\nZweiter Absatz.") == ["Die Kündigung ist missbräuchlich.", "Zweiter Absatz."]


def test_no_pdf_reader_gives_a_hint(tmp_path, monkeypatch):
    monkeypatch.setattr(documents, "_pdf_pages_pypdf", lambda path: None)
    monkeypatch.setattr(documents.shutil, "which", lambda name: None)
    with pytest.raises(ValueError, match="pypdf"):
        documents.pdf_pages(write_pdf(tmp_path / "x.pdf", ["BGE 136 III 513 E. 2.3 " * 3]))


def _client():
    def cite(params):
        ref = params["reference"]
        if ref == "BGE 136 III 513":
            return {"exists": True, "decision_id": "bge_BGE_136_III_513", "citation_string_de": "BGE 136 III 513"}
        return {"exists": False, "close_matches": []}
    overrides = {"/api/cite": cite, "/api/lookup": {"is_case_number": True, "exact": True, "results": []},
                 "/api/decisions/bge_BGE_136_III_513": {"decision_id": "bge_BGE_136_III_513", "citation_string_de": "BGE 136 III 513", "court": "bge", "decision_date": "2010-10-07"},
                 "/api/erwaegung/bge_BGE_136_III_513/2.3": {"decision_id": "bge_BGE_136_III_513", "e_number": "2.3", "text": "served text"}}
    return FakeClient(overrides)


def test_batch_over_a_directory_writes_an_index_and_reports_unreadable_files(tmp_path, monkeypatch, capsys):
    folder = tmp_path / "eingaben"; folder.mkdir()
    make_docx(folder / "a.docx", MEMO)
    write_pdf(folder / "b.pdf", ["Beschwerde. Vgl. BGE 136 III 513 E. 2.3 und BGE 999 III 1. " * 2])
    write_pdf(folder / "scan.pdf", ["x"])
    (folder / "notes.check.html").write_text("old", encoding="utf-8")   # a report from an earlier run is not an input
    (folder / "~$lock.docx").write_bytes(b"")
    monkeypatch.setattr(cli, "create_client", lambda args: _client())
    monkeypatch.setenv("OCL_CONFIG", "/nonexistent"); monkeypatch.setenv("OCL_JOBS", "1")
    code = cli.main(["check", str(folder), "--kind", "submission", "--format", "json"])
    out = json.loads(capsys.readouterr().out)
    assert code == 2 and out["kind"] == "opencaselaw-check-batch" and [e["name"] for e in out["files"]] == ["a.docx", "b.pdf", "scan.pdf"]
    assert out["files"][2]["status"] == "unreadable" and "no text layer" in out["files"][2]["error"]["message"]
    assert out["summary"]["unreadable"] == 1 and out["summary"]["files"] == 3
    b = out["files"][1]
    assert b["report_kind"] == "submission" and b["pages"] == 1 and all((r.get("input") or {}).get("page") == 1 for r in b["results"])
    index = Path(out["index_path"]); assert index.name == "check-index.html" and index.parent == folder
    html = index.read_text(encoding="utf-8")
    assert "a.docx" in html and "b.pdf" in html and "scan.pdf" in html and (folder / "b.check.html").is_file()
    code = cli.main(["check", str(folder / "a.docx"), str(folder / "b.pdf"), "--kind", "submission", "--format", "text", "--color", "never", "--report", str(folder / "index.md")])
    text = capsys.readouterr().out
    assert code == 4 and "2 Dateien" in text and (folder / "index.md").read_text(encoding="utf-8").startswith("# ")


def test_submission_kind_titles_the_report_and_leads_with_the_counts(tmp_path, monkeypatch, capsys):
    pdf = write_pdf(tmp_path / "Berufung.pdf", ["Vgl. BGE 136 III 513 E. 2.3 sowie BGE 999 III 1. " * 2])
    monkeypatch.setattr(cli, "create_client", lambda args: _client())
    monkeypatch.setenv("OCL_CONFIG", "/nonexistent"); monkeypatch.setenv("OCL_JOBS", "1")
    code = cli.main(["check", str(pdf), "--kind", "submission", "--language", "de", "--format", "text", "--color", "never"])
    text = capsys.readouterr().out
    assert code == 4 and "nicht im Korpus" in text and "S. 1" in text
    html = (tmp_path / "Berufung.check.html").read_text(encoding="utf-8")
    assert "Zitatprüfung der Eingabe" in html and "angefochtene" in html and "S. 1" in html
    code = cli.main(["check", str(pdf), "--kind", "draft", "--language", "en", "--format", "json", "--no-report"])
    out = json.loads(capsys.readouterr().out)
    assert out["report_kind"] == "draft" and out["pages"] == 1


def test_corrupt_docx_is_an_input_error(tmp_path):
    bad = tmp_path / "bad.docx"; bad.write_bytes(b"not a zip")
    with pytest.raises(ValueError, match="not a readable Word document"):
        documents.read_document(bad)

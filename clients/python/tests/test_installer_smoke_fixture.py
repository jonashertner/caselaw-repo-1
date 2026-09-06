"""The installer workflow's smoke fixture (installer/smoke/make_fixture.py) writes a
one-page PDF filing by hand, with no PDF library, for the installed tree to read
with the pypdf it bundles. The bytes must be a well-formed PDF (a reader that
rebuilds a broken cross-reference table would hide a bug here), and where pypdf
happens to be importable the text must come back with the citation intact."""
import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import pytest

CLIENT = Path(__file__).resolve().parents[1]
SMOKE = CLIENT / "installer" / "smoke" / "make_fixture.py"


def _load():
    spec = importlib.util.spec_from_file_location("make_fixture", SMOKE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_pdf_is_well_formed(tmp_path):
    fixture = _load()
    pdf = fixture.write_pdf(tmp_path / "eingabe.pdf", fixture.FILING).read_bytes()
    assert pdf.startswith(b"%PDF-1.4\n") and pdf.endswith(b"%%EOF\n")
    assert b"/Type /Catalog" in pdf and b"/Count 1" in pdf and b"/BaseFont /Helvetica" in pdf
    # Every xref entry points at "N 0 obj", and startxref at the xref table.
    xref_offset = int(re.search(rb"startxref\n(\d+)\n%%EOF", pdf).group(1))
    assert pdf[xref_offset:].startswith(b"xref\n0 6\n")
    entries = re.findall(rb"(\d{10}) 00000 n \n", pdf[xref_offset:])
    assert len(entries) == 5
    for number, entry in enumerate(entries, 1):
        assert pdf[int(entry):].startswith(f"{number} 0 obj\n".encode()), number
    # The content stream's /Length matches the bytes between stream and endstream.
    match = re.search(rb"/Length (\d+) >>\nstream\n(.*?)\nendstream", pdf, re.S)
    assert len(match.group(2)) == int(match.group(1))
    # The citation is on the page as a single string operand, WinAnsi-encoded.
    assert b"(BGE 136 III 513 E. 2.3)" not in pdf  # parentheses in the text are escaped ...
    assert b"\\(BGE 136 III 513 E. 2.3\\)" in pdf  # ... and the citation survives as one Tj string
    assert "Kündigung".encode("cp1252") in pdf and "«le contrat".encode("cp1252") in pdf


def test_pdf_escapes_and_rejects(tmp_path):
    fixture = _load()
    pdf = fixture.write_pdf(tmp_path / "x.pdf", ["a (b) c \\ d"]).read_bytes()
    assert b"(a \\(b\\) c \\\\ d) Tj" in pdf
    with pytest.raises(UnicodeEncodeError):
        fixture.write_pdf(tmp_path / "y.pdf", ["\u2026 outside WinAnsi: \u4e2d"])
    with pytest.raises(ValueError):
        fixture.write_pdf(tmp_path / "z.pdf", [])


def test_pypdf_reads_the_citation_back(tmp_path):
    pypdf = pytest.importorskip("pypdf")
    fixture = _load()
    path = fixture.write_pdf(tmp_path / "eingabe.pdf", fixture.FILING)
    reader = pypdf.PdfReader(str(path))
    assert len(reader.pages) == 1
    text = reader.pages[0].extract_text()
    assert "BGE 136 III 513 E. 2.3" in text and "ATF 136 III 513 consid. 2.3" in text and "BGE 999 III 1" in text
    assert "Kündigung" in text and "«le contrat de travail conclu pour une durée indéterminée»" in text


def test_make_fixture_writes_pack_draft_and_filing(tmp_path):
    result = subprocess.run([sys.executable, str(SMOKE), str(tmp_path / "fx")], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    for name in ("pack.sqlite", "draft.docx", "eingabe.pdf"):
        assert (tmp_path / "fx" / name).stat().st_size > 0, name
    assert "filing=" in result.stdout and "decisions=4" in result.stdout

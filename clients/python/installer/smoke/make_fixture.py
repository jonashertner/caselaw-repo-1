"""Build the offline smoke-test fixture the installer workflow runs `ocl check` against:
a small verification pack (from the test-suite fixtures), a Word draft that cites
decisions in it, and a one-page PDF filing that cites BGE 136 III 513 (written by
hand, no PDF library: the installed tree must read it with the pypdf it bundles).
Reuses the helpers of the client test suite so the fixture stays in step with what
the tests cover; pytest must be importable (the tests import it).

    python installer/smoke/make_fixture.py OUT_DIR
        ->  OUT_DIR/pack.sqlite, OUT_DIR/draft.docx, OUT_DIR/eingabe.pdf
"""
from __future__ import annotations

import sys
from pathlib import Path

CLIENT = Path(__file__).resolve().parents[2]  # clients/python

# A party's filing as it arrives from justitia.swiss: one decision the pack holds
# (with an indexed Erwägung and a verbatim quotation), one that no corpus holds.
FILING = [
    "Beschwerde in Zivilsachen",
    "Die Kündigung ist missbräuchlich, wenn sie wegen einer Eigenschaft ausgesprochen wird (BGE 136 III 513 E. 2.3).",
    "Das Bundesgericht hielt fest: «le contrat de travail conclu pour une durée indéterminée» (ATF 136 III 513 consid. 2.3).",
    "Die Gegenpartei beruft sich zu Unrecht auf BGE 999 III 1.",
]


def write_pdf(path: Path, paragraphs: list[str]) -> Path:
    """A one-page PDF written by hand: Helvetica in WinAnsiEncoding, one text line
    per paragraph, a correct cross-reference table. Enough for any PDF reader to
    extract the text; no library involved on the writing side."""

    def escape(text: str) -> bytes:
        return text.encode("cp1252").replace(b"\\", b"\\\\").replace(b"(", b"\\(").replace(b")", b"\\)")

    if not paragraphs:
        raise ValueError("a page needs at least one paragraph")
    content = b"BT /F1 11 Tf 14 TL 56 780 Td " + b" T* ".join(b"(" + escape(p) + b") Tj" for p in paragraphs) + b" ET"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
        b"<< /Length " + str(len(content)).encode() + b" >>\nstream\n" + content + b"\nendstream",
    ]
    out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode() + b"0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    path.write_bytes(bytes(out))
    return path


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    out = Path(argv[1]).resolve()
    out.mkdir(parents=True, exist_ok=True)
    sys.path[:0] = [str(CLIENT / "src"), str(CLIENT / "tests")]
    from test_check_document import MEMO, make_docx  # noqa: E402
    from test_local_pack import _build_pack  # noqa: E402

    pack, meta, _builder = _build_pack(out)
    draft = make_docx(out / "draft.docx", MEMO)
    filing = write_pdf(out / "eingabe.pdf", FILING)
    print(f"pack={pack} decisions={meta.get('decisions')} paragraphs={meta.get('paragraphs')} draft={draft} filing={filing}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

"""Read a draft or a party submission (Word, PDF, Markdown, HTML, plain text) and
find the citations and quotations in it, so a lawyer hands over the document and
gets a report back.

Reading is stdlib only: a .docx is a zip with word/document.xml (and footnotes,
where citations usually live). A .pdf (electronic filings arrive as PDF) is read
page by page through the optional `pypdf` package or the `pdftotext` executable;
each paragraph keeps its page number. Finding is regex over the prose, feeding the
same reference parser the checks use. Nothing here decides anything; every
candidate goes through `citations resolve`.
"""
from __future__ import annotations

import html
import re
import shutil
import subprocess
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

from .references import parse_reference

_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


def _docx_paragraphs(path: Path) -> list[str]:
    """Paragraphs of a .docx; a file that is not a Word document is an input error, not a traceback."""
    try:
        return _docx_paragraphs_raw(path)
    except (zipfile.BadZipFile, KeyError, ET.ParseError) as exc:
        raise ValueError(f"{path.name}: not a readable Word document ({type(exc).__name__}: {exc})") from exc


def _docx_paragraphs_raw(path: Path) -> list[str]:
    paragraphs: list[str] = []
    with zipfile.ZipFile(path) as zf:
        for member in ("word/document.xml", "word/footnotes.xml", "word/endnotes.xml"):
            if member not in zf.namelist():
                continue
            root = ET.fromstring(zf.read(member))
            for p in root.iter(_W + "p"):
                parts = []
                for node in p.iter():
                    if node.tag == _W + "t" and node.text:
                        parts.append(node.text)
                    elif node.tag in (_W + "tab",):
                        parts.append(" ")
                    elif node.tag in (_W + "br", _W + "cr"):
                        parts.append("\n")
                text = "".join(parts).strip()
                if text:
                    paragraphs.append(text)
    return paragraphs


# ── PDF: page texts through pypdf or pdftotext ────────────────────────────
# A text layer that averages fewer characters per page than this is a scan: pypdf returns
# nothing or a few stray glyphs for an image-only page, while even a one-line cover letter
# carries dozens of characters.
PDF_MIN_CHARS_PER_PAGE = 12
PDF_READERS_HINT = "reading a PDF needs the pypdf package (pip install opencaselaw-cli[pdf]) or the pdftotext executable (poppler) on PATH"


def _pdf_pages_pypdf(path: Path) -> list[str] | None:
    """One text per page via pypdf; None when the package is not installed."""
    try:
        import pypdf  # optional extra: opencaselaw-cli[pdf]
    except ImportError:
        return None
    try:
        reader = pypdf.PdfReader(str(path))
        return [(page.extract_text() or "") for page in reader.pages]
    except Exception as exc:  # pypdf's own error classes (PdfReadError, ...) and the odd TypeError on a damaged file
        raise ValueError(f"{path.name}: pypdf could not read the file ({exc})") from exc


def _pdf_pages_pdftotext(path: Path) -> list[str] | None:
    """One text per page via poppler's pdftotext (pages separated by form feeds); None when it is not on PATH."""
    executable = shutil.which("pdftotext")
    if not executable:
        return None
    try:
        completed = subprocess.run([executable, "-enc", "UTF-8", str(path), "-"], capture_output=True, timeout=600)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"{path.name}: pdftotext could not run ({exc})") from exc
    if completed.returncode != 0:
        message = completed.stderr.decode("utf-8", errors="replace").strip().splitlines()
        raise ValueError(f"{path.name}: pdftotext could not read the file" + (f" ({message[-1]})" if message else ""))
    pages = completed.stdout.decode("utf-8", errors="replace").split("\f")
    if pages and pages[-1] == "":
        pages.pop()  # pdftotext ends every page, the last included, with a form feed
    return pages


def pdf_pages(path: str | Path) -> list[str]:
    """The text of each page of a PDF, in order. pypdf when installed, else pdftotext; a PDF whose
    text layer is (nearly) empty is a scan and is refused: OCR has to come first."""
    path = Path(path).expanduser()
    pages = _pdf_pages_pypdf(path)
    if pages is None:
        pages = _pdf_pages_pdftotext(path)
    if pages is None:
        raise ValueError(f"{path.name}: {PDF_READERS_HINT}")
    if not pages:
        raise ValueError(f"{path.name}: the PDF has no pages")
    characters = sum(len(re.sub(r"\s+", "", page)) for page in pages)
    if characters / len(pages) < PDF_MIN_CHARS_PER_PAGE:
        raise ValueError(f"{path.name}: no text layer (scanned PDF); OCR is needed before checking")
    return pages


def _pdf_paragraphs(page_text: str) -> list[str]:
    """Paragraphs of one page: text blocks split on blank lines, hyphenated line breaks joined
    ("Kündi-\ngung" -> "Kündigung"), whitespace normalised."""
    text = page_text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"(?<=[^\W\d_])-\n(?=[a-zßà-öø-ÿ])", "", text)
    paragraphs = []
    for block in re.split(r"\n\s*\n", text):
        block = re.sub(r"\s+", " ", block).strip()
        if block:
            paragraphs.append(block)
    return paragraphs


def read_paragraphs(path: str | Path) -> list[tuple[str, int | None]]:
    """(paragraph, page) pairs of a document. The page is the PDF page the paragraph starts on;
    it is None for every other format, whose paragraphs are those of `read_document`."""
    path = Path(path).expanduser()
    if path.suffix.lower() != ".pdf":
        return [(paragraph, None) for paragraph in read_document(path)]
    pairs: list[tuple[str, int | None]] = []
    for number, page_text in enumerate(pdf_pages(path), start=1):
        pairs += [(paragraph, number) for paragraph in _pdf_paragraphs(page_text)]
    return pairs


def read_document(path: str | Path) -> list[str]:
    """Paragraphs of a draft. .docx (body, footnotes, endnotes), .pdf (page by page), .md/.txt, .html."""
    path = Path(path).expanduser()
    suffix = path.suffix.lower()
    if suffix == ".docx":
        return _docx_paragraphs(path)
    if suffix == ".pdf":
        return [paragraph for paragraph, _ in read_paragraphs(path)]
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    if suffix in (".html", ".htm"):
        text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text, flags=re.S | re.I)
        text = re.sub(r"<br\s*/?>|</p>|</div>|</li>|</h\d>|</tr>", "\n", text, flags=re.I)
        text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    return [p.strip() for p in re.split(r"\n\s*\n|\n(?=\S)", text) if p.strip()]


# ── finding citations in prose ────────────────────────────────────────────
_PIN = r"(?:,?\s*(?:E\.|Erw\.|consid\.|cons\.|c\.)\s*\d+(?:\.\d+)*[a-z]{0,2}(?:/[a-z]{1,2})?)?"
_PAGE = r"(?:,?\s*(?:S\.|p\.|pag\.)\s*\d{1,4}(?:\s*(?:ff?\.|ss?\.))?)?"
_DATE = r"(?:\s+(?:vom|du|del|de)\s+\d{1,2}(?:\.|er)?\s*[A-Za-zÀ-ÿ]+\s+\d{4})?"
_BGE = re.compile(r"(?<![A-Za-z0-9])(?:BGE|ATF|DTF)\s?\d{1,3}\s(?:Ia|Ib|III|II|IV|I|V)\s\d{1,4}" + _PIN + _PAGE)
_COURT_PREFIX = (r"(?:(?:Urteil(?:\s+des)?\s+(?:Bundesgerichts|Bundesverwaltungsgerichts|Bundesstrafgerichts|BGer|BVGer|BStGer)|"
                 r"arrêt\s+du\s+(?:Tribunal\s+fédéral|Tribunal\s+administratif\s+fédéral|TF|TAF)|sentenza\s+del\s+(?:Tribunale\s+federale|TF)|"
                 r"BGer|BVGer|BStGer|TF|TAF|TPF)\s+)?")
_FEDERAL = re.compile(r"(?<![A-Za-z0-9/])" + _COURT_PREFIX + r"(?:\d[A-Z]{1,2}[ _.]\d{1,5}/\d{4}|[A-Z]{1,2}-\d{1,5}/\d{4})(?![0-9/])" + _DATE + _PIN)
_CANTONAL_COURT = r"(?:Ober|Kantons|Verwaltungs|Handels|Bezirks|Appellations|Sozialversicherungs|Steuerrekurs)gericht(?:s)?|Tribunal\s+cantonal|Cour\s+de\s+justice|Tribunale\s+(?:d'appello|cantonale)|OGer|KGer|VGer|Gericht|Tribunal|Kantonsgerichts"
_CANTONAL = re.compile(r"(?<![A-Za-z0-9])(?:(?:Urteil|Entscheid|Beschluss|arrêt|décision|sentenza)\s+(?:des|der|du|de\s+la|del|della)?\s*)?(?:" + _CANTONAL_COURT +
                       r")[^\n.;()«»]{0,40}?((?:[A-Z]{1,6}\.\d{4}\.\d{1,6}|[A-Z]{2}\d{6}(?:-[A-Z](?:_U\d+)?)?|[A-Za-zÀ-ÿ]{1,8} ?/ ?\d{1,6} ?/ ?\d{1,6}|[A-Z]{1,3} \d{4}/\d{1,4}|\d{3} \d{2} \d{1,4}|[A-Z]{2,4}\d? (?:19|20)\d{2} \d{1,4}|[A-Za-z]{2,6} \d{4}(?:/\d{2,4})? Nr\. \d{1,5}))" + _DATE + _PIN)
# Paired marks first («…», „…“, “…”, "…"; an apostrophe inside is part of the
# quotation), then single marks only where they cannot be an apostrophe
# ("l'art. 335" opened a quotation before).
_QUOTE = re.compile(r"[«„“\"]([^«»„“”\"]{25,600})[»“”\"]|(?<![A-Za-zÀ-ÿ0-9])[‘']([^‘’']{25,600})[’'](?![A-Za-zÀ-ÿ0-9])")


def find_citations(paragraphs: list[str]) -> list[dict]:
    """Candidate references with the quotation next to them, in document order, deduplicated by written form."""
    found: list[dict] = []
    seen: dict[str, int] = {}
    for index, paragraph in enumerate(paragraphs):
        spans = []
        for pattern in (_BGE, _FEDERAL, _CANTONAL):
            for m in pattern.finditer(paragraph):
                spans.append((m.start(), m.end(), m.group(0).strip().rstrip(",;")))
        spans.sort()
        kept = []
        last_end = -1
        for start, end, text in spans:
            if start < last_end:
                continue
            kept.append((start, end, text)); last_end = end
        quotes = [(m.start(), m.end(), (m.group(1) or m.group(2) or "").strip()) for m in _QUOTE.finditer(paragraph)]
        for start, end, text in kept:
            parsed = parse_reference(text)
            if not (parsed.bge_label or parsed.dockets):
                continue
            key = text
            quote = None
            for qs, qe, qtext in quotes:
                if abs(qs - end) <= 300 or abs(start - qe) <= 300:
                    quote = qtext
                    break
            if key in seen:
                if quote and not found[seen[key]].get("quote"):
                    found[seen[key]]["quote"] = quote
                continue
            seen[key] = len(found)
            found.append({"reference": text, "paragraph": index + 1, "context": paragraph[max(0, start - 60):end + 60].strip(),
                          **({"quote": quote} if quote else {})})
    return found


# ── silent recall: citation-like strings the finder did not read ──────────
# Docket-like shapes and collection labels (ZR, Pra, GVP, ...) that are not
# part of any checked reference. Listed so a reader knows what was not checked;
# nothing about them is decided.
_UNPARSED_SHAPES = (
    re.compile(r"(?<![A-Za-z0-9])[A-Z]{1,3}[ _.]?(?:\d{1,5}/\d{4}|\d{4}/\d{1,4})(?![0-9/])"),       # C 1234/2020, SK 2019/12
    re.compile(r"(?<![A-Za-z0-9])[A-Z]{2}\d{6}(?:-[A-Z](?:_U\d+)?)?(?![A-Za-z0-9])"),               # LA210005 without a court word
    re.compile(r"(?<![0-9.])\d{3} \d{2} \d{1,4}(?![0-9])"),                                           # BL 810 16 9
    re.compile(r"(?<![A-Za-z])(?:ZR|Pra|GVP|BVR|RBOG|SJZ|AJP|JdT|SJ|RDAF)\s+\d{1,4}(?:/\d{2,4})?"
               r"(?:\s+(?:I{1,3}|IV)(?:\s+\d{1,5})?)?(?:\s*(?:Nr\.|n°|no\.|N)\s*\d{1,5})?(?:,?\s*(?:S\.|p\.|pag\.)\s*\d{1,5})?(?![0-9])"),  # ZR 110 Nr. 23, Pra 2015 Nr. 45, JdT 2019 II 45
)


def _fold(text: str) -> str:
    return re.sub(r"[\s_.]+", "", text or "").casefold()


def unparsed_candidates(paragraphs: list[str], found: list[dict]) -> list[dict]:
    """Citation-like strings that did not become a checked reference, in document order,
    deduplicated by written form: {"text", "paragraph", "context"}. A string counts as
    checked when it lies inside a found reference (any paragraph, since references are
    deduplicated across the document)."""
    checked = [_fold(f.get("reference", "")) for f in found]
    out: list[dict] = []
    seen: set[str] = set()
    for index, paragraph in enumerate(paragraphs):
        spans = []
        for pattern in _UNPARSED_SHAPES:
            for m in pattern.finditer(paragraph):
                spans.append((m.start(), m.end(), m.group(0).strip()))
        spans.sort(key=lambda span: (span[0], -span[1]))   # the longest match at a position wins
        last_end = -1
        for start, end, text in spans:
            if start < last_end:
                continue
            last_end = end
            key = _fold(text)
            if not key or key in seen or any(key in c for c in checked):
                continue
            seen.add(key)
            out.append({"text": text, "paragraph": index + 1, "context": paragraph[max(0, start - 60):end + 60].strip()})
    return out


# ── finding statute references in prose ───────────────────────────────────
def find_statutes(paragraphs: list[str], claimed_quotes=None) -> list[dict]:
    """Statute references ("Art. 8 Abs. 1 ZGB", "art. 335 al. 1 CO", "§ 18 VRG (ZH)", "SR 210")
    with the quotation next to them; the grammar and the rows live in `statutes.py`."""
    from .statutes import find_statute_references
    return find_statute_references(paragraphs, quote_pattern=_QUOTE, claimed_quotes=claimed_quotes)

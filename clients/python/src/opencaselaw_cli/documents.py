"""Read a draft (Word, Markdown, HTML, plain text) and find the citations and
quotations in it, so a lawyer hands over the document and gets a report back.

Reading is stdlib only: a .docx is a zip with word/document.xml (and footnotes,
where citations usually live). Finding is regex over the prose, feeding the
same reference parser the checks use. Nothing here decides anything; every
candidate goes through `citations resolve`.
"""
from __future__ import annotations

import html
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

from .references import parse_reference

_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


def _docx_paragraphs(path: Path) -> list[str]:
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


def read_document(path: str | Path) -> list[str]:
    """Paragraphs of a draft. .docx (body, footnotes, endnotes), .md/.txt, .html."""
    path = Path(path).expanduser()
    suffix = path.suffix.lower()
    if suffix == ".docx":
        return _docx_paragraphs(path)
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    if suffix in (".html", ".htm"):
        text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text, flags=re.S | re.I)
        text = re.sub(r"<br\s*/?>|</p>|</div>|</li>|</h\d>|</tr>", "\n", text, flags=re.I)
        text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    if suffix == ".pdf":
        raise ValueError("PDF is not read directly; save the draft as .docx, .md or .txt (or run `pdftotext` first)")
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
_QUOTE = re.compile(r"[«„“\"‘']([^»“”\"’']{25,600})[»“”\"’']")


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
        quotes = [(m.start(), m.end(), m.group(1).strip()) for m in _QUOTE.finditer(paragraph)]
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

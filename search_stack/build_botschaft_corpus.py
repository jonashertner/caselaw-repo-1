"""Verbatim Botschaft corpus — Phase 2 of the Materialien commitment.

Per ``MEMORY.md → materialien_build_commitment``:

    verbatim text + FTS5 + ELI URI links (NOT Sonnet digests);
    ~3 wks, $0 LLM. Fedlex SPARQL serves Acts/Consultations as
    first-class data; only AmtlBull needs parlament.ch separately.

This module extends ``output/materialien.db`` with a verbatim, paragraph-
level corpus of Federal Council Botschaften. The downstream MCP tool
``get_article_purpose(sr_number, article)`` joins:

    article_botschaft_links.sr_number/article
       → botschaft_documents
       → botschaft_paragraphs (where article_anchor matches)

…and returns verbatim paragraphs the LLM can quote with a
verifiable citation.

Pipeline
--------
    amendment_refs (existing)        → unique (year, page) tuples
                                     ↓
    fetch_bbl_pdf(year, page)        → Fedlex PDF URL → raw bytes
                                     ↓
    parse_botschaft(pdf_bytes)       → paragraph dicts with article anchors
                                     ↓
    upsert_paragraphs                → botschaft_paragraphs + FTS5
                                     ↓
    upsert_links                     → article_botschaft_links

Usage::

    # Schema migration (idempotent, safe to re-run)
    python3 -m search_stack.build_botschaft_corpus --schema-only

    # Ingest a single Botschaft (PoC validation)
    python3 -m search_stack.build_botschaft_corpus --ingest-one \\
        --year 1999 --page 6013 --sr 935.61

    # Ingest all unique BBl refs from amendment_refs (run nightly)
    python3 -m search_stack.build_botschaft_corpus --ingest-all
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from xml.etree import ElementTree as ET

# Reuse the existing Fedlex SPARQL manifestation resolver. v0.1 used a
# naive URL builder that returned the SPA HTML wrapper; this resolver
# returns actual filestore URLs. (Imported lazily so unit tests can
# monkeypatch without spinning up the SPARQL endpoint.)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
try:
    from scrapers.fedlex_materialien import fetch_manifestations  # noqa: E402
except ImportError:
    fetch_manifestations = None  # type: ignore[assignment]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_botschaft_corpus")


# ── Schema ────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS botschaft_documents (
    botschaft_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    bbl_year        INTEGER NOT NULL,
    bbl_page        INTEGER NOT NULL,
    bbl_citation    TEXT NOT NULL,            -- "BBl 1999 6013"
    eli_uri         TEXT,                     -- https://fedlex.data.admin.ch/eli/fga/1999/6013
    title           TEXT,
    publication_date TEXT,
    source_url      TEXT NOT NULL,
    format          TEXT NOT NULL CHECK(format IN ('akoma-ntoso-xml','pdf')),
    language        TEXT NOT NULL,
    page_count      INTEGER,
    text_hash       TEXT,                     -- sha-256 of full text, for delta detection
    ingested_at     TEXT NOT NULL,
    UNIQUE(bbl_year, bbl_page, language)
);

CREATE INDEX IF NOT EXISTS ix_bd_eli ON botschaft_documents(eli_uri);

CREATE TABLE IF NOT EXISTS botschaft_paragraphs (
    paragraph_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    botschaft_id    INTEGER NOT NULL REFERENCES botschaft_documents(botschaft_id) ON DELETE CASCADE,
    para_order      INTEGER NOT NULL,
    page_number     INTEGER,
    section_path    TEXT,                     -- "Besonderer Teil > Zu Artikel 1"
    article_anchor  TEXT,                     -- "1" if paragraph belongs to a specific article
    text            TEXT NOT NULL,
    text_length     INTEGER NOT NULL,
    UNIQUE(botschaft_id, para_order)
);

CREATE INDEX IF NOT EXISTS ix_bp_anchor ON botschaft_paragraphs(article_anchor);
CREATE INDEX IF NOT EXISTS ix_bp_doc ON botschaft_paragraphs(botschaft_id);

CREATE VIRTUAL TABLE IF NOT EXISTS botschaft_paragraphs_fts USING fts5(
    text,
    section_path,
    article_anchor UNINDEXED,
    content='botschaft_paragraphs',
    content_rowid='paragraph_id',
    tokenize='unicode61 remove_diacritics 1'
);

-- Keep FTS5 in lockstep with the parent table.
CREATE TRIGGER IF NOT EXISTS botschaft_paragraphs_ai AFTER INSERT ON botschaft_paragraphs BEGIN
    INSERT INTO botschaft_paragraphs_fts(rowid, text, section_path, article_anchor)
    VALUES (new.paragraph_id, new.text, new.section_path, new.article_anchor);
END;
CREATE TRIGGER IF NOT EXISTS botschaft_paragraphs_ad AFTER DELETE ON botschaft_paragraphs BEGIN
    INSERT INTO botschaft_paragraphs_fts(botschaft_paragraphs_fts, rowid, text, section_path, article_anchor)
    VALUES ('delete', old.paragraph_id, old.text, old.section_path, old.article_anchor);
END;

CREATE TABLE IF NOT EXISTS article_botschaft_links (
    sr_number       TEXT NOT NULL,
    article         TEXT NOT NULL,
    botschaft_id    INTEGER NOT NULL REFERENCES botschaft_documents(botschaft_id) ON DELETE CASCADE,
    relation        TEXT NOT NULL CHECK(relation IN ('enacted','amended','considered')),
    evidence        TEXT,                     -- e.g. "amendment_refs row 12345"
    PRIMARY KEY (sr_number, article, botschaft_id, relation)
);

CREATE INDEX IF NOT EXISTS ix_abl_lookup ON article_botschaft_links(sr_number, article);
CREATE INDEX IF NOT EXISTS ix_abl_botschaft ON article_botschaft_links(botschaft_id);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


# ── Fedlex BBl URL helpers ────────────────────────────────────────────

# v0.2 (2026-05-07): the v0.1 ``bbl_pdf_url`` returned the Fedlex SPA's
# HTML wrapper, not the actual PDF. Real downloads live on the SPARQL-
# resolved filestore URL (``https://fedlex.data.admin.ch/filestore/...``).
# Use ``resolve_manifestation()`` below.

# Format priority. Akoma Ntoso XML when available (clean, structured,
# avoids pdfplumber column-detection issues); plain XML next; PDF/A as
# the durable fallback. ``-an`` suffix marks Akoma Ntoso variants.
_FORMAT_PRIORITY = ("xml-an", "xml", "pdf-a-an", "pdf-a", "pdf")


def bbl_eli_uri(year: int, page: int) -> str:
    return f"https://fedlex.data.admin.ch/eli/fga/{year}/{page}"


def bbl_citation(year: int, page: int) -> str:
    return f"BBl {year} {page}"


_LANG_URI_DEU = "http://publications.europa.eu/resource/authority/language/DEU"
_LANG_URI_FRA = "http://publications.europa.eu/resource/authority/language/FRA"
_LANG_URI_ITA = "http://publications.europa.eu/resource/authority/language/ITA"
_LANG_URI = {"de": _LANG_URI_DEU, "fr": _LANG_URI_FRA, "it": _LANG_URI_ITA}

# typeDocument vocabulary URIs that mark "this is a Federal Council
# Message". Probed empirically on 2026-05-07 against the live SPARQL
# endpoint:
#   td=23 → Botschaft (~1,874 with German title 'Botschaft*' + td=23)
#   td=21 → Bundesgesetz (law text — useless for purpose-of-article)
#   td=8  → Bundesbeschluss (federal decree)
#   td=33 → Mitteilung / Notifikation / Allgemeinverfügung (noise)
#   td=40 → Vernehmlassungsverfahren
# The Botschaft type is td=23. The other ~8,766 FGA URIs whose title
# starts 'Botschaft*' but have no typeDocument are caught by the title
# fallback below.
_BOTSCHAFT_TYPE_URI = "https://fedlex.data.admin.ch/vocabulary/resource-type/23"
_BOTSCHAFT_TITLE_PREFIX = {"de": "botschaft", "fr": "message", "it": "messaggio"}


def is_botschaft(eli_uri: str, language: str = "de") -> bool:
    """Two-pronged check: SPARQL ASK true if either the URI's
    typeDocument is the Botschaft vocabulary URI OR its title in the
    requested language starts with 'Botschaft' / 'Message' / 'Messaggio'.

    Filters out the ~204K noise FGA URIs (Mitteilungen, Allgemein-
    verfügungen, Notifikationen) before the expensive PDF/XML fetch.
    """
    if fetch_manifestations is None:  # SPARQL helper not importable
        return True  # fail-open: keep v0.2's behavior
    import urllib.parse
    import urllib.request
    import json as _json
    lang_iri = _LANG_URI.get(language, _LANG_URI_DEU)
    prefix = _BOTSCHAFT_TITLE_PREFIX.get(language, "botschaft")
    ask_q = f"""
    PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>
    ASK {{
      <{eli_uri}> jolux:isRealizedBy ?expr .
      ?expr jolux:language <{lang_iri}> .
      {{ <{eli_uri}> jolux:typeDocument <{_BOTSCHAFT_TYPE_URI}> . }}
      UNION
      {{ ?expr jolux:title ?title .
        FILTER(STRSTARTS(LCASE(STR(?title)), "{prefix}")) }}
    }}
    """
    body = urllib.parse.urlencode({"query": ask_q}).encode()
    req = urllib.request.Request(
        "https://fedlex.data.admin.ch/sparqlendpoint",
        data=body,
        headers={
            "Accept": "application/sparql-results+json",
            "User-Agent": "opencaselaw-materialien/0.3",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return bool(_json.loads(r.read()).get("boolean", False))
    except Exception as e:
        log.warning(f"  is_botschaft ASK failed for {eli_uri}: {e}")
        # fail-open: don't lose data on a transient SPARQL error
        return True


def resolve_manifestation(
    eli_uri: str,
    language: str = "de",
    strict_language: bool = True,
) -> tuple[str | None, str | None]:
    """Return ``(file_url, format)`` for the best manifestation in
    ``language``, or ``(None, None)`` if Fedlex SPARQL has no entry.

    With ``strict_language=True`` (the default since v0.3) we never fall
    back to a different language — get_article_purpose returning a
    French Botschaft to a German caller is a worse failure mode than
    returning empty (the caller can retry with the canonical language).
    Pass ``strict_language=False`` to opt back into v0.2 behaviour.

    Coverage: post-~2003 BBl publications. Pre-2003 returns no
    manifestations and callers must fall through to a different source
    (amtsdruckschriften.bar.admin.ch — v0.5 work).
    """
    if fetch_manifestations is None:
        return (None, None)
    try:
        mans = fetch_manifestations(eli_uri)
    except Exception as e:
        log.warning(f"  SPARQL manifestation lookup failed for {eli_uri}: {e}")
        return (None, None)

    # Filter to requested language; prefer best format.
    same_lang = [m for m in mans if m.language == language]
    if strict_language:
        pool = same_lang
    else:
        pool = same_lang or mans  # any-language fallback (legacy)
    by_format: dict[str, str] = {m.format: m.file_url for m in pool}
    for fmt in _FORMAT_PRIORITY:
        if fmt in by_format:
            return (by_format[fmt], fmt)
    # Otherwise return whatever's first (usually docx/html — last resort).
    if pool:
        return (pool[0].file_url, pool[0].format)
    return (None, None)


# ── Akoma Ntoso XML parser ────────────────────────────────────────────

_AKN_NS = "{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}"


def _strip_ns(tag: str) -> str:
    """Strip the Akoma Ntoso namespace from an element tag."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _akn_text(elem: ET.Element) -> str:
    """Concatenate all text within an element, normalising whitespace."""
    parts: list[str] = []
    for x in elem.iter():
        if x.text:
            parts.append(x.text)
        if x.tail:
            parts.append(x.tail)
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def parse_akoma_ntoso_xml(xml_bytes: bytes) -> Iterator[dict]:
    """Parse a Fedlex Akoma Ntoso BBl document into paragraph dicts.

    Yielded shape matches ``parse_botschaft_text``:
        {para_order, page_number, section_path, article_anchor, text, text_length}

    Notes
    -----
    Akoma Ntoso uses ``<article eId="art_N">`` to anchor per-article
    sections. Sections / chapters provide the breadcrumb. ``<p>`` and
    ``<paragraph>`` carry the text. Many Fedlex FGA publications are
    metadata-only wrappers (no ``<body>``) — yielding 0 paragraphs is
    a valid signal to fall through to PDF.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        log.warning(f"  XML parse error: {e}")
        return

    # Walk the body. If there's no body, we yield nothing.
    body = next(
        (x for x in root.iter() if _strip_ns(x.tag) in ("body", "mainBody")),
        None,
    )
    if body is None:
        return

    section_stack: list[str] = []
    current_anchor: str | None = None
    para_order = 0

    def _emit(text: str) -> dict | None:
        nonlocal para_order
        text = text.strip()
        if len(text) < 20:
            return None
        para_order += 1
        return {
            "para_order": para_order,
            "page_number": None,  # XML doesn't carry pages
            "section_path": " > ".join(section_stack) or None,
            "article_anchor": current_anchor,
            "text": text,
            "text_length": len(text),
        }

    for elem in body.iter():
        tag = _strip_ns(elem.tag)
        if tag == "article":
            # Anchor change. eId="art_41" → "41"
            eid = elem.attrib.get("eId") or elem.attrib.get("id") or ""
            m = re.match(r"art_(\d+[a-z]?)$", eid)
            if m:
                current_anchor = m.group(1)
        elif tag in ("chapter", "section", "subsection", "part"):
            heading_el = next(
                (c for c in elem if _strip_ns(c.tag) == "heading"), None,
            )
            if heading_el is not None:
                section_stack = [_akn_text(heading_el)[:80]]
        elif tag in ("p", "paragraph", "blockList", "intro"):
            # Skip nested paragraphs — outer iter() already visits them.
            # Only emit when this element has no <p>/<paragraph> children
            # (it's a leaf paragraph).
            has_inner = any(
                _strip_ns(c.tag) in ("p", "paragraph") for c in elem
            )
            if has_inner:
                continue
            row = _emit(_akn_text(elem))
            if row is not None:
                yield row


# ── Article-anchor parsing ────────────────────────────────────────────

# Header patterns Botschaften use for per-article comments. Captures
# the article number (with optional letter suffix like "41a"). Designed
# to match a line that starts with the marker after possible leading
# whitespace and a chapter indent — but NOT a mid-sentence reference
# like "wie in Artikel 41 vorgesehen".
ARTICLE_HEADER_RE = re.compile(
    r"""^\s{0,8}                       # optional indent
        (?:Zu\s+|Ad\s+|All['’]\s*)?    # German "Zu", French "Ad", Italian "All'"
        (?:Art(?:ikel|\.|icolo)?\.?)   # Art / Artikel / Articolo / Art.
        \s+
        (\d+[a-z]?)                    # captured article number, e.g. "41" or "41a"
        \b
        (?:.{0,80}?)?                  # optional title text on the same line
        $""",
    re.IGNORECASE | re.VERBOSE | re.MULTILINE,
)

# A "section header" = capitalised line of <= 80 chars surrounded by
# blank lines. Used to track section_path while walking the PDF.
SECTION_HEADER_RE = re.compile(
    r"^\s{0,8}([A-ZÄÖÜ][^a-z\n]{2,80}|\d+(?:\.\d+)*\s+[A-ZÄÖÜ].{0,80})\s*$",
    re.MULTILINE,
)


def parse_botschaft_text(
    pages: list[str],
    language: str = "de",
) -> Iterator[dict]:
    """Yield paragraph dicts (page_number, section_path, article_anchor, text).

    Input ``pages`` is a list of plain-text page strings (one per PDF page).
    Each non-empty paragraph (split on double newline) becomes one dict.
    The article anchor is set to the most recent ``Zu Art. N`` header
    above this paragraph, until the next such header is seen. The
    section path is a slash-joined trail of the recent section headers.
    """
    section_path_stack: list[str] = []
    current_anchor: str | None = None
    para_order = 0

    for page_idx, page_text in enumerate(pages, start=1):
        # Split on 2+ newlines to approximate paragraph boundaries.
        for chunk in re.split(r"\n\s*\n", page_text):
            chunk = chunk.strip()
            if len(chunk) < 20:
                continue

            # Update section trail if this chunk is itself a header.
            sh = SECTION_HEADER_RE.match(chunk)
            if sh and len(chunk.split("\n")) == 1:
                # Treat as section header — push and don't emit as paragraph.
                # Reset article anchor: a top-level section ("Übersicht",
                # "Schlussbestimmungen", …) ends the run of "Zu Art. N"
                # paragraphs. Without this reset the previous anchor
                # leaks into unrelated paragraphs of the next section.
                section_path_stack = [chunk]
                current_anchor = None
                continue

            # Detect article anchor on the FIRST line of the chunk.
            first_line = chunk.split("\n", 1)[0]
            ah = ARTICLE_HEADER_RE.match(first_line)
            if ah:
                current_anchor = ah.group(1)

            para_order += 1
            yield {
                "para_order": para_order,
                "page_number": page_idx,
                "section_path": " > ".join(section_path_stack) or None,
                "article_anchor": current_anchor,
                "text": chunk,
                "text_length": len(chunk),
            }


# ── Ingest one Botschaft ──────────────────────────────────────────────


def fetch_pdf_bytes(url: str, timeout: int = 60) -> bytes:
    import urllib.request
    log.info(f"  fetching {url}")
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "opencaselaw-materialien/0.3"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


_PART_SUFFIX_RE = re.compile(r"-(\d+)\.pdf$", re.IGNORECASE)


def _url_exists(url: str, timeout: int = 15) -> bool:
    """HEAD-probe a URL; True only if it returns 2xx AND the response
    Content-Type is ``application/pdf``.

    Fedlex sometimes responds 200 with an HTML SPA wrapper for missing
    PDF parts; we don't want those treated as real parts. Checking the
    declared Content-Type catches that case before we waste a GET.
    """
    import urllib.request
    req = urllib.request.Request(
        url,
        method="HEAD",
        headers={"User-Agent": "opencaselaw-materialien/0.3"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if not (200 <= r.status < 300):
                return False
            ct = (r.headers.get("Content-Type") or "").lower()
            return "application/pdf" in ct
    except Exception:
        return False


def fetch_pdf_parts(
    first_url: str,
    timeout: int = 60,
    max_parts: int = 200,
) -> list[bytes]:
    """Fetch ``first_url`` and any sibling parts.

    Fedlex splits big Botschaften into ``…-1.pdf``, ``…-2.pdf``, …
    SPARQL only reports the first manifestation; we have to discover
    ``-2.pdf``, ``-3.pdf`` etc. by HEAD-probing siblings until we hit a
    404. Returns a list of raw bytes objects, one per part, in order.

    For URLs that don't follow the ``-N.pdf`` Fedlex convention this
    degrades to a single-element list (caller behaviour unchanged from
    v0.2).
    """
    parts: list[bytes] = [fetch_pdf_bytes(first_url, timeout=timeout)]
    m = _PART_SUFFIX_RE.search(first_url)
    if not m or int(m.group(1)) != 1:
        return parts  # not a multi-part URL we can extend
    prefix = first_url[: m.start()]
    for n in range(2, max_parts + 1):
        url = f"{prefix}-{n}.pdf"
        if not _url_exists(url, timeout=15):
            break
        log.info(f"  multi-part: discovered part {n}")
        parts.append(fetch_pdf_bytes(url, timeout=timeout))
    if len(parts) > 1:
        log.info(f"  multi-part: stitched {len(parts)} parts total")
    return parts


def extract_pdf_pages(pdf_bytes: bytes) -> list[str]:
    """Extract text per page using pdfplumber. Falls back to empty
    string for pages that fail. Returns one entry per page."""
    import io
    try:
        import pdfplumber
    except ImportError as e:
        raise SystemExit(
            "pdfplumber not installed. Run: pip install pdfplumber"
        ) from e

    pages_out: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            try:
                pages_out.append(page.extract_text() or "")
            except Exception as e:
                log.warning(f"    page extract failed: {e}")
                pages_out.append("")
    return pages_out


def ingest_one(
    conn: sqlite3.Connection,
    year: int,
    page: int,
    language: str = "de",
    sr_number: str | None = None,
    article: str | None = None,
    relation: str = "considered",
) -> int | None:
    """Fetch, parse, and store one Botschaft. Returns botschaft_id, or
    None if Fedlex has no manifestation (pre-~2003 BBl).

    v0.2 (2026-05-07):
      - URL resolved via Fedlex SPARQL (``resolve_manifestation``);
        previously a naive URL builder returned the SPA HTML wrapper.
      - Format priority: xml-an > xml > pdf-a-an > pdf-a > pdf.
      - XML path uses the Akoma Ntoso parser; PDF path uses pdfplumber.
      - Idempotent: text_hash gates re-ingestion.
    """
    ensure_schema(conn)

    citation = bbl_citation(year, page)
    eli = bbl_eli_uri(year, page)
    log.info(f"Ingesting {citation} ({language}) for sr={sr_number} art={article}")

    # v0.3 (2026-05-07): pre-filter via Fedlex SPARQL — only ingest
    # publications whose typeDocument == resource-type/23 OR whose
    # title starts with 'Botschaft'. Drops Mitteilungen / decrees /
    # notices that the amendment_refs index pulls in as false matches.
    if not is_botschaft(eli, language=language):
        log.info(
            f"  skipping {citation} ({language}) — not a Botschaft "
            f"(typeDocument != 23 AND title doesn't start with 'Botschaft')"
        )
        return None

    url, fmt = resolve_manifestation(eli, language=language)
    if url is None:
        log.warning(
            f"  no Fedlex manifestation for {citation} ({language}) — "
            f"pre-2003 publications need v0.3 amtsdruckschriften adapter"
        )
        return None
    log.info(f"  resolved → {fmt}: {url[:120]}")

    # v0.3 (2026-05-07): for PDF formats we probe Fedlex's multi-part
    # convention (``-1.pdf``/``-2.pdf``…) so big Botschaften aren't
    # truncated to their first part. XML formats are single-file.
    if fmt and fmt.startswith("pdf"):
        parts = fetch_pdf_parts(url)
    else:
        parts = [fetch_pdf_bytes(url)]
    # Hash the concatenation so a new part triggers re-ingest.
    text_hash = hashlib.sha256(b"".join(parts)).hexdigest()
    raw = parts[0]  # XML parser only needs the first (single) part

    row = conn.execute(
        "SELECT botschaft_id, text_hash FROM botschaft_documents "
        "WHERE bbl_year=? AND bbl_page=? AND language=?",
        (year, page, language),
    ).fetchone()

    if row and row[1] == text_hash:
        log.info(f"  unchanged ({citation}) — keeping existing rows")
        botschaft_id = row[0]
    else:
        # Parse: prefer XML/Akoma Ntoso, fall back to PDF.
        paragraphs: list[dict] = []
        page_count = 0
        if fmt and fmt.startswith(("xml", "pdf-a-an")):
            # Try Akoma Ntoso XML. (The ``-an`` PDF variant carries
            # Akoma Ntoso semantics in metadata; we can still parse it
            # as PDF, but actual XML is preferred.)
            if fmt.startswith("xml"):
                paragraphs = list(parse_akoma_ntoso_xml(raw))
                log.info(f"  XML parsed {len(paragraphs)} paragraphs")
        if not paragraphs:
            # PDF fallback — either fmt was pdf-a or the XML was an
            # FRBR metadata wrapper (no body content).
            try:
                pdf_pages: list[str] = []
                for part_bytes in parts:
                    pdf_pages.extend(extract_pdf_pages(part_bytes))
                page_count = len(pdf_pages)
                paragraphs = list(parse_botschaft_text(pdf_pages, language=language))
                log.info(
                    f"  PDF parsed {page_count} pages "
                    f"({len(parts)} part{'s' if len(parts) != 1 else ''}) "
                    f"→ {len(paragraphs)} paragraphs"
                )
            except Exception as e:
                log.warning(f"  PDF parse failed: {e}")
                paragraphs = []

        # Storage format label: keep XML if we successfully parsed XML,
        # else mark PDF.
        stored_format = "akoma-ntoso-xml" if fmt and fmt.startswith("xml") and paragraphs else "pdf"

        if row:
            conn.execute(
                "DELETE FROM botschaft_paragraphs WHERE botschaft_id = ?",
                (row[0],),
            )
            conn.execute(
                "UPDATE botschaft_documents SET text_hash=?, page_count=?, "
                "format=?, source_url=?, ingested_at=? WHERE botschaft_id=?",
                (text_hash, page_count, stored_format, url,
                 datetime.now(timezone.utc).isoformat(), row[0]),
            )
            botschaft_id = row[0]
        else:
            cur = conn.execute(
                """
                INSERT INTO botschaft_documents
                (bbl_year, bbl_page, bbl_citation, eli_uri, source_url,
                 format, language, page_count, text_hash, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (year, page, citation, eli, url, stored_format, language,
                 page_count, text_hash,
                 datetime.now(timezone.utc).isoformat()),
            )
            botschaft_id = cur.lastrowid

        n_paras = 0
        for p in paragraphs:
            conn.execute(
                """
                INSERT INTO botschaft_paragraphs
                (botschaft_id, para_order, page_number, section_path,
                 article_anchor, text, text_length)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (botschaft_id, p["para_order"], p["page_number"],
                 p["section_path"], p["article_anchor"],
                 p["text"], p["text_length"]),
            )
            n_paras += 1
        log.info(f"  inserted {n_paras} paragraphs into botschaft_paragraphs")

    if sr_number and article:
        conn.execute(
            """
            INSERT OR IGNORE INTO article_botschaft_links
            (sr_number, article, botschaft_id, relation, evidence)
            VALUES (?, ?, ?, ?, ?)
            """,
            (sr_number, article, botschaft_id, relation,
             "build_botschaft_corpus.ingest_one v0.2"),
        )
    conn.commit()
    return botschaft_id


# ── CLI ────────────────────────────────────────────────────────────────


def link_from_amendment_refs(conn: sqlite3.Connection) -> dict:
    """Populate ``article_botschaft_links`` from ``amendment_refs``.

    For every Botschaft already in ``botschaft_documents``, find every
    matching ``amendment_refs`` row (same year + page) and create a link
    row tying the Botschaft to that article. Idempotent — uses
    ``INSERT OR IGNORE`` against the link table's PK.

    The relation is set to ``considered`` because ``amendment_refs``
    doesn't carry enacted-vs-amended semantics; a future enrichment can
    upgrade specific links via Fedlex's history API.

    Returns a stats dict.
    """
    ensure_schema(conn)
    # Sanity: the existing materialien.db builder may not have created
    # amendment_refs yet on a fresh checkout.
    try:
        conn.execute("SELECT 1 FROM amendment_refs LIMIT 1").fetchone()
    except sqlite3.OperationalError:
        log.warning(
            "  amendment_refs table not present — run build_materialien_db "
            "first to populate it from statute footnotes"
        )
        return {"linked": 0, "reason": "no_amendment_refs"}

    before = conn.execute(
        "SELECT COUNT(*) FROM article_botschaft_links"
    ).fetchone()[0]

    cur = conn.execute(
        """
        INSERT OR IGNORE INTO article_botschaft_links
            (sr_number, article, botschaft_id, relation, evidence)
        SELECT DISTINCT
            ar.sr_number,
            ar.article,
            bd.botschaft_id,
            'considered' AS relation,
            'amendment_refs:' || ar.id AS evidence
        FROM amendment_refs ar
        JOIN botschaft_documents bd
          ON bd.bbl_year = ar.year AND bd.bbl_page = ar.page
        WHERE ar.ref_type = 'BBl'
          AND ar.sr_number IS NOT NULL
          AND ar.article IS NOT NULL
          AND ar.year IS NOT NULL
          AND ar.page IS NOT NULL
        """
    )
    conn.commit()
    after = conn.execute(
        "SELECT COUNT(*) FROM article_botschaft_links"
    ).fetchone()[0]
    new_links = after - before
    log.info(
        f"  article_botschaft_links: {before} → {after} (+{new_links} new)"
    )

    # Coverage breakdown
    by_sr = conn.execute(
        """
        SELECT sr_number, COUNT(*) AS n
        FROM article_botschaft_links
        GROUP BY sr_number
        ORDER BY n DESC
        LIMIT 5
        """
    ).fetchall()
    if by_sr:
        log.info("  top SR coverage: " + ", ".join(
            f"{s}={n}" for s, n in by_sr
        ))
    return {
        "linked": new_links,
        "total_links": after,
        "top_sr": [{"sr_number": s, "links": n} for s, n in by_sr],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--db",
        default=str(Path("output/materialien.db")),
        help="materialien.db path",
    )
    parser.add_argument(
        "--schema-only", action="store_true",
        help="just create the new tables and exit",
    )
    parser.add_argument(
        "--ingest-one", action="store_true",
        help="ingest a single Botschaft (PoC); requires --year + --page",
    )
    parser.add_argument("--year", type=int)
    parser.add_argument("--page", type=int)
    parser.add_argument("--sr", help="SR number to link the article to")
    parser.add_argument("--article", help="article number (links to the Botschaft)")
    parser.add_argument(
        "--language", default="de", choices=["de", "fr", "it"],
    )
    parser.add_argument(
        "--ingest-all", action="store_true",
        help="discover BBl refs from amendment_refs and ingest all Botschaften (live)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap the number of candidates processed in --ingest-all (smoke-test).",
    )
    parser.add_argument(
        "--rate-limit", type=float, default=0.3,
        help="Seconds to sleep between fetches in --ingest-all (default 0.3).",
    )
    parser.add_argument(
        "--min-year", type=int, default=2003,
        help="Lowest BBl year to consider in --ingest-all (Fedlex coverage starts ~2003).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="--ingest-all dry-run: list candidates without fetching.",
    )
    parser.add_argument(
        "--link-amendment-refs", action="store_true",
        help=(
            "Auto-populate article_botschaft_links from the existing "
            "amendment_refs table for every ingested Botschaft. Idempotent."
        ),
    )
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        log.error(f"DB not found: {db_path}")
        return 1

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    if args.schema_only:
        ensure_schema(conn)
        log.info("schema migrated")
        return 0

    if args.ingest_one:
        if not args.year or not args.page:
            log.error("--ingest-one requires --year and --page")
            return 2
        bid = ingest_one(
            conn,
            year=args.year, page=args.page,
            language=args.language,
            sr_number=args.sr,
            article=args.article,
            relation="enacted" if args.article else "considered",
        )
        log.info(f"botschaft_id = {bid}")
        return 0

    if args.link_amendment_refs:
        stats = link_from_amendment_refs(conn)
        import json as _json
        print(_json.dumps(stats, indent=2, ensure_ascii=False))
        return 0

    if args.ingest_all:
        rows = conn.execute(
            """
            SELECT DISTINCT ar.year, ar.page
            FROM amendment_refs ar
            LEFT JOIN botschaft_documents bd
              ON bd.bbl_year = ar.year AND bd.bbl_page = ar.page
              AND bd.language = ?
            WHERE ar.ref_type = 'BBl'
              AND ar.year IS NOT NULL AND ar.page IS NOT NULL
              AND ar.year >= ?
              AND bd.botschaft_id IS NULL
            ORDER BY ar.year DESC, ar.page DESC
            """,
            (args.language, args.min_year),
        ).fetchall()
        candidates = [(int(r[0]), int(r[1])) for r in rows]
        if args.limit:
            candidates = candidates[: args.limit]
        log.info(
            f"--ingest-all: {len(candidates)} unique (year, page) candidates "
            f"≥ {args.min_year} not yet in materialien.db, lang={args.language}"
        )
        if args.dry_run:
            for year, page in candidates[:25]:
                log.info(f"  candidate: BBl {year} {page}")
            if len(candidates) > 25:
                log.info(f"  … and {len(candidates) - 25} more")
            return 0

        import time as _time
        stats = {"accepted": 0, "rejected_by_filter": 0, "errors": 0,
                 "total": len(candidates), "started_at": datetime.now(timezone.utc).isoformat()}
        t0 = _time.time()
        for i, (year, page) in enumerate(candidates, 1):
            try:
                bid = ingest_one(
                    conn, year=year, page=page,
                    language=args.language,
                    sr_number=None, article=None,
                    relation="considered",
                )
                if bid is None:
                    stats["rejected_by_filter"] += 1
                else:
                    stats["accepted"] += 1
            except Exception as e:
                stats["errors"] += 1
                log.warning(f"  BBl {year} {page} ({args.language}) failed: {e}")
            _time.sleep(args.rate_limit)
            if i % 25 == 0 or i == len(candidates):
                elapsed = _time.time() - t0
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(candidates) - i) / rate if rate > 0 else 0
                log.info(
                    f"  progress {i}/{len(candidates)} "
                    f"(accepted={stats['accepted']} "
                    f"filtered={stats['rejected_by_filter']} "
                    f"errors={stats['errors']}) "
                    f"rate={rate:.2f}/s eta={int(eta/60)}min"
                )

        # Auto-link from amendment_refs after bulk ingest
        log.info("Auto-linking from amendment_refs…")
        link_stats = link_from_amendment_refs(conn)
        stats["link_stats"] = link_stats
        stats["elapsed_seconds"] = round(_time.time() - t0, 2)
        print(json.dumps(stats, indent=2, ensure_ascii=False))
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())

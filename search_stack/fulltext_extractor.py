#!/usr/bin/env python3
"""Shared full-text extractor used by Options B (batch) + C (on-demand MCP).

Given a publication's URL or pdf_url:
  1. License-gate: only proceed for permissively-licensed records
     (CC-BY/SA/NC + non-ND variants, OA-Swiss-federal,
     OA-author-permitted-reuse). Skip CC-BY-ND, rightsstatements-in-
     copyright, raw copyright statements.
  2. Resolve to a PDF URL (per-source heuristics for landing pages).
  3. Download with rate-limit + retries.
  4. Extract text with pymupdf (fitz).
  5. Return text + provenance + bytes-fetched.

The PDF file is optionally cached on disk for re-extraction.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

log = logging.getLogger("fulltext_extractor")

USER_AGENT = (
    "OpenCaseLaw-scholarship/0.1 (mailto:scholarship@opencaselaw.ch; "
    "+https://opencaselaw.ch)"
)

# Permissive licenses we'll fully extract for. CC-BY-ND-* is excluded
# because ND restricts derivative works; storing extracted text could
# be argued as a derivative form (conservative interpretation).
_PERMISSIVE_CC = re.compile(
    r"^(CC-BY(?!.*-ND)[A-Z0-9.\-]*|OA-Swiss-federal|"
    r"OA-author-permitted-reuse)$",
    re.I,
)
# OpenAIRE / DRIVER access-level markers used by DSpace 7 IRs when the
# upstream dc:rights doesn't include a specific CC license URL. These
# indicate the work is freely-accessible (the IR's deposit policy
# requires OA); the specific licensing terms may vary per record but
# the access is permitted. We treat these as permissive for full-text
# extraction; downstream re-use must still respect upstream-specific
# author/publisher terms (we surface the source URL with every result).
_PERMISSIVE_ACCESS = {
    "info:eu-repo/semantics/openaccess",
    "openaccess",
    "open access",       # BORIS Bern plain-text label
    "open_access",
    "free access",
}

# Sources whose deposit policy requires OA — we treat NULL-license
# records from these IRs as openly accessible (the IR's mandate is
# the implicit license signal). Downstream users still get the source
# URL and per-source attribution.
_OA_BY_POLICY_SOURCES = {
    "edoc_unibas_law",   # UniBas edoc — institutional OA mandate
    "libra_unine",       # UniNE LIBRA — institutional OA mandate
}


def is_permissive_license(
    license_code: str | None,
    source: str | None = None,
) -> bool:
    if license_code:
        if _PERMISSIVE_CC.match(license_code):
            return True
        if license_code.lower() in _PERMISSIVE_ACCESS:
            return True
    # NULL-license: only if the source's deposit policy is itself OA
    if not license_code and source in _OA_BY_POLICY_SOURCES:
        return True
    return False


def fetch_bytes(url: str, timeout: int = 60, max_bytes: int = 50_000_000) -> bytes | None:
    """Fetch a URL up to max_bytes. Returns None on failure."""
    req = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read(max_bytes)
    except Exception as e:
        log.debug("fetch failed %s: %s", url, e)
        return None


# Per-source PDF-link resolution. Returns the direct PDF URL given
# a publication record (dict), or None.

_CITATION_PDF_RE = re.compile(
    r'<meta[^>]+name=["\']citation_pdf_url["\'][^>]+content=["\']([^"\']+)["\']',
    re.I,
)
_UUID_IN_URL_RE = re.compile(
    r"/(?:items|entities/publication)/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.I,
)
_HANDLE_PATH_RE = re.compile(
    r"/handle/([0-9]+(?:\.[0-9]+)*/[0-9]+)",
)
_HDL_HANDLENET_RE = re.compile(
    r"hdl\.handle\.net/([0-9]+(?:\.[0-9]+)*/[0-9]+)",
)


def _ojs_pdf(record: dict) -> str | None:
    """Generic OJS resolver: fetch article landing page, find citation_pdf_url
    meta tag. Works for cognitio, LEOH, ex_ante, cfs, and any standard OJS.
    Returns None for sui-generis (which serves HTML fulltext only).
    """
    landing = record.get("url")
    if not landing:
        return None
    data = fetch_bytes(landing, timeout=15, max_bytes=2_000_000)
    if not data:
        return None
    try:
        html = data.decode("utf-8", errors="replace")
    except Exception:
        return None
    m = _CITATION_PDF_RE.search(html)
    if m:
        return m.group(1).strip()
    return None


# Handle-prefix → IR host. When a record's URL is the generic
# https://hdl.handle.net/PREFIX/N redirector, we need the IR's own host
# to query its REST API. Resolved 2026-05-27 by reading the redirect
# Location header of one sample handle per IR.
_HANDLE_PREFIX_TO_HOST = {
    "20.500.14716": "https://edoc.unibas.ch",
    "20.500.12422": "https://boris-portal.unibe.ch",
    "20.500.11850": "https://www.research-collection.ethz.ch",
    "20.500.14171": "https://www.alexandria.unisg.ch",
    "20.500.14299": "https://infoscience.epfl.ch",
    "11475":        "https://digitalcollection.zhaw.ch",
    "11654":        "https://irf.fhnw.ch",
    "123456789":    "https://libra.unine.ch",
}


def _follow_redirect(url: str, timeout: int = 15) -> str | None:
    """Issue a HEAD-like GET and return the FINAL URL after redirects."""
    req = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.geturl()
    except Exception:
        return None


def _dspace7_pdf(record: dict) -> str | None:
    """DSpace 7 resolver: derive (IR_host, item_uuid) from a record URL,
    query <IR_host>/server/api/core/items/<uuid>/bundles → ORIGINAL bundle
    → first PDF bitstream's content URL.

    Three URL flavors handled:
      a) IR-direct host with /entities/publication/<uuid> in URL
      b) Generic hdl.handle.net/<prefix>/<id> — followed via redirect
         or resolved via the handle-prefix → IR-host map below
      c) IR-direct host with /handle/<prefix>/<id> — handle-prefix map
         gives us the API base, then find-by-handle returns the UUID
    """
    landing = record.get("url") or ""
    if not landing:
        return None
    base = None
    uuid = None
    # Path A: UUID already in URL
    m = _UUID_IN_URL_RE.search(landing)
    if m:
        uuid = m.group(1).lower()
        m_host = re.match(r"(https?://[^/]+)", landing)
        base = m_host.group(1) if m_host else None
    # Path B: hdl.handle.net redirector
    elif "hdl.handle.net" in landing:
        m_h = _HANDLE_PATH_RE.search(landing) or _HDL_HANDLENET_RE.search(landing)
        if m_h:
            handle = m_h.group(1)
            prefix = handle.split("/", 1)[0]
            base = _HANDLE_PREFIX_TO_HOST.get(prefix)
        if not base:
            # Last resort: follow the redirect
            final = _follow_redirect(landing)
            if final:
                m_uuid = _UUID_IN_URL_RE.search(final)
                if m_uuid:
                    uuid = m_uuid.group(1).lower()
                m_host = re.match(r"(https?://[^/]+)", final)
                if m_host:
                    base = m_host.group(1)
    # Path C: IR-direct /handle/<prefix>/<id>
    else:
        m_h = _HANDLE_PATH_RE.search(landing)
        if m_h:
            m_host = re.match(r"(https?://[^/]+)", landing)
            base = m_host.group(1) if m_host else None
            # Try find-by-handle (DSpace 7 standard)
            if base:
                findurl = (
                    f"{base}/server/api/pid/find?id=hdl:"
                    f"{urllib.parse.quote(m_h.group(1))}"
                )
                data = fetch_bytes(findurl, timeout=15, max_bytes=200_000)
                if data:
                    try:
                        j = json.loads(data)
                        uuid = j.get("uuid") or j.get("id")
                    except Exception:
                        pass
        if not uuid and base:
            # Last resort: fetch landing page, extract UUID from /entities/publication/<uuid>
            data = fetch_bytes(landing, timeout=15, max_bytes=1_000_000)
            if data:
                try:
                    html = data.decode("utf-8", errors="replace")
                    m = _UUID_IN_URL_RE.search(html)
                    if m:
                        uuid = m.group(1).lower()
                except Exception:
                    pass
    if not base:
        return None
    # If we have the IR-host but no UUID, try find-by-handle
    if not uuid:
        m_h = _HANDLE_PATH_RE.search(landing) or _HDL_HANDLENET_RE.search(landing)
        if m_h:
            findurl = (
                f"{base}/server/api/pid/find?id=hdl:"
                f"{urllib.parse.quote(m_h.group(1))}"
            )
            data = fetch_bytes(findurl, timeout=15, max_bytes=200_000)
            if data:
                try:
                    j = json.loads(data)
                    uuid = j.get("uuid") or j.get("id")
                except Exception:
                    pass
    if not uuid:
        return None
    # Query bundles
    bundles_url = (
        f"{base}/server/api/core/items/{uuid}/bundles?embed=bitstreams"
    )
    data = fetch_bytes(bundles_url, timeout=20, max_bytes=1_000_000)
    if not data:
        return None
    try:
        j = json.loads(data)
    except Exception:
        return None
    bundles = (
        j.get("_embedded", {}).get("bundles", [])
        if isinstance(j, dict) else []
    )
    if not bundles:
        return None
    # Scan EVERY bundle for the first .pdf bitstream — many DSpace
    # deployments split content across ORIGINAL / CONTENT / etc.
    pdf_bs = None
    fallback_bs = None
    # Prefer ORIGINAL bundle, but search all
    bundle_order = (
        [b for b in bundles if b.get("name") == "ORIGINAL"]
        + [b for b in bundles if b.get("name") != "ORIGINAL"]
    )
    for b in bundle_order:
        bs = (
            b.get("_embedded", {})
            .get("bitstreams", {})
            .get("_embedded", {})
            .get("bitstreams", [])
        )
        for bit in bs:
            name = (bit.get("name") or "").lower()
            if name.endswith(".pdf"):
                pdf_bs = bit
                break
            if fallback_bs is None and not name.endswith((".txt", ".jpg", ".png")):
                fallback_bs = bit
        if pdf_bs:
            break
    target = pdf_bs or fallback_bs
    if not target:
        return None
    return (
        target.get("_links", {})
        .get("content", {})
        .get("href")
    )


def _eperiodica_pdf(record: dict) -> str | None:
    """e-periodica: PDF URL is in all_identifiers as cntmng?type=pdf&pid=..."""
    raw = record.get("raw_metadata")
    if raw:
        try:
            d = json.loads(raw) if isinstance(raw, str) else raw
            for ident in d.get("all_identifiers") or []:
                if "cntmng?type=pdf" in ident:
                    return ident
        except Exception:
            pass
    return None


def _anci_pdf(record: dict) -> str | None:
    return record.get("pdf_url")


def _onlinekommentar_pdf(record: dict) -> str | None:
    return record.get("pdf_url")


def _generic_pdf(record: dict) -> str | None:
    """Last-resort: if url itself ends in .pdf use it directly."""
    url = record.get("url") or record.get("pdf_url")
    if url and url.lower().endswith(".pdf"):
        return url
    return None


# Per-source resolvers, by source key (most-specific first)
PDF_RESOLVERS = {
    # e-periodica: PDF URL is in OAI all_identifiers as cntmng?type=pdf
    "e_periodica_law":      _eperiodica_pdf,
    "e_periodica_polsci":   _eperiodica_pdf,
    "e_periodica_pubadmin": _eperiodica_pdf,
    # Direct pdf_url column
    "anci_ch":              _anci_pdf,
    "onlinekommentar":      _onlinekommentar_pdf,
    # OJS journals: citation_pdf_url meta on landing page
    "leoh":                 _ojs_pdf,
    "cognitio":             _ojs_pdf,
    "cfs":                  _ojs_pdf,
    "ex_ante":              _ojs_pdf,
    "sui_generis":          _ojs_pdf,  # may return None (HTML-only fulltext)
    # UNIGE Archive ouverte uses the same academic citation_pdf_url
    # meta convention even though it's NOT OJS. Custom non-DSpace URL
    # format (`archive-ouverte.unige.ch/unige:N`) → fetch landing →
    # citation_pdf_url points at access.archive-ouverte.unige.ch.
    "unige_law":            _ojs_pdf,
    # DSpace 7 IRs: REST /server/api/core/items/<uuid>/bundles
    "edoc_unibas_law":      _dspace7_pdf,
    "boris_law":            _dspace7_pdf,
    # unige_law is OJS-style (uses citation_pdf_url), mapped above
    "zhaw_digitalcollection": _dspace7_pdf,
    "fhnw_irf":             _dspace7_pdf,
    "libra_unine":          _dspace7_pdf,
    "alexandria_law":       _dspace7_pdf,
    "eth_research_collection": _dspace7_pdf,
    "repositorium_ch":      _dspace7_pdf,  # may need its own (Supabase-backed)
}


def resolve_pdf_url(record: dict) -> str | None:
    """Find a directly-downloadable PDF URL for a publication record."""
    src = record.get("source") or ""
    resolver = PDF_RESOLVERS.get(src)
    if resolver:
        pdf = resolver(record)
        if pdf:
            return pdf
    return _generic_pdf(record)


def extract_text_from_pdf(data: bytes) -> str | None:
    """Extract plain text from PDF bytes via pymupdf (fitz)."""
    try:
        import fitz  # type: ignore
    except ImportError:
        log.error("pymupdf (fitz) not installed")
        return None
    try:
        doc = fitz.open(stream=data, filetype="pdf")
        text_parts = []
        for page in doc:
            text_parts.append(page.get_text("text"))
        doc.close()
        text = "\n".join(text_parts)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip() or None
    except Exception as e:
        log.warning("PDF extract failed: %s", e)
        return None


def fetch_and_extract(
    record: dict,
    *,
    rate_limit_secs: float = 1.0,
    require_permissive: bool = True,
    pdf_cache_dir: Path | None = None,
) -> dict:
    """End-to-end: resolve PDF URL → fetch → extract → return dict.

    Returns dict with: ok, reason, text, text_chars, pdf_url, bytes,
    sha256.
    """
    if require_permissive and not is_permissive_license(
        record.get("license"), record.get("source"),
    ):
        return {"ok": False, "reason": "non_permissive_license"}

    pdf_url = resolve_pdf_url(record)
    if not pdf_url:
        return {"ok": False, "reason": "no_resolvable_pdf_url"}

    time.sleep(rate_limit_secs)
    data = fetch_bytes(pdf_url)
    if not data:
        return {"ok": False, "reason": "fetch_failed", "pdf_url": pdf_url}

    if not data.startswith(b"%PDF"):
        return {
            "ok": False, "reason": "not_a_pdf",
            "pdf_url": pdf_url, "bytes": len(data),
        }

    text = extract_text_from_pdf(data)
    if not text:
        return {
            "ok": False, "reason": "extract_failed",
            "pdf_url": pdf_url, "bytes": len(data),
        }

    sha256 = hashlib.sha256(data).hexdigest()
    if pdf_cache_dir:
        pdf_cache_dir.mkdir(parents=True, exist_ok=True)
        out = pdf_cache_dir / f"{sha256[:2]}" / f"{sha256}.pdf"
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            out.write_bytes(data)
        except Exception as e:
            log.debug("cache write failed: %s", e)

    return {
        "ok": True,
        "text": text,
        "text_chars": len(text),
        "pdf_url": pdf_url,
        "bytes": len(data),
        "sha256": sha256,
    }

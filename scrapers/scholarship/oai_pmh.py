#!/usr/bin/env python3
"""Generic OAI-PMH harvester for Swiss OA legal scholarship sources.

Speaks vanilla OAI-PMH 2.0 (`oai_dc` metadata format). Streams records into
a JSONL file at output/legal_scholarship/<source>.jsonl.

Reused by every source that exposes OAI-PMH:
  - sui-generis.ch (OJS, set=suigeneris)
  - University IRs (ZORA, BORIS, SERVAL, UNIGE, edoc.unibas, Alexandria SG,
    FOLIA, LIBRA, e-Helvetica, …) with source-specific set filters
  - Other OJS-based Swiss law journals (LeGes if exposed)

Each record is normalized to a JSON dict with the canonical fields the
build_legal_scholarship.py builder expects.

Usage:
    python -m scrapers.scholarship.oai_pmh \
        --source sui_generis \
        --base-url https://sui-generis.ch/oai \
        --set suigeneris
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from xml.etree import ElementTree as ET

log = logging.getLogger("scholarship.oai_pmh")

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO_ROOT / "output" / "legal_scholarship"

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DC_NS = "http://purl.org/dc/elements/1.1/"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"

_DOI_RE = re.compile(r"^(?:info:doi/|doi:|https?://(?:dx\.)?doi\.org/)(.+)$", re.I)
_ISSN_RE = re.compile(r"^urn:issn:(.+)$", re.I)
_URN_NBN_RE = re.compile(r"^urn:nbn:.+$", re.I)
_HTTP_RE = re.compile(r"^https?://", re.I)

# Strip XML 1.0-invalid control characters (everything 0x00-0x1F except
# tab/LF/CR). Some OAI providers (e.g. ex-ante.ch) emit \x17 in
# dc:description, which makes the response not well-formed.
_STRIP_INVALID_XML = re.compile(
    rb"[\x00-\x08\x0b\x0c\x0e-\x1f]",
)

# Map dc:language values onto ISO-639-1 short codes used elsewhere in the repo.
_LANG_MAP = {
    "deu": "de", "ger": "de", "de": "de", "de-DE": "de", "de_DE": "de",
    "fra": "fr", "fre": "fr", "fr": "fr", "fr-FR": "fr", "fr_FR": "fr",
    "ita": "it", "it": "it", "it-IT": "it",
    "eng": "en", "en": "en", "en-US": "en", "en-GB": "en",
    "roh": "rm", "rm": "rm",
}


def _normalize_lang(raw: str | None) -> str | None:
    if not raw:
        return None
    return _LANG_MAP.get(raw.strip(), raw.strip()[:2].lower() or None)


def _normalize_license(raw_list: list[str]) -> tuple[str | None, str | None]:
    """Return (license-code, license-url) from a list of dc:rights values.

    Strategy: ALL dc:rights values are scanned FIRST for a CC license URL.
    If one is found anywhere in the list, it wins regardless of position.
    Only when no CC URL is present do we fall back to the first non-empty
    free-form value (typically a copyright statement).

    Why: many OJS sources (ex-ante.ch, OpenLegalCommentary, …) emit two
    dc:rights — the copyright statement first, the CC URL second. Earlier
    logic took the first value it saw and stored "Copyright (c) 2024 …"
    as the license; we'd lose the CC code. Now CC wins.
    """
    cc_re = re.compile(
        r"creativecommons\.org/licenses/([a-z\-]+)/(\d+\.\d+)", re.I,
    )
    cleaned = [(r or "").strip() for r in raw_list if (r or "").strip()]
    # Pass 1: prefer any CC URL match
    for r in cleaned:
        m = cc_re.search(r)
        if m:
            kind = m.group(1).upper()
            ver = m.group(2)
            code = f"CC-{kind}-{ver}"
            url = r if _HTTP_RE.match(r) else None
            if not url:
                # Reconstruct canonical CC URL when only the path is given
                url = f"https://creativecommons.org/licenses/{m.group(1).lower()}/{ver}/"
            return code, url
    # Pass 2: fall back to first non-empty free-form value
    for r in cleaned:
        return r[:120], None
    return None, None


def _split_identifiers(ids: list[str]) -> dict:
    """Pick out DOI, ISSN, urn:nbn, and canonical URL from a list of dc:identifier."""
    out = {"doi": None, "issn": None, "urn": None, "url": None}
    for ident in ids:
        ident = (ident or "").strip()
        if not ident:
            continue
        m = _DOI_RE.match(ident)
        if m and not out["doi"]:
            out["doi"] = m.group(1).strip()
            continue
        m = _ISSN_RE.match(ident)
        if m and not out["issn"]:
            out["issn"] = m.group(1).strip()
            continue
        if _URN_NBN_RE.match(ident) and not out["urn"]:
            out["urn"] = ident
            continue
        if _HTTP_RE.match(ident) and not out["url"]:
            out["url"] = ident
    return out


def _parse_year(date_raw: str | None) -> int | None:
    if not date_raw:
        return None
    m = re.match(r"(\d{4})", date_raw)
    return int(m.group(1)) if m else None


def _record_to_dict(rec: ET.Element, source: str) -> dict | None:
    """Turn a single OAI-PMH <record> into our unified publication dict.

    Returns None for deleted records (status="deleted").
    """
    header = rec.find(f"{{{OAI_NS}}}header")
    if header is None:
        return None
    if (header.get("status") or "").lower() == "deleted":
        return None
    ident = header.findtext(f"{{{OAI_NS}}}identifier", default="").strip()
    datestamp = header.findtext(f"{{{OAI_NS}}}datestamp", default="").strip()
    setspecs = [
        e.text.strip() for e in header.findall(f"{{{OAI_NS}}}setSpec")
        if e.text and e.text.strip()
    ]

    md = rec.find(f"{{{OAI_NS}}}metadata")
    if md is None:
        return None
    dc = md.find(f"{{{OAI_DC_NS}}}dc")
    if dc is None:
        return None

    def _all(tag: str) -> list[str]:
        return [
            (e.text or "").strip()
            for e in dc.findall(f"{{{DC_NS}}}{tag}")
            if (e.text or "").strip()
        ]

    titles = _all("title")
    creators = _all("creator")
    descriptions = _all("description")
    publishers = _all("publisher")
    dates = _all("date")
    types = _all("type")
    identifiers = _all("identifier")
    sources = _all("source")
    languages = _all("language")
    rights = _all("rights")
    subjects = _all("subject")
    contributors = _all("contributor")
    formats = _all("format")
    relations = _all("relation")

    if not titles:
        return None

    idinfo = _split_identifiers(identifiers)
    lic_code, lic_url = _normalize_license(rights)

    # pub_type: derive from dc:type values
    pub_type = "article"
    type_lower = " ".join(t.lower() for t in types)
    if "doctoralthesis" in type_lower or "dissertation" in type_lower:
        pub_type = "dissertation"
    elif "masterthesis" in type_lower:
        pub_type = "master_thesis"
    elif "bachelorthesis" in type_lower:
        pub_type = "bachelor_thesis"
    elif "book" in type_lower:
        pub_type = "book"
    elif "bookpart" in type_lower or "chapter" in type_lower:
        pub_type = "chapter"
    elif "workingpaper" in type_lower or "preprint" in type_lower:
        pub_type = "working_paper"
    elif "report" in type_lower:
        pub_type = "report"

    return {
        "source": source,
        "source_record_id": ident,
        "datestamp": datestamp,
        "set_specs": setspecs,
        "pub_type": pub_type,
        "title": titles[0],
        "title_alt": titles[1:] or None,
        "authors": creators,
        "contributors": contributors,
        "abstract": descriptions[0] if descriptions else None,
        "abstract_alt": descriptions[1:] or None,
        "publisher": publishers[0] if publishers else None,
        "publication_date": dates[0] if dates else None,
        "year": _parse_year(dates[0] if dates else None),
        "types_raw": types,
        "doi": idinfo["doi"],
        "issn": idinfo["issn"],
        "urn": idinfo["urn"],
        "url": idinfo["url"],
        "all_identifiers": identifiers,
        "sources_raw": sources,
        "language": _normalize_lang(languages[0] if languages else None),
        "languages_raw": languages,
        "license": lic_code,
        "license_url": lic_url,
        "rights_raw": rights,
        "subjects": subjects,
        "formats": formats,
        "relations": relations,
    }


def _record_matches_subject(record_dict: dict, subject_keywords: list[str]) -> bool:
    """Return True if any of the record's dc:subject values contain any
    keyword (case-insensitive).

    Used to post-hoc filter big multi-faculty IRs to law content only.
    Tests against:
      - dc:subject strings (typically Dewey codes, keywords, MeSH terms)
      - record types (for DDC-style 'info:eu-repo/classification/ddc/340')
      - title (some IRs put DDC code in subject as plain '340')
    """
    if not subject_keywords:
        return True
    haystack = " ".join([
        " | ".join(record_dict.get("subjects") or []),
        " | ".join(record_dict.get("types_raw") or []),
        record_dict.get("title") or "",
        " | ".join(record_dict.get("languages_raw") or []),
    ]).lower()
    return any(k.lower() in haystack for k in subject_keywords)


def harvest(
    base_url: str,
    source: str,
    *,
    set_spec: str | None = None,
    metadata_prefix: str = "oai_dc",
    from_date: str | None = None,
    until_date: str | None = None,
    output_dir: Path = DEFAULT_OUT,
    rate_limit: float = 1.0,
    max_records: int | None = None,
    subject_filter: list[str] | None = None,
    fetch_timeout: int = 180,
    # If set, applied to records where dc:rights does NOT contain a CC URL
    # (i.e. the per-record license parse falls back to a copyright string).
    # Used for sources whose editorial license is documented externally
    # (e.g. ex-ante.ch publishes CC-BY-NC-ND on the journal homepage but
    # only emits a per-author copyright line in OAI dc:rights).
    license_override: tuple[str, str] | None = None,
    user_agent: str = "OpenCaseLaw-scholarship/0.1 (+https://opencaselaw.ch)",
) -> dict:
    """Stream OAI-PMH ListRecords into a JSONL file.

    Returns a summary dict.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{source}.jsonl"

    # Initial request parameters; subsequent requests use resumptionToken only.
    params: dict[str, str] = {
        "verb": "ListRecords",
        "metadataPrefix": metadata_prefix,
    }
    if set_spec:
        params["set"] = set_spec
    if from_date:
        params["from"] = from_date
    if until_date:
        params["until"] = until_date

    total = 0
    deleted = 0
    pages = 0
    token: str | None = None
    started = time.time()

    with out_path.open("w", encoding="utf-8") as fh:
        while True:
            pages += 1
            if token:
                qs = urllib.parse.urlencode({"verb": "ListRecords", "resumptionToken": token})
            else:
                qs = urllib.parse.urlencode(params)
            url = f"{base_url}?{qs}"
            try:
                req = urllib.request.Request(
                    url, headers={"User-Agent": user_agent, "Accept": "application/xml"},
                )
                with urllib.request.urlopen(req, timeout=fetch_timeout) as r:
                    raw = r.read()
            except Exception as e:
                log.error("OAI fetch failed (%s, page %d): %s", source, pages, e)
                break

            try:
                root = ET.fromstring(raw)
            except ET.ParseError as e:
                # Some IRs (e.g. ex-ante.ch as of 2026-05-27) emit control
                # characters in dc:description that make the document not
                # well-formed under XML 1.0. Strip XML-invalid control
                # chars and retry once before giving up on the page.
                cleaned = _STRIP_INVALID_XML.sub(b"", raw)
                try:
                    root = ET.fromstring(cleaned)
                    log.warning(
                        "OAI XML had invalid control chars (%s, page %d) — "
                        "stripped and retried",
                        source, pages,
                    )
                except ET.ParseError as e2:
                    log.error("OAI XML parse failed (%s, page %d): %s",
                              source, pages, e2)
                    break

            # OAI error?
            err = root.find(f"{{{OAI_NS}}}error")
            if err is not None:
                code = err.get("code", "?")
                msg = (err.text or "").strip()
                log.error("OAI error (%s, page %d) code=%s msg=%s",
                          source, pages, code, msg)
                if code == "noRecordsMatch":
                    # Treat as empty harvest, not a failure
                    pass
                break

            list_recs = root.find(f"{{{OAI_NS}}}ListRecords")
            if list_recs is None:
                log.warning("OAI: no ListRecords element (%s, page %d)", source, pages)
                break

            page_real = 0
            page_filtered = 0
            for rec in list_recs.findall(f"{{{OAI_NS}}}record"):
                # Surface deleted-record stats without writing them out
                hdr = rec.find(f"{{{OAI_NS}}}header")
                if hdr is not None and (hdr.get("status") or "").lower() == "deleted":
                    deleted += 1
                    continue
                d = _record_to_dict(rec, source)
                if d is None:
                    continue
                if subject_filter and not _record_matches_subject(d, subject_filter):
                    page_filtered += 1
                    continue
                # Apply source-level license override when dc:rights didn't
                # surface a CC license. Sources where the editorial license
                # is documented externally use this to assert the correct
                # per-record license.
                if license_override and (
                    not d.get("license")
                    or not d["license"].upper().startswith("CC-")
                ):
                    d["license"] = license_override[0]
                    d["license_url"] = license_override[1]
                fh.write(json.dumps(d, ensure_ascii=False) + "\n")
                total += 1
                page_real += 1
                if max_records and total >= max_records:
                    break

            log.info(
                "%s page %d: %d records (running total: %d, deleted: %d, subject-filtered: %d)",
                source, pages, page_real, total, deleted, page_filtered,
            )

            if max_records and total >= max_records:
                break

            rt = list_recs.find(f"{{{OAI_NS}}}resumptionToken")
            if rt is None or not (rt.text or "").strip():
                break
            token = rt.text.strip()
            time.sleep(rate_limit)

    elapsed = time.time() - started
    summary = {
        "source": source,
        "base_url": base_url,
        "set": set_spec,
        "metadata_prefix": metadata_prefix,
        "pages": pages,
        "total_records": total,
        "deleted_records": deleted,
        "elapsed_seconds": round(elapsed, 1),
        "output_path": str(out_path),
    }
    log.info("Done %s: %s", source, summary)
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--source", required=True,
                   help="Short source name (used as JSONL filename)")
    p.add_argument("--base-url", required=True, help="OAI-PMH base URL")
    p.add_argument("--set", dest="set_spec", default=None, help="OAI setSpec to filter")
    p.add_argument("--metadata-prefix", default="oai_dc")
    p.add_argument("--from-date", default=None, help="OAI 'from' (YYYY-MM-DD)")
    p.add_argument("--until-date", default=None, help="OAI 'until' (YYYY-MM-DD)")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUT)
    p.add_argument("--rate-limit", type=float, default=1.0,
                   help="Sleep between resumption pages (s)")
    p.add_argument("--max-records", type=int, default=None,
                   help="Stop after N records (for smoke tests)")
    args = p.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    summary = harvest(
        args.base_url,
        args.source,
        set_spec=args.set_spec,
        metadata_prefix=args.metadata_prefix,
        from_date=args.from_date,
        until_date=args.until_date,
        output_dir=args.output_dir,
        rate_limit=args.rate_limit,
        max_records=args.max_records,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

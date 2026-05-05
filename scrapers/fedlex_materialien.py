"""Fedlex Materialien discovery — SPARQL queries for Acts (Botschaft enactments)
and Consultations (Vernehmlassungen).

Phase 1 of the Materialien build (per memory note ``materialien_build_commitment``).
Extends the statute scraper without touching it — separate module so the
working statute pipeline stays unaffected during iteration.

Fedlex publishes Materialien across multiple ELI namespaces, all queryable
via the same SPARQL endpoint we already use for statutes:

    eli/oc/<orig_AS_ref>           — Act (Botschaft enactment publication)
    eli/dl/proj/<year>/<n>/cons_N  — Consultation (Vernehmlassung)
    eli/pudocc/...                 — PublicationProcess metadata

This module provides the discovery layer:

    discover_acts(sr_numbers=None)         → iterator of Act metadata dicts
    discover_consultations(sr_numbers=None)→ iterator of Consultation dicts
    fetch_manifestations(eli_uri)          → multilingual PDF/HTML/XML URLs

Output is JSONL (raw scrape store, append-only — same convention as the
``decisions/*.jsonl`` shards). The downstream build script
(``search_stack/build_materialien_db.py``, to be written in Phase 2)
ingests these JSONLs into the ``materialien_doc`` table and runs the
Akoma Ntoso / pdfplumber / FTS5 pipeline.

CLI:

    python -m scrapers.fedlex_materialien discover-acts \\
        --sr 220,210,311.0 --output output/raw/materialien/acts.jsonl

    python -m scrapers.fedlex_materialien discover-consultations \\
        --output output/raw/materialien/consultations.jsonl

    python -m scrapers.fedlex_materialien manifestations \\
        --eli https://fedlex.data.admin.ch/eli/oc/27/317_321_377
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterator

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fedlex_materialien")

SPARQL_ENDPOINT = "https://fedlex.data.admin.ch/sparqlendpoint"

# JOLUX is the Fedlex ontology (also used by Luxembourg and the EU
# Publications Office). Predicates we need:
JOLUX = "http://data.legilux.public.lu/resource/ontology/jolux#"
SKOS = "http://www.w3.org/2004/02/skos/core#"

# Language URIs as used throughout Fedlex
LANG_URIS = {
    "de": "http://publications.europa.eu/resource/authority/language/DEU",
    "fr": "http://publications.europa.eu/resource/authority/language/FRA",
    "it": "http://publications.europa.eu/resource/authority/language/ITA",
}

REQUEST_DELAY = 0.3
DEFAULT_TIMEOUT = 120

session = requests.Session()
session.headers.update({
    "User-Agent": "OpenCaseLaw/1.0 (materialien scraper; +https://opencaselaw.ch)",
    "Accept": "application/sparql-results+json",
})


# ── Data classes ─────────────────────────────────────────────────────


@dataclass
class ActRealization:
    """One per-language Expression of an Act. Holds the per-language title
    and the memorial reference (the AS / BBl publication coordinates)."""
    language: str                       # 'de' | 'fr' | 'it'
    title: str | None = None            # e.g. "Bundesgesetz vom 30. März 1911 …"
    title_alternative: str | None = None  # e.g. "OR" / "CO"
    memorial_name: str | None = None    # e.g. "AS"
    memorial_year: str | None = None    # e.g. "27"
    memorial_number: str | None = None  # e.g. ""
    memorial_page: str | None = None    # e.g. "317"
    identifier: str | None = None
    pdf_url: str | None = None          # constructed from ELI URI


@dataclass
class ActRecord:
    """An Act = Botschaft-enactment Official Compilation entry.

    Maps to ``materialien_doc`` rows with kind='act'. Multiple Acts can
    target the same SR number (one per amendment cycle through history) —
    v0.2 currently only pulls the original ``basicAct``; per-amendment Acts
    will arrive in v0.3 via the ``amendment_refs`` table join.
    """
    eli_uri: str
    sr_number: str | None = None        # joined via the work URI
    work_uri: str | None = None         # eli/cc/... the consolidated law
    date_document: str | None = None
    date_entry_in_force: str | None = None
    publication_date: str | None = None
    process_type: str | None = None
    type_document: str | None = None
    legal_resource_genre: str | None = None
    is_part_of: str | None = None       # eli/collection/oc/<vol>/<...>
    realizations: list[ActRealization] = field(default_factory=list)


@dataclass
class ConsultationRecord:
    """A Consultation = Vernehmlassung entry on a draft law."""
    eli_uri: str
    sr_number: str | None = None
    title: str | None = None
    description: str | None = None
    status: str | None = None
    consultation_id: str | None = None
    impacts_work_uri: str | None = None  # the law it concerns


@dataclass
class Manifestation:
    """One downloadable file for a given expression+format+language."""
    file_url: str
    format: str        # 'pdf' | 'html' | 'xml' (Akoma Ntoso) | 'epub' | etc.
    language: str      # 'de' | 'fr' | 'it'


# ── SPARQL execution ────────────────────────────────────────────────


def sparql_query(query: str, timeout: int = DEFAULT_TIMEOUT) -> list[dict]:
    """POST to the Fedlex SPARQL endpoint, return result bindings.

    POST (not GET) because the endpoint returns 400 for GETs longer than
    ~4-6 kB — same defensive pattern used in scrapers/fedlex.py.
    """
    resp = session.post(
        SPARQL_ENDPOINT,
        data={"query": query},
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()["results"]["bindings"]


def _val(binding: dict, key: str) -> str | None:
    """Safely extract a SPARQL binding value."""
    cell = binding.get(key)
    return cell["value"] if cell else None


# ── Acts discovery (eli/oc/... namespace) ────────────────────────────


def _canonical_act_pdf_url(act_eli_uri: str, language: str) -> str:
    """Construct the canonical Fedlex PDF URL for an Act in a given language.

    Fedlex serves Acts at a content-negotiation URL pattern; the archival
    Type-A PDF (signed, long-term-preservation) is what citizens get from
    the federal-law search. We don't ask SPARQL for these — the URL is
    fully derivable from the ELI URI.

    Example:
      eli/oc/27/317_321_377  +  'de'
      → https://www.fedlex.admin.ch/eli/oc/27/317_321_377/de/pdf-a
    """
    # Strip any language suffix the caller may have included
    base = act_eli_uri.rstrip("/")
    if base.endswith(f"/{language}"):
        base = base[: -len(f"/{language}")]
    base_user = base.replace(
        "fedlex.data.admin.ch", "www.fedlex.admin.ch", 1
    )
    return f"{base_user}/{language}/pdf-a"


def discover_acts(sr_numbers: list[str] | None = None) -> Iterator[ActRecord]:
    """Yield ActRecord entries for every Botschaft-enactment Act
    referenced by the given SR numbers (or ALL classified compilation
    laws if sr_numbers is None).

    Acts are reached via the basicAct relation from a ConsolidationAbstract:

        ?work jolux:basicAct ?act .

    Per-amendment Acts (each revision had its own message) are NOT yet
    pulled here — the basicAct only points to the ORIGINAL act. v0.3 will
    add amendment-level Acts via the existing ``materialien.amendment_refs``
    table (which already has 83,958 BBl/AS pointers per article).

    Implementation note: per-language metadata (title, memorial coords)
    lives on the Act's Expressions, not the Act itself. Per the Fedlex
    JOLUX schema, titles are language-dependent so they hang off the
    Expression resource. We aggregate them under one ActRecord with a
    list of ActRealization rows so the JSONL output is one-Act-per-line.
    """
    sr_filter = ""
    if sr_numbers:
        sr_list = ", ".join(f'"{sr}"' for sr in sr_numbers)
        sr_filter = f"FILTER(?srNumber IN ({sr_list}))"

    # Single query that joins Acts with ALL their Expression metadata so
    # we don't have to do N+1 round-trips. GROUP_CONCAT collapses the
    # per-language rows into one Act row; we re-split client-side.
    query = f"""
    PREFIX jolux: <{JOLUX}>
    SELECT
        ?work ?srNumber ?act ?dateDoc ?dateEif
        ?publicationDate ?processType ?typeDoc ?genre ?isPartOf
        ?expr ?lang ?title ?titleAlt ?identifier
        ?memName ?memYear ?memNumber ?memPage
    WHERE {{
      ?work a jolux:ConsolidationAbstract .
      ?work jolux:historicalLegalId ?srNumber .
      ?work jolux:basicAct ?act .
      {sr_filter}
      OPTIONAL {{ ?act jolux:dateDocument ?dateDoc }}
      OPTIONAL {{ ?act jolux:dateEntryInForce ?dateEif }}
      OPTIONAL {{ ?act jolux:publicationDate ?publicationDate }}
      OPTIONAL {{ ?act jolux:processType ?processType }}
      OPTIONAL {{ ?act jolux:typeDocument ?typeDoc }}
      OPTIONAL {{ ?act jolux:legalResourceGenre ?genre }}
      OPTIONAL {{ ?act jolux:isPartOf ?isPartOf }}
      OPTIONAL {{
        ?act jolux:isRealizedBy ?expr .
        OPTIONAL {{ ?expr jolux:language ?lang }}
        OPTIONAL {{ ?expr jolux:title ?title }}
        OPTIONAL {{ ?expr jolux:titleAlternative ?titleAlt }}
        OPTIONAL {{ ?expr jolux:identifier ?identifier }}
        OPTIONAL {{ ?expr jolux:memorialName ?memName }}
        OPTIONAL {{ ?expr jolux:memorialYear ?memYear }}
        OPTIONAL {{ ?expr jolux:memorialNumber ?memNumber }}
        OPTIONAL {{ ?expr jolux:memorialPage ?memPage }}
      }}
    }}
    """
    rows = sparql_query(query, timeout=600)

    # Group by Act ELI URI — multiple rows per Act (one per Expression).
    by_act: dict[str, ActRecord] = {}
    for r in rows:
        act_uri = _val(r, "act")
        if not act_uri:
            continue
        if act_uri not in by_act:
            by_act[act_uri] = ActRecord(
                eli_uri=act_uri,
                sr_number=_val(r, "srNumber"),
                work_uri=_val(r, "work"),
                date_document=_val(r, "dateDoc"),
                date_entry_in_force=_val(r, "dateEif"),
                publication_date=_val(r, "publicationDate"),
                process_type=_val(r, "processType"),
                type_document=_val(r, "typeDoc"),
                legal_resource_genre=_val(r, "genre"),
                is_part_of=_val(r, "isPartOf"),
            )
        # Collect per-Expression realization
        lang_uri = _val(r, "lang") or ""
        lang_code = next(
            (k for k, v in LANG_URIS.items() if v == lang_uri),
            lang_uri.rsplit("/", 1)[-1].lower()[:3] if lang_uri else "",
        )
        if not lang_code:
            continue
        # Dedup on (act_uri, language) — same expression may appear once
        existing_langs = {rz.language for rz in by_act[act_uri].realizations}
        if lang_code in existing_langs:
            continue
        by_act[act_uri].realizations.append(ActRealization(
            language=lang_code,
            title=_val(r, "title"),
            title_alternative=_val(r, "titleAlt"),
            identifier=_val(r, "identifier"),
            memorial_name=_val(r, "memName"),
            memorial_year=_val(r, "memYear"),
            memorial_number=_val(r, "memNumber"),
            memorial_page=_val(r, "memPage"),
            pdf_url=_canonical_act_pdf_url(act_uri, lang_code),
        ))

    log.info(
        "Discovered %d Acts via SPARQL (%d Expression rows aggregated)",
        len(by_act), len(rows),
    )
    yield from by_act.values()


# ── Consultations discovery (eli/dl/proj/... namespace) ─────────────


def discover_consultations(sr_numbers: list[str] | None = None) -> Iterator[ConsultationRecord]:
    """Yield ConsultationRecord entries for Vernehmlassungen.

    Consultations are linked to the law they concern via:

        ?cons jolux:foreseenImpactToLegalResource ?work .

    Where ?work is an eli/cc/... ConsolidationAbstract. We optionally
    join through to its SR number for filtering.
    """
    sr_filter = ""
    if sr_numbers:
        sr_list = ", ".join(f'"{sr}"' for sr in sr_numbers)
        sr_filter = f"""
        ?work jolux:historicalLegalId ?srNumber .
        FILTER(?srNumber IN ({sr_list}))
        """
    else:
        # Still want srNumber when present, but don't filter
        sr_filter = "OPTIONAL { ?work jolux:historicalLegalId ?srNumber }"

    query = f"""
    PREFIX jolux: <{JOLUX}>
    SELECT DISTINCT
        ?cons ?title ?description ?status ?eventId ?work ?srNumber
    WHERE {{
      ?cons a jolux:Consultation .
      ?cons jolux:foreseenImpactToLegalResource ?work .
      {sr_filter}
      OPTIONAL {{ ?cons jolux:eventTitle ?title }}
      OPTIONAL {{ ?cons jolux:eventDescription ?description }}
      OPTIONAL {{ ?cons jolux:consultationStatus ?status }}
      OPTIONAL {{ ?cons jolux:eventId ?eventId }}
    }}
    """
    rows = sparql_query(query, timeout=600)
    log.info("Discovered %d Consultations via SPARQL", len(rows))
    for r in rows:
        eli_uri = _val(r, "cons")
        if not eli_uri:
            continue
        yield ConsultationRecord(
            eli_uri=eli_uri,
            sr_number=_val(r, "srNumber"),
            impacts_work_uri=_val(r, "work"),
            title=_val(r, "title"),
            description=_val(r, "description"),
            status=_val(r, "status"),
            consultation_id=_val(r, "eventId"),
        )


# ── Manifestations (downloadable files) ─────────────────────────────


def fetch_manifestations(eli_uri: str) -> list[Manifestation]:
    """For one Act / Consultation / any ELI URI, return the multilingual
    file URLs across all formats Fedlex publishes.

    Fedlex's FRBR layering: Work → Expression (per language) →
    Manifestation (per format) → Item (the actual file URL).
    """
    query = f"""
    PREFIX jolux: <{JOLUX}>
    SELECT DISTINCT ?fmt ?lang ?file WHERE {{
      <{eli_uri}> jolux:isRealizedBy ?expr .
      ?expr jolux:isEmbodiedBy ?manif .
      OPTIONAL {{ ?expr jolux:language ?lang . }}
      OPTIONAL {{ ?manif jolux:userFormat ?fmt . }}
      OPTIONAL {{ ?manif jolux:isExemplifiedBy ?file . }}
    }}
    """
    rows = sparql_query(query, timeout=60)
    out: list[Manifestation] = []
    for r in rows:
        file_url = _val(r, "file")
        if not file_url:
            continue
        fmt_uri = _val(r, "fmt") or ""
        lang_uri = _val(r, "lang") or ""
        # Translate URIs to short codes
        fmt = fmt_uri.rsplit("/", 1)[-1].lower() if fmt_uri else "unknown"
        lang_code = next(
            (k for k, v in LANG_URIS.items() if v == lang_uri),
            lang_uri.rsplit("/", 1)[-1].lower()[:3],
        )
        out.append(Manifestation(file_url=file_url, format=fmt, language=lang_code))
    return out


# ── JSONL output ─────────────────────────────────────────────────────


def write_jsonl(records: Iterator, path: Path) -> int:
    """Write dataclass records as one-JSON-per-line. Returns count.

    ActRecord.realizations is a list of ActRealization dataclasses;
    asdict() recurses into them so the JSONL line carries full per-
    language metadata (title, memorial coords, pdf_url) inline.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(asdict(rec), ensure_ascii=False))
            f.write("\n")
            n += 1
    return n


# ── CLI ──────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fedlex Materialien discovery (Phase 1)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_acts = sub.add_parser("discover-acts", help="SPARQL → Act metadata JSONL")
    p_acts.add_argument(
        "--sr", default=None,
        help="Comma-separated SR numbers (e.g. 220,210,311.0). Default: all laws.",
    )
    p_acts.add_argument(
        "--output", default="output/raw/materialien/acts.jsonl",
        help="Output JSONL path",
    )

    p_cons = sub.add_parser(
        "discover-consultations", help="SPARQL → Consultation metadata JSONL",
    )
    p_cons.add_argument("--sr", default=None)
    p_cons.add_argument(
        "--output", default="output/raw/materialien/consultations.jsonl",
    )

    p_man = sub.add_parser(
        "manifestations", help="Print downloadable file URLs for one ELI URI",
    )
    p_man.add_argument("--eli", required=True, help="Fully-qualified ELI URI")

    args = parser.parse_args(argv)

    if args.cmd == "discover-acts":
        sr_list = [s.strip() for s in args.sr.split(",")] if args.sr else None
        n = write_jsonl(discover_acts(sr_list), Path(args.output))
        log.info("Wrote %d Acts → %s", n, args.output)
        return 0

    if args.cmd == "discover-consultations":
        sr_list = [s.strip() for s in args.sr.split(",")] if args.sr else None
        n = write_jsonl(discover_consultations(sr_list), Path(args.output))
        log.info("Wrote %d Consultations → %s", n, args.output)
        return 0

    if args.cmd == "manifestations":
        manifs = fetch_manifestations(args.eli)
        for m in manifs:
            print(f"  [{m.language} / {m.format:6s}] {m.file_url}")
        log.info("Found %d manifestations for %s", len(manifs), args.eli)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())

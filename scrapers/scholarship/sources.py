"""Registry of Swiss OA legal scholarship sources.

Each entry describes how to harvest one source via OAI-PMH (or, occasionally,
a custom scraper module). Add new sources here; the orchestrator
`scrapers/scholarship/harvest_all.py` walks this list.

`active=False` entries are scaffolded but not yet wired (e.g. require set-spec
discovery or a custom adapter).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class ScholarshipSource:
    key: str                       # short slug; JSONL filename
    name: str                      # human-readable
    kind: str                      # 'oai_pmh' | 'custom'
    base_url: Optional[str] = None
    set_spec: Optional[str] = None
    metadata_prefix: str = "oai_dc"
    rate_limit: float = 1.0
    # Default-license metadata applied when individual records don't carry
    # license info. The CC license URL we display in attribution.
    license_default: Optional[str] = None
    license_url_default: Optional[str] = None
    # Free-form attribution text shown alongside every result served from
    # this source — required by CC-BY / CC-BY-SA / CC-BY-NC-SA / "free to
    # read" upstream terms.
    attribution: str = ""
    # User-facing homepage (linked from /docs/scholarship-licenses.html)
    homepage: Optional[str] = None
    notes: str = ""
    active: bool = True
    # For 'custom' kind: module path (e.g. "scrapers.scholarship.leges")
    custom_module: Optional[str] = None


SOURCES: list[ScholarshipSource] = [
    # ── Authored OA reference works ─────────────────────────────────────
    ScholarshipSource(
        key="thegoodboard",
        name="The Good Board — Swiss Corporate Governance Reference",
        kind="custom",
        custom_module="scrapers.scholarship.thegoodboard",
        license_default="OA-author-permitted-reuse",
        license_url_default="https://thegoodboard.ch/llms.txt",
        attribution=(
            "© Jonas Hertner. Published at thegoodboard.ch. Author "
            "explicitly permits ingestion by training corpora and "
            "retrieval systems; substantial reproduction must attribute "
            "the author and link to the original."
        ),
        homepage="https://thegoodboard.ch/",
        notes="Authored reference work on Swiss corporate governance — "
              "reference articles + commentary + agenda briefings + glossary. "
              "Sitemap-driven scrape (no OAI-PMH).",
        active=True,
    ),

    # ── Standalone OA law journals (small but high-quality) ─────────────
    ScholarshipSource(
        key="sui_generis",
        name="sui generis (OJS, hosted on hope.uzh.ch)",
        kind="oai_pmh",
        base_url="https://sui-generis.ch/oai",
        set_spec="suigeneris",
        license_default="CC-BY-SA-4.0",
        license_url_default="https://creativecommons.org/licenses/by-sa/4.0/",
        attribution=(
            "© respective authors. Published in sui generis "
            "(sui-generis.ch), CC-BY-SA-4.0. A small minority of articles "
            "carry CC-BY-NC-SA-4.0 — check the per-record license field."
        ),
        homepage="https://sui-generis.ch/",
        rate_limit=1.0,
        notes="Peer-reviewed OA Swiss law journal since 2014. Set 'suigeneris' "
              "groups all sub-sets (ART, INFO, MIGRAT, OEFF, PRIVAT, STRAF).",
        active=True,
    ),
    ScholarshipSource(
        key="leoh",
        name="LEOH — Journal of Animal Law, Ethics and One Health",
        kind="oai_pmh",
        base_url="https://leoh.ch/oai",
        set_spec="leoh",
        license_default="CC-BY-ND-4.0",
        license_url_default="https://creativecommons.org/licenses/by-nd/4.0/",
        attribution=(
            "© respective authors. Published in LEOH — Journal of Animal Law, "
            "Ethics and One Health (leoh.ch), peer-reviewed OA. Per-record "
            "license is typically CC-BY-ND-4.0 (no derivative works); some "
            "articles may carry CC-BY-4.0 — always check the per-record "
            "license field before deriving or remixing."
        ),
        homepage="https://leoh.ch/",
        notes="OJS, hosted on hope.uzh.ch like sui-generis. Sub-sets cover "
              "articles, legal education, jurisprudence, legislation, book reviews.",
        active=True,
    ),
    ScholarshipSource(
        key="leges",
        name="LeGes — Gesetzgebung & Evaluation (Bundeskanzlei)",
        kind="custom",
        custom_module="scrapers.scholarship.leges",
        license_default="OA-Swiss-federal",
        license_url_default="https://www.bk.admin.ch/bk/de/home/dokumentation/zeitschrift--leges-.html",
        attribution=(
            "Published by the Federal Chancellery of Switzerland (Bundeskanzlei). "
            "Federal publication: free of copyright (Art. 5 al. 1 lit. a URG) — "
            "may be reproduced without permission."
        ),
        homepage="https://www.bk.admin.ch/bk/de/home/dokumentation/zeitschrift--leges-.html",
        notes="Federal Chancellery's quarterly journal on legislation and "
              "evaluation. Free PDFs on bk.admin.ch/leges. No OAI-PMH; needs "
              "custom HTML+PDF scraper.",
        active=False,
    ),
    ScholarshipSource(
        key="justice",
        name="Justice-Justiz-Giustizia (judges' association)",
        kind="custom",
        custom_module="scrapers.scholarship.justice",
        license_default="OA-no-redistribution",
        attribution=(
            "© respective authors and Schweizerische Vereinigung der "
            "Richterinnen und Richter (SVR-ASM). Reproduction beyond fair use "
            "requires permission from the publisher."
        ),
        homepage="https://richterzeitung.weblaw.ch/",
        notes="OA portion of the Swiss judges' association journal. Custom "
              "HTML scrape from justice-justiz-giustizia.ch / richterzeitung.weblaw.ch.",
        active=False,
    ),

    # ── University institutional repositories filtered to law ───────────
    # All Swiss IRs expose OAI-PMH. Set specs vary; "law"-filter discovery is
    # per-IR (some use faculty codes, some Dewey 340, some custom). Listed
    # here as scaffolding; activate as set-spec is verified.
    ScholarshipSource(
        key="zora_law",
        name="UZH ZORA — law faculty",
        kind="oai_pmh",
        base_url="https://www.zora.uzh.ch/cgi/oai2",
        set_spec=None,   # ZORA sets are per-DDC; needs discovery + multi-set merge
        attribution=(
            "© respective authors. Deposited in the Zurich Open Repository and "
            "Archive (ZORA), University of Zurich. License per record — typically "
            "the author's CC-BY or 'free to read' grant; check each item."
        ),
        homepage="https://www.zora.uzh.ch/",
        notes="University of Zurich's repository. Filter to Faculty of Law "
              "via DDC 340 or organisational set.",
        active=False,
    ),
    ScholarshipSource(
        key="boris_law",
        name="UniBE BORIS — law faculty",
        kind="oai_pmh",
        base_url="https://boris.unibe.ch/cgi/oai2",
        notes="University of Bern's EPrints-based repository.",
        active=False,
    ),
    ScholarshipSource(
        key="serval_law",
        name="UNIL SERVAL — law faculty",
        kind="oai_pmh",
        base_url="https://serval.unil.ch/oai2",
        notes="University of Lausanne. Filter to Faculté de droit.",
        active=False,
    ),
    ScholarshipSource(
        key="unige_law",
        name="UNIGE Archive ouverte — law faculty",
        kind="oai_pmh",
        base_url="https://archive-ouverte.unige.ch/oai2",
        notes="University of Geneva. Filter to Faculté de droit.",
        active=False,
    ),
    ScholarshipSource(
        key="edoc_unibas_law",
        name="UniBas edoc — law faculty",
        kind="oai_pmh",
        base_url="https://edoc.unibas.ch/cgi/oai2",
        notes="University of Basel. EPrints. Filter to Rechtswissenschaft.",
        active=False,
    ),
    ScholarshipSource(
        key="folia_law",
        name="UniFR FOLIA — law faculty",
        kind="oai_pmh",
        base_url="https://folia.unifr.ch/oai2",
        notes="University of Fribourg, bilingual (de/fr). Filter to law.",
        active=False,
    ),
    ScholarshipSource(
        key="alexandria_law",
        name="UniSG Alexandria — law + business law",
        kind="oai_pmh",
        base_url="https://www.alexandria.unisg.ch/cgi/oai2",
        notes="University of St Gallen. Filter to law subject set.",
        active=False,
    ),
    ScholarshipSource(
        key="libra_law",
        name="UniNE LIBRA — law faculty",
        kind="oai_pmh",
        base_url="https://libra.unine.ch/oai2",
        notes="University of Neuchâtel.",
        active=False,
    ),
    ScholarshipSource(
        key="unilu_law",
        name="UniLU — law faculty",
        kind="oai_pmh",
        base_url="https://zenodo.org/oai2d",  # tentative
        notes="University of Lucerne deposits to Zenodo + own portal. "
              "Endpoint TBD.",
        active=False,
    ),

    # ── National library + federal repositories ────────────────────────
    ScholarshipSource(
        key="e_helvetica_law",
        name="e-Helvetica (Swiss National Library)",
        kind="oai_pmh",
        base_url="https://www.e-helvetica.nb.admin.ch/oai/edoc",
        notes="Federal deposits (theses, government reports). Filter to law.",
        active=False,
    ),
    ScholarshipSource(
        key="bj_studien",
        name="BJ (Bundesamt für Justiz) — Berichte & Studien",
        kind="custom",
        custom_module="scrapers.scholarship.bj",
        notes="Federal Office of Justice publishes its commissioned legal "
              "research as PDFs on bj.admin.ch. No OAI-PMH; custom scrape.",
        active=False,
    ),

    # ── Historical legal periodicals via e-periodica.ch (ETH Library) ──
    # e-periodica hosts ~30 Swiss law journals with full-text OCR + PDFs,
    # mostly with moving wall ~5-10 years. Tremendous historical coverage
    # (ZBJV from 1872, ZSR from 1881, etc.).
    ScholarshipSource(
        key="e_periodica_law",
        name="e-periodica law journals (ETH Library)",
        kind="custom",
        custom_module="scrapers.scholarship.e_periodica",
        license_default="ETH-Library-free-to-read",
        notes="ZBJV (1872–), ZSR historical, SJZ historical, RDS, Revue de "
              "droit administratif et de droit fiscal, plus ~25 other "
              "journals. No OAI-PMH on e-periodica; per-issue HTML scrape.",
        active=False,
    ),
]


def active_sources() -> list[ScholarshipSource]:
    return [s for s in SOURCES if s.active]


def by_key(key: str) -> ScholarshipSource | None:
    for s in SOURCES:
        if s.key == key:
            return s
    return None


# ── Re-exported corpora (not in SOURCES, but served as scholarship) ──────
# These come from ok_commentaries.db via the build_legal_scholarship
# re-export step. They aren't OAI-PMH-harvested, but they ARE served from
# the scholarship corpus, so the licensing layer needs to know about them.
_REEXPORTED = {
    "onlinekommentar": {
        "name": "OnlineKommentar.ch",
        "license_default": "CC-BY-4.0",
        "license_url_default": "https://creativecommons.org/licenses/by/4.0/",
        "attribution": (
            "© respective authors. Published on OnlineKommentar.ch under "
            "CC-BY-4.0. Re-use must credit author + journal + license."
        ),
        "homepage": "https://onlinekommentar.ch/",
        "notes": "Scholarly commentary on Swiss federal law. CC-BY-4.0.",
    },
    "openlegalcommentary": {
        "name": "OpenLegalCommentary.ch",
        "license_default": "CC-BY-SA-4.0",
        "license_url_default": "https://creativecommons.org/licenses/by-sa/4.0/",
        "attribution": (
            "© respective authors. Published on OpenLegalCommentary.ch under "
            "CC-BY-SA-4.0. Re-use must credit author + journal + license; "
            "derivatives must be licensed CC-BY-SA-4.0."
        ),
        "homepage": "https://openlegalcommentary.ch/",
        "notes": "OA commentaries on the Swiss Federal Constitution (BV).",
    },
}


def attribution_for_source(source_key: str) -> dict:
    """Return a dict with attribution / license / homepage for a source key.

    Falls back to a generic attribution string for sources without explicit
    metadata so we never serve an unattributed publication. The returned
    dict is safe to dump in MCP responses.
    """
    s = by_key(source_key)
    if s is not None:
        return {
            "source": s.key,
            "name": s.name,
            "license": s.license_default,
            "license_url": s.license_url_default,
            "attribution": s.attribution or (
                f"© respective authors. Indexed from {s.name}. "
                "License per record — check each item."
            ),
            "homepage": s.homepage,
        }
    if source_key in _REEXPORTED:
        r = _REEXPORTED[source_key]
        return {
            "source": source_key,
            "name": r["name"],
            "license": r["license_default"],
            "license_url": r["license_url_default"],
            "attribution": r["attribution"],
            "homepage": r["homepage"],
        }
    return {
        "source": source_key,
        "name": source_key,
        "license": None,
        "license_url": None,
        "attribution": (
            f"© respective authors. Source: {source_key}. License per record."
        ),
        "homepage": None,
    }


def license_usage_hint(license_code: str | None) -> dict:
    """Return machine-readable downstream-use guidance for a license code.

    Surfaced in MCP responses so LLMs / consumers know what they can do
    with the content. Keyed by upstream CC code + our internal labels.
    """
    if not license_code:
        return {
            "license": None,
            "may_attribute": True,
            "may_quote_verbatim": True,
            "may_summarize_or_paraphrase": "unknown",
            "may_redistribute": "unknown",
            "may_use_commercially": "unknown",
            "share_alike_required": False,
            "note": (
                "License not declared by the source. Default to fair-use "
                "quotation with attribution; do not redistribute without "
                "verifying the upstream rights statement."
            ),
        }
    code = license_code.upper()
    base = {
        "license": code,
        "may_attribute": True,
        "may_quote_verbatim": True,
        "may_summarize_or_paraphrase": True,
        "may_redistribute": True,
        "may_use_commercially": True,
        "share_alike_required": False,
        "note": "",
    }
    if code == "CC-BY-4.0":
        base["note"] = (
            "CC-BY-4.0: attribution required (author + license + link). "
            "Derivatives, commercial use, and redistribution are permitted."
        )
        return base
    if code == "CC-BY-SA-4.0":
        base["share_alike_required"] = True
        base["note"] = (
            "CC-BY-SA-4.0: attribution required. Derivatives MUST be "
            "released under CC-BY-SA-4.0 (Share-Alike). Indexing/search "
            "is a §3(b) collection — not a derivative — so the index "
            "itself need not be CC-BY-SA. Quotation downstream is fine."
        )
        return base
    if code == "CC-BY-ND-4.0":
        base["may_summarize_or_paraphrase"] = False
        base["note"] = (
            "CC-BY-ND-4.0: attribution required, NoDerivatives. You may "
            "quote verbatim with attribution; you may NOT publish a "
            "modified, paraphrased, abridged, or transformed version. "
            "LLM-generated summaries that re-publish modified content "
            "violate the ND clause."
        )
        return base
    if code == "CC-BY-NC-4.0":
        base["may_use_commercially"] = False
        base["note"] = (
            "CC-BY-NC-4.0: attribution required; non-commercial use only. "
            "Re-use in a commercial product or service requires separate "
            "permission from the rightsholder."
        )
        return base
    if code == "CC-BY-NC-SA-4.0":
        base["may_use_commercially"] = False
        base["share_alike_required"] = True
        base["note"] = (
            "CC-BY-NC-SA-4.0: attribution required, non-commercial only, "
            "Share-Alike. Derivatives must be CC-BY-NC-SA-4.0. Commercial "
            "use requires separate permission from the rightsholder."
        )
        return base
    if code == "CC-BY-NC-ND-4.0":
        base["may_use_commercially"] = False
        base["may_summarize_or_paraphrase"] = False
        base["note"] = (
            "CC-BY-NC-ND-4.0: attribution required, non-commercial, "
            "NoDerivatives. Verbatim quotation with attribution only; "
            "no modification; no commercial use."
        )
        return base
    if code == "OA-AUTHOR-PERMITTED-REUSE":
        base["note"] = (
            "OA-author-permitted-reuse: author has explicitly invited "
            "training corpora and retrieval systems to ingest the work. "
            "Substantial reproduction must attribute the author and link "
            "to the original."
        )
        return base
    if code == "OA-SWISS-FEDERAL":
        base["note"] = (
            "Federal Swiss publication: no copyright under Art. 5(1)(a) URG. "
            "Reproduction permitted without permission; attribution courteous."
        )
        return base
    if code == "ETH-LIBRARY-FREE-TO-READ":
        base["may_redistribute"] = "see-source"
        base["note"] = (
            "ETH Library 'free to read' grant: full text accessible; "
            "redistribution rights vary by upstream publisher. Check the "
            "original record before redistributing."
        )
        return base
    if code == "OA-NO-REDISTRIBUTION":
        base["may_redistribute"] = False
        base["note"] = (
            "Open access for reading; redistribution beyond fair use "
            "requires permission from the publisher."
        )
        return base
    # Unknown / vendor-specific
    base["note"] = (
        f"License '{license_code}' is not a recognized CC variant. Treat "
        "as 'all rights reserved' for derivative work and redistribution; "
        "fair-use quotation with attribution should still be safe."
    )
    base["may_summarize_or_paraphrase"] = "unknown"
    base["may_redistribute"] = "unknown"
    base["may_use_commercially"] = "unknown"
    return base


def licenses_catalog() -> list[dict]:
    """Full source/license catalog including re-exported corpora.

    Used by:
      - `list_scholarship_sources` MCP tool (license summary block)
      - `/api/scholarship/licenses` REST endpoint
      - `/scholarship-licenses.html` static dashboard page
    """
    rows = []
    for s in SOURCES:
        rows.append({
            "source": s.key,
            "name": s.name,
            "kind": s.kind,
            "license": s.license_default,
            "license_url": s.license_url_default,
            "attribution": s.attribution,
            "homepage": s.homepage,
            "active": s.active,
        })
    for k, r in _REEXPORTED.items():
        rows.append({
            "source": k,
            "name": r["name"],
            "kind": "re-export",
            "license": r["license_default"],
            "license_url": r["license_url_default"],
            "attribution": r["attribution"],
            "homepage": r["homepage"],
            "active": True,
        })
    return rows

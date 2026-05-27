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
    license_default: Optional[str] = None
    notes: str = ""
    active: bool = True
    # For 'custom' kind: module path (e.g. "scrapers.scholarship.leges")
    custom_module: Optional[str] = None


SOURCES: list[ScholarshipSource] = [
    # ── Standalone OA law journals (small but high-quality) ─────────────
    ScholarshipSource(
        key="sui_generis",
        name="sui generis (OJS, hosted on hope.uzh.ch)",
        kind="oai_pmh",
        base_url="https://sui-generis.ch/oai",
        set_spec="suigeneris",
        license_default="CC-BY-SA-4.0",
        rate_limit=1.0,
        notes="Peer-reviewed OA Swiss law journal since 2014. Set 'suigeneris' "
              "groups all sub-sets (ART, INFO, MIGRAT, OEFF, PRIVAT, STRAF).",
        active=True,
    ),
    ScholarshipSource(
        key="leges",
        name="LeGes — Gesetzgebung & Evaluation (Bundeskanzlei)",
        kind="custom",
        custom_module="scrapers.scholarship.leges",
        license_default="OA-public-domain",
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
        notes="OA portion of the Swiss judges' association journal. Custom "
              "HTML scrape from justice-justiz-giustizia.ch.",
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

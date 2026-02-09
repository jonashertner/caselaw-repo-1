"""
Swiss Cantonal Courts Registry
================================

Maps all 26 cantons to their court decision platforms.

Platform types discovered:
1. WEBLAW — Weblaw-hosted search portals (query ticket + HTML parsing)
2. TRIBUNA — Tribuna federal GWT/AJAX portals (encrypted, paginated)
3. WEBLAW_VAADIN — Weblaw Vaadin-based portals (multiple generations: v1, v2, v3)
4. CUSTOM — Canton-specific implementations (each unique)
5. FINDINFO — VD FindInfo platform
6. OMNI — Catch-all spider for cantons with simple listings

Each entry contains:
- canton: Two-letter code
- name: Full name
- platform: Platform type
- courts: List of court instances with URLs

- notes: Implementation notes
- status: 'ready' | 'template' | 'research_needed'

This registry is used by the pipeline to determine which scraper to use.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CourtInfo:
    """A single court within a canton."""
    name: str          # e.g., "Obergericht"
    url: str           # Base URL
    has_pdf: bool = True
    has_html: bool = True
    notes: str = ""


@dataclass
class CantonRegistry:
    """Registry entry for a canton's courts."""
    canton: str
    name_de: str
    name_fr: str
    platform: str
    courts: list[CourtInfo] = field(default_factory=list)
    neuescaper_spider: str = ""
    notes: str = ""
    status: str = "template"  # 'ready', 'template', 'research_needed'


# ============================================================
# Full registry
# ============================================================

CANTON_REGISTRY: dict[str, CantonRegistry] = {

    # --- AARGAU ---
    "AG": CantonRegistry(
        canton="AG",
        name_de="Aargau",
        name_fr="Argovie",
        platform="WEBLAW",
        courts=[
            CourtInfo("Gerichte", "https://agve.weblaw.ch"),
            CourtInfo("Baugesetzgebung", "https://ag-baurecht.weblaw.ch"),
        ],
        neuescaper_spider="AG_Gerichte, AG_Baugesetzgebung, AG_Weitere",
        notes="Three separate Weblaw portals. AG_Weitere is for additional publications.",
        status="template",
    ),

    # --- APPENZELL INNERRHODEN ---
    "AI": CantonRegistry(
        canton="AI",
        name_de="Appenzell Innerrhoden",
        name_fr="Appenzell Rhodes-Intérieures",
        platform="CUSTOM",
        courts=[
            CourtInfo("Kantonsgericht", "https://www.ai.ch"),
        ],
        neuescaper_spider="AI_Aktuell, AI_Bericht",
        notes="Very small. Two spiders: current decisions + older annual reports. Custom scraping.",
        status="research_needed",
    ),

    # --- APPENZELL AUSSERRHODEN ---
    "AR": CantonRegistry(
        canton="AR",
        name_de="Appenzell Ausserrhoden",
        name_fr="Appenzell Rhodes-Extérieures",
        platform="CUSTOM",
        courts=[
            CourtInfo("Obergericht / Verwaltungsgericht", "https://www.ar.ch"),
        ],
        neuescaper_spider="AR_Gerichte",
        notes="Custom website scraping.",
        status="research_needed",
    ),

    # --- BERN ---
    "BE": CantonRegistry(
        canton="BE",
        name_de="Bern",
        name_fr="Berne",
        platform="TRIBUNA",
        courts=[
            CourtInfo("Zivil- und Strafgerichte", "https://www.justice.be.ch", notes="Tribuna-based"),
            CourtInfo("Verwaltungsgericht", "https://www.justice.be.ch", notes="Tribuna-based"),
            CourtInfo("Steuerrekurskommission", "https://be-steuerrekurs.weblaw.ch", notes="Weblaw-based"),
            CourtInfo("Anwaltsaufsicht", "https://be-anwaltsaufsicht.weblaw.ch", notes="Weblaw-based"),
        ],
        neuescaper_spider="BE_ZivilStraf, BE_Verwaltungsgericht, BE_Steuerrekurs, BE_Anwaltsaufsicht, BE_BVD, BE_Weitere",
        notes="Mixed: Tribuna for main courts, Weblaw for specialized tribunals.",
        status="template",
    ),

    # --- BASEL-LANDSCHAFT ---
    "BL": CantonRegistry(
        canton="BL",
        name_de="Basel-Landschaft",
        name_fr="Bâle-Campagne",
        platform="WEBLAW_VAADIN",
        courts=[
            CourtInfo("Gerichte", "https://blekg.weblaw.ch"),
        ],
        neuescaper_spider="BL_Gerichte",
        notes="Weblaw Vaadin portal.",
        status="template",
    ),

    # --- BASEL-STADT ---
    "BS": CantonRegistry(
        canton="BS",
        name_de="Basel-Stadt",
        name_fr="Bâle-Ville",
        platform="CUSTOM",
        courts=[
            CourtInfo("Appellationsgericht / Verwaltungsgericht", "https://www.grosserrat.bs.ch"),
        ],
        neuescaper_spider="BS_Omni",
        notes="Custom 'Omni' spider — probably a catch-all HTML scraper.",
        status="research_needed",
    ),

    # --- FRIBOURG ---
    "FR": CantonRegistry(
        canton="FR",
        name_de="Freiburg",
        name_fr="Fribourg",
        platform="TRIBUNA",
        courts=[
            CourtInfo("Kantonsgericht / Tribunal cantonal", "https://bdlf.fr.ch"),
        ],
        neuescaper_spider="FR_Gerichte",
        notes="Tribuna-based. Bilingual (de/fr).",
        status="template",
    ),

    # --- GENÈVE ---
    "GE": CantonRegistry(
        canton="GE",
        name_de="Genf",
        name_fr="Genève",
        platform="CUSTOM",
        courts=[
            CourtInfo("Cour de justice", "https://justice.ge.ch"),
        ],
        neuescaper_spider="GE_Gerichte",
        notes="Custom 165-line spider. French-language court.",
        status="template",
    ),

    # --- GLARUS ---
    "GL": CantonRegistry(
        canton="GL",
        name_de="Glarus",
        name_fr="Glaris",
        platform="CUSTOM",
        courts=[
            CourtInfo("Obergericht / Verwaltungsgericht", "https://www.gl.ch"),
        ],
        neuescaper_spider="GL_Omni",
        notes="Omni spider. Very small canton.",
        status="research_needed",
    ),

    # --- GRAUBÜNDEN ---
    "GR": CantonRegistry(
        canton="GR",
        name_de="Graubünden",
        name_fr="Grisons",
        platform="TRIBUNA",
        courts=[
            CourtInfo("Kantonsgericht / Verwaltungsgericht", "https://www.gr.ch"),
        ],
        neuescaper_spider="GR_Gerichte",
        notes="Tribuna-based. Trilingual (de/rm/it).",
        status="template",
    ),

    # --- JURA ---
    "JU": CantonRegistry(
        canton="JU",
        name_de="Jura",
        name_fr="Jura",
        platform="CUSTOM",
        courts=[
            CourtInfo("Tribunal cantonal", "https://www.jura.ch"),
        ],
        neuescaper_spider="JU_Gerichte",
        notes="Custom spider. French-language.",
        status="research_needed",
    ),

    # --- LUZERN ---
    "LU": CantonRegistry(
        canton="LU",
        name_de="Luzern",
        name_fr="Lucerne",
        platform="TRIBUNA",
        courts=[
            CourtInfo("Kantonsgericht / Verwaltungsgericht", "https://gerichte.lu.ch"),
        ],
        neuescaper_spider="LU_Gerichte",
        notes="Tribuna-based.",
        status="template",
    ),

    # --- NEUCHÂTEL ---
    "NE": CantonRegistry(
        canton="NE",
        name_de="Neuenburg",
        name_fr="Neuchâtel",
        platform="CUSTOM",
        courts=[
            CourtInfo("Tribunal cantonal", "https://www.ne.ch"),
        ],
        neuescaper_spider="NE_Omni",
        notes="Omni spider. French-language.",
        status="research_needed",
    ),

    # --- NIDWALDEN ---
    "NW": CantonRegistry(
        canton="NW",
        name_de="Nidwalden",
        name_fr="Nidwald",
        platform="CUSTOM",
        courts=[
            CourtInfo("Obergericht / Verwaltungsgericht", "https://www.nw.ch"),
        ],
        neuescaper_spider="NW_Gerichte",
        notes="Custom spider. Very small canton.",
        status="research_needed",
    ),

    # --- OBWALDEN ---
    "OW": CantonRegistry(
        canton="OW",
        name_de="Obwalden",
        name_fr="Obwald",
        platform="CUSTOM",
        courts=[
            CourtInfo("Obergericht / Verwaltungsgericht", "https://www.ow.ch"),
        ],
        neuescaper_spider="OW_Gerichte",
        notes="Custom spider. Very small canton.",
        status="research_needed",
    ),

    # --- ST. GALLEN ---
    "SG": CantonRegistry(
        canton="SG",
        name_de="St. Gallen",
        name_fr="Saint-Gall",
        platform="WEBLAW_VAADIN",
        courts=[
            CourtInfo("Gerichte", "https://sg-entscheide.weblaw.ch"),
        ],
        neuescaper_spider="SG_Gerichte, SG_Publikationen",
        notes="Weblaw Vaadin + separate publications spider.",
        status="template",
    ),

    # --- SCHAFFHAUSEN ---
    "SH": CantonRegistry(
        canton="SH",
        name_de="Schaffhausen",
        name_fr="Schaffhouse",
        platform="CUSTOM",
        courts=[
            CourtInfo("Obergericht", "https://www.sh.ch"),
        ],
        neuescaper_spider="SH_OG",
        notes="Custom spider for Obergericht.",
        status="research_needed",
    ),

    # --- SOLOTHURN ---
    "SO": CantonRegistry(
        canton="SO",
        name_de="Solothurn",
        name_fr="Soleure",
        platform="CUSTOM",
        courts=[
            CourtInfo("Obergericht / Verwaltungsgericht", "https://www.so.ch"),
        ],
        neuescaper_spider="SO_Omni",
        notes="Omni spider.",
        status="research_needed",
    ),

    # --- SCHWYZ ---
    "SZ": CantonRegistry(
        canton="SZ",
        name_de="Schwyz",
        name_fr="Schwyz",
        platform="CUSTOM",
        courts=[
            CourtInfo("Kantonsgericht", "https://www.sz.ch"),
            CourtInfo("Verwaltungsgericht", "https://www.sz.ch"),
        ],
        neuescaper_spider="SZ_Gerichte, SZ_Verwaltungsgericht",
        notes="Two separate custom spiders.",
        status="research_needed",
    ),

    # --- THURGAU ---
    "TG": CantonRegistry(
        canton="TG",
        name_de="Thurgau",
        name_fr="Thurgovie",
        platform="CUSTOM",
        courts=[
            CourtInfo("Obergericht / Verwaltungsgericht", "https://www.tg.ch"),
        ],
        neuescaper_spider="TG_OG",
        notes="Custom spider for Obergericht.",
        status="research_needed",
    ),

    # --- TICINO ---
    "TI": CantonRegistry(
        canton="TI",
        name_de="Tessin",
        name_fr="Tessin",
        platform="TRIBUNA",
        courts=[
            CourtInfo("Tribunale d'appello / Tribunale amministrativo", "https://www3.ti.ch"),
        ],
        neuescaper_spider="TI_Gerichte",
        notes="Tribuna-based. Italian-language.",
        status="template",
    ),

    # --- URI ---
    "UR": CantonRegistry(
        canton="UR",
        name_de="Uri",
        name_fr="Uri",
        platform="CUSTOM",
        courts=[
            CourtInfo("Obergericht / Verwaltungsgericht", "https://www.ur.ch"),
        ],
        neuescaper_spider="UR_Gerichte",
        notes="Custom spider. Very small canton.",
        status="research_needed",
    ),

    # --- VAUD ---
    "VD": CantonRegistry(
        canton="VD",
        name_de="Waadt",
        name_fr="Vaud",
        platform="FINDINFO",
        courts=[
            CourtInfo("Tribunal cantonal", "https://www.findinfo.ch"),
        ],
        neuescaper_spider="VD_FindInfo, VD_Omni",
        notes="FindInfo platform (154 lines) + Omni. French-language. Two spiders.",
        status="template",
    ),

    # --- VALAIS ---
    "VS": CantonRegistry(
        canton="VS",
        name_de="Wallis",
        name_fr="Valais",
        platform="CUSTOM",
        courts=[
            CourtInfo("Kantonsgericht / Tribunal cantonal", "https://www.vs.ch"),
        ],
        neuescaper_spider="VS_Gerichte",
        notes="Custom spider. Bilingual (de/fr).",
        status="research_needed",
    ),

    # --- ZUG ---
    "ZG": CantonRegistry(
        canton="ZG",
        name_de="Zug",
        name_fr="Zoug",
        platform="CUSTOM",
        courts=[
            CourtInfo("Obergericht", "https://www.zg.ch"),
            CourtInfo("Verwaltungsgericht", "https://www.zg.ch"),
        ],
        neuescaper_spider="ZG_Obergericht, ZG_Verwaltungsgericht",
        notes="Two separate custom spiders.",
        status="research_needed",
    ),

    # --- ZÜRICH ---
    "ZH": CantonRegistry(
        canton="ZH",
        name_de="Zürich",
        name_fr="Zurich",
        platform="CUSTOM",
        courts=[
            CourtInfo("Obergericht", "https://www.gerichte-zh.ch", notes="livesearch.php API, date-range pagination"),
            CourtInfo("Verwaltungsgericht", "https://vger.zh.ch"),
            CourtInfo("Sozialversicherungsgericht", "https://www.sozialversicherungsgericht.zh.ch"),
            CourtInfo("Steuerrekursgericht", "https://www.steuerrekurs.zh.ch"),
            CourtInfo("Baurekursgericht", "https://www.brg.zh.ch"),
        ],
        neuescaper_spider="ZH_Obergericht, ZH_Verwaltungsgericht, ZH_Sozialversicherungsgericht, ZH_Steuerrekurs, ZH_Baurekurs",
        notes="Most complex canton: 5 separate spiders. ZH_Obergericht uses livesearch.php with 500-day windows.",
        status="template",
    ),
}


# ============================================================
# Summary statistics
# ============================================================


def print_registry_summary():
    """Print a summary of the cantonal court registry."""
    print(f"{'Canton':<6} {'Platform':<18} {'Spiders':<60} {'Status'}")
    print("-" * 100)
    for code, entry in sorted(CANTON_REGISTRY.items()):
        print(
            f"{code:<6} {entry.platform:<18} "
            f"{entry.neuescaper_spider[:58]:<60} {entry.status}"
        )

    platforms = {}
    for entry in CANTON_REGISTRY.values():
        platforms[entry.platform] = platforms.get(entry.platform, 0) + 1

    print(f"\nPlatform distribution:")
    for p, count in sorted(platforms.items(), key=lambda x: -x[1]):
        print(f"  {p}: {count} cantons")

    statuses = {}
    for entry in CANTON_REGISTRY.values():
        statuses[entry.status] = statuses.get(entry.status, 0) + 1
    print(f"\nStatus distribution:")
    for s, count in sorted(statuses.items()):
        print(f"  {s}: {count} cantons")


if __name__ == "__main__":
    print_registry_summary()

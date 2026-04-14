"""Cantonal law scrapers — direct scraping from official cantonal portals."""
from __future__ import annotations

# Registry: canton code → (module, class_name)
CANTONAL_LAW_SCRAPERS: dict[str, tuple[str, str]] = {
    # ZH — custom AEM CMS + PDF from notes.zh.ch
    "ZH": ("scrapers.cantonal_laws.zh", "ZHScraper"),
    # SIL cantons — server-rendered HTML (Word-generated)
    "GE": ("scrapers.cantonal_laws.sil", "SILScraper"),
    "NE": ("scrapers.cantonal_laws.sil", "SILScraper"),
    # LexWork cantons (18) — all use the same API
    "AG": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "AI": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "AR": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "BE": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "BL": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "BS": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "FR": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "GL": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "GR": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "LU": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "NW": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "OW": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "SG": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "SH": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "SO": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "TG": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "VS": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
    "ZG": ("scrapers.cantonal_laws.lexwork", "LexWorkScraper"),
}

# LexWork portal hostnames per canton
LEXWORK_HOSTS: dict[str, str] = {
    "AG": "gesetzessammlungen.ag.ch",
    "AI": "ai.clex.ch",
    "AR": "ar.clex.ch",
    "BE": "www.belex.sites.be.ch",
    "BL": "bl.clex.ch",
    "BS": "www.gesetzessammlung.bs.ch",
    "FR": "fr.clex.ch",
    "GL": "gl.clex.ch",
    "GR": "www.gr-lex.gr.ch",
    "LU": "srl.lu.ch",
    "NW": "nw.clex.ch",
    "OW": "ow.clex.ch",
    "SG": "www.gesetzessammlung.sg.ch",
    "SH": "sh.clex.ch",
    "SO": "bgs.so.ch",
    "TG": "www.rechtsbuch.tg.ch",
    "VS": "vs.clex.ch",
    "ZG": "zg.clex.ch",
}

# Primary legislation language per canton
CANTON_LANG: dict[str, str] = {
    "AG": "de", "AI": "de", "AR": "de", "BE": "de", "BL": "de",
    "BS": "de", "FR": "fr", "GE": "fr", "GL": "de", "GR": "de",
    "JU": "fr", "LU": "de", "NE": "fr", "NW": "de", "OW": "de",
    "SG": "de", "SH": "de", "SO": "de", "SZ": "de", "TG": "de",
    "TI": "it", "UR": "de", "VD": "fr", "VS": "de", "ZG": "de",
    "ZH": "de",
}

"""Per-court audit: direct-scraper count vs entscheidsuche count vs portal liveness.

Output: a tab-separated report per cantonal court, with columns:
  canton | court | direct_count | es_count | total | portal | http_status | recommendation
"""
from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

# (canton, court_code, portal_url)
COURTS = [
    ("AG", "ag_gerichte", "https://agve.weblaw.ch"),
    ("AI", "ai_gerichte", "https://www.ai.ch"),
    ("AR", "ar_gerichte", "https://ar-gerichte.weblaw.ch"),
    ("BE", "be_zivilstraf", "https://www.zsg-entscheide.apps.be.ch/tribunapublikation"),
    ("BE", "be_verwaltungsgericht", "https://www.vg-urteile.apps.be.ch/tribunapublikation"),
    ("BE", "be_anwaltsaufsicht", "https://www.aa-entscheide.apps.be.ch/tribunapublikation"),
    ("BE", "be_steuerrekurs", "https://www.strk-entscheide.apps.be.ch/tribunapublikation"),
    ("BL", "bl_gerichte", "https://www.baselland.ch"),
    ("BS", "bs_gerichte", "https://www.appellationsgericht.bs.ch"),
    ("FR", "fr_gerichte", "https://bdlf.fr.ch"),
    ("GE", "ge_gerichte", "https://justice.ge.ch"),
    ("GL", "gl_gerichte", "https://findinfo.gl.ch"),
    ("GR", "gr_gerichte", "https://entscheide.gr.ch"),
    ("JU", "ju_gerichte", "https://jurisprudence.jura.ch"),
    ("LU", "lu_gerichte", "https://gerichte.lu.ch"),
    ("NE", "ne_gerichte", "https://jurisprudence.ne.ch"),
    ("NW", "nw_gerichte", "https://www.nw.ch"),
    ("OW", "ow_gerichte", "https://ow-gerichte.weblaw.ch"),
    ("SG", "sg_publikationen", "https://www.publikationen.sg.ch"),
    ("SH", "sh_gerichte", "https://obergerichtsentscheide.sh.ch"),
    ("SO", "so_gerichte", "https://so-gerichte.weblaw.ch"),
    ("SZ", "sz_gerichte", "https://gerichte.sz.ch"),
    ("SZ", "sz_verwaltungsgericht", "https://gerichte.sz.ch"),
    ("TG", "tg_gerichte", "https://rechtsprechung.tg.ch"),
    ("TI", "ti_gerichte", "https://www3.ti.ch/CAN/giurisprudenza"),
    ("UR", "ur_gerichte", "https://www.ur.ch"),
    ("VD", "vd_gerichte", "https://prestations.vd.ch/pub/101623/"),
    ("VD", "vd_findinfo", "https://www.findinfo-tc.vd.ch/justice/findinfo-pub/"),
    ("VD", "vd_omni", "(legacy aggregator)"),
    ("VS", "vs_gerichte", "https://www.vs.ch"),
    ("ZG", "zg_obergericht", "https://www.zg.ch"),
    ("ZG", "zg_verwaltungsgericht", "https://www.zg.ch"),
    ("ZH", "zh_gerichte", "https://www.gerichte-zh.ch"),
    ("ZH", "zh_verwaltungsgericht", "https://vger.zh.ch"),
    ("ZH", "zh_sozialversicherungsgericht", "https://www.sozialversicherungsgericht.zh.ch"),
    ("ZH", "zh_steuerrekursgericht", "https://www.steuerrekurs.zh.ch"),
    ("ZH", "zh_baurekursgericht", "https://www.brg.zh.ch"),
]


def probe_url(url: str) -> str:
    """Return HTTP status code as string, or 'DEAD' if no response."""
    if "(" in url:
        return "n/a"
    try:
        r = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
             "--max-time", "10", "-L", url],
            capture_output=True, text=True, timeout=15,
        )
        return r.stdout.strip() or "DEAD"
    except Exception as e:
        return f"ERR:{type(e).__name__}"


def main() -> int:
    db = Path("/opt/caselaw/repo/output/decisions.db")
    c = sqlite3.connect(str(db)).cursor()

    print(f"{'canton':6} {'court':32} {'direct':>8} {'es':>8} {'total':>8} "
          f"{'http':>5}  recommendation")
    print("-" * 110)

    for canton, court, portal in COURTS:
        direct = c.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=? AND (source IS NULL OR source != ?)",
            (court, "entscheidsuche"),
        ).fetchone()[0]
        es = c.execute(
            "SELECT COUNT(*) FROM decisions WHERE court=? AND source=?",
            (court, "entscheidsuche"),
        ).fetchone()[0]
        total = direct + es

        http = probe_url(portal)

        # Recommendation
        if direct == 0 and es == 0:
            rec = "EMPTY (no rows; check scraper)"
        elif http in ("000", "DEAD") or http.startswith("ERR"):
            if direct > 0 and es > 0:
                rec = "PORTAL DEAD; keep es as supplement"
            elif direct > 0:
                rec = "PORTAL DEAD; archive direct data"
            else:
                rec = "PORTAL DEAD; es is sole source"
        elif http in ("404", "410"):
            rec = "PORTAL 4xx; investigate"
        elif http in ("200", "301", "302"):
            if direct == 0 and es > 0:
                rec = "BUILD direct scraper (no current)"
            elif direct > 0 and es == 0:
                rec = "OK (direct only, no es)"
            elif direct > 0 and es > 0:
                pct = 100 * es / total
                if pct < 5:
                    rec = f"AUDIT then RETIRE es ({pct:.0f}%)"
                elif pct < 30:
                    rec = f"EXTEND direct (es adds {pct:.0f}%)"
                else:
                    rec = f"INVESTIGATE gap (es adds {pct:.0f}%)"
            else:
                rec = "?"
        else:
            rec = f"HTTP {http}; review"

        print(f"{canton:6} {court:32} {direct:>8d} {es:>8d} {total:>8d} "
              f"{http:>5}  {rec}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

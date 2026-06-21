"""Offline tests for the source-coverage discovery harness (scripts/source_coverage_audit.py).

Diffs the entscheidsuche universe (Spiderliste.xml) against our coverage. Uses a fixture Spiderliste
(no live network, invariant #8); the audit classifies against the REAL SPIDER_MAP + run_scraper.SCRAPERS,
which lets one test validate that every curated court code is an actually-registered scraper.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import scripts.source_coverage_audit as sca  # noqa: E402

FIXTURE = """<Spiderliste>
 <Kanton Name="Eidgenossenschaft" Kurz="ch">
   <Spider Name="CH_BGer"><Eintrag/></Spider>
   <Spider Name="CH_VB"><Eintrag/></Spider>
   <Spider Name="XX_Upload"><Eintrag/></Spider>
 </Kanton>
 <Kanton Name="Aargau" Kurz="ag">
   <Spider Name="AG_Gerichte"><Eintrag/></Spider>
   <Spider Name="AG_Baugesetzgebung"><Eintrag/></Spider>
   <Spider Name="XX_Upload"><Eintrag/></Spider>
 </Kanton>
 <Kanton Name="Neuland" Kurz="xx">
   <Spider Name="XX_BrandNewCourt"><Eintrag/></Spider>
 </Kanton>
</Spiderliste>"""


def test_fetch_dedups_per_canton_meta_bucket():
    spiders = [r["spider"] for r in sca.fetch_spiderliste(FIXTURE)]
    assert spiders.count("XX_Upload") == 1   # the per-canton manual-upload bucket -> one logical entry
    assert "CH_BGer" in spiders and "XX_BrandNewCourt" in spiders


def test_classify_each_class():
    spider_map = {"AG_Baugesetzgebung", "CH_VB"}      # es-active feeds (ingested, no direct scraper)
    scrapers = {"bger", "ag_gerichte"}
    assert sca.classify("CH_BGer", spider_map=spider_map, scrapers=scrapers) == ("direct", "bger")
    assert sca.classify("AG_Baugesetzgebung", spider_map=spider_map, scrapers=scrapers)[0] == "es-active"
    assert sca.classify("CH_VB", spider_map=spider_map, scrapers=scrapers)[0] == "es-active"
    assert sca.classify("XX_Upload", spider_map=spider_map, scrapers=scrapers)[0] == "es-meta"
    assert sca.classify("XX_BrandNewCourt", spider_map=spider_map, scrapers=scrapers)[0] == "NEW"


def test_audit_flags_new_spider_and_classifies_direct():
    report = sca.audit(spiderliste_text=FIXTURE)
    assert "XX_BrandNewCourt" in report["residual_uncovered_or_new"]
    assert any("XX_BrandNewCourt" in a for a in sca.build_alerts(report))
    cls = {r["es_spider"]: r["coverage"] for r in report["rows"]}
    assert cls["CH_BGer"] == "direct"          # real SCRAPERS has 'bger'
    assert cls["XX_Upload"] == "es-meta"


def test_curated_map_has_no_mapping_errors():
    # Every non-None court in SPIDER_TO_COURT must be a registered scraper — guards typos in the map.
    report = sca.audit(spiderliste_text=FIXTURE)
    assert report["mapping_errors"] == [], (
        f"curated SPIDER_TO_COURT points at courts not in run_scraper.SCRAPERS: {report['mapping_errors']}"
    )

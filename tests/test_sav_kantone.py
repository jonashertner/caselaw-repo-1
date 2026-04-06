import sys
sys.path.insert(0, ".")


def test_scraper_instantiates():
    from scrapers.sav_kantone import SAVKantoneScraper
    scraper = SAVKantoneScraper()
    assert scraper.court_code == "sav_kantone"


def test_scraper_registered():
    from run_scraper import SCRAPERS
    assert "sav_kantone" in SCRAPERS
    mod, cls = SCRAPERS["sav_kantone"]
    assert mod == "scrapers.sav_kantone"
    assert cls == "SAVKantoneScraper"

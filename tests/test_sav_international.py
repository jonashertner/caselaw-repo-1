import sys
sys.path.insert(0, ".")


def test_scraper_instantiates():
    from scrapers.sav_international import SAVInternationalScraper
    scraper = SAVInternationalScraper()
    assert scraper.court_code == "sav_international"


def test_scraper_registered():
    from run_scraper import SCRAPERS
    assert "sav_international" in SCRAPERS
    mod, cls = SCRAPERS["sav_international"]
    assert mod == "scrapers.sav_international"
    assert cls == "SAVInternationalScraper"

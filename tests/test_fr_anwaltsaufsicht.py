import sys
sys.path.insert(0, ".")

def test_scraper_instantiates():
    from scrapers.cantonal.fr_anwaltsaufsicht import FRAnwaltsaufsichtScraper
    scraper = FRAnwaltsaufsichtScraper()
    assert scraper.court_code == "fr_anwaltsaufsicht"

def test_scraper_registered():
    from run_scraper import SCRAPERS
    assert "fr_anwaltsaufsicht" in SCRAPERS

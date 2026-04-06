import sys
sys.path.insert(0, ".")


def test_scraper_instantiates():
    from scrapers.cantonal.tg_anwaltskommission import TGAnwaltskommissionScraper
    scraper = TGAnwaltskommissionScraper()
    assert scraper.court_code == "tg_anwaltskommission"


def test_scraper_registered():
    from run_scraper import SCRAPERS
    assert "tg_anwaltskommission" in SCRAPERS

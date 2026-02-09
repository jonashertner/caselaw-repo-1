"""
Swiss Court Scrapers — Federal + Cantonal Framework
"""
__all__ = [
    "BGerScraper", "BGELeitentscheideScraper",
    "BVGerScraper", "BStGerScraper", "BPatGerScraper",
    "ZHObergerichtScraper",
]

def get_registry() -> dict:
    """Return all available scrapers as {court_code: ScraperClass}."""
    from scrapers.bger import BGerScraper
    from scrapers.bge import BGELeitentscheideScraper
    from scrapers.bvger import BVGerScraper
    from scrapers.bstger import BStGerScraper
    from scrapers.bpatger import BPatGerScraper
    from scrapers.cantonal.zh_obergericht import ZHObergerichtScraper
    return {
        "bger": BGerScraper,
        "bge": BGELeitentscheideScraper,
        "bvger": BVGerScraper,
        "bstger": BStGerScraper,
        "bpatger": BPatGerScraper,
        "zh_obergericht": ZHObergerichtScraper,
    }

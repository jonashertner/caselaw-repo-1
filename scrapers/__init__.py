"""
Swiss Court Scrapers package exports.
"""
from __future__ import annotations

import importlib

_CLASS_MAP: dict[str, tuple[str, str]] = {
    "BgerScraper": ("scrapers.bger", "BgerScraper"),
    # Backward-compat alias for older spelling.
    "BGerScraper": ("scrapers.bger", "BgerScraper"),
    "BGELeitentscheideScraper": ("scrapers.bge", "BGELeitentscheideScraper"),
    "BVGerScraper": ("scrapers.bvger", "BVGerScraper"),
    "BStGerScraper": ("scrapers.bstger", "BStGerScraper"),
    "BPatGerScraper": ("scrapers.bpatger", "BPatGerScraper"),
    "ZHObergerichtScraper": ("scrapers.cantonal.zh_obergericht", "ZHObergerichtScraper"),
}

__all__ = ["get_registry", *_CLASS_MAP.keys()]


def __getattr__(name: str):
    """Lazily resolve scraper classes exposed at package level."""
    if name not in _CLASS_MAP:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, class_name = _CLASS_MAP[name]
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def get_registry() -> dict:
    """Return available scrapers as {court_code: ScraperClass}."""
    return {
        "bger": __getattr__("BgerScraper"),
        "bge": __getattr__("BGELeitentscheideScraper"),
        "bvger": __getattr__("BVGerScraper"),
        "bstger": __getattr__("BStGerScraper"),
        "bpatger": __getattr__("BPatGerScraper"),
        "zh_obergericht": __getattr__("ZHObergerichtScraper"),
    }

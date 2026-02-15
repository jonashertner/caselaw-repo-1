"""
Swiss Court Scrapers package exports.

get_registry() returns {court_code: ScraperClass} for all registered scrapers.
The canonical list lives in run_scraper.SCRAPERS — this module delegates to it.
"""
from __future__ import annotations

import importlib
import logging

logger = logging.getLogger(__name__)


def get_registry() -> dict:
    """Return available scrapers as {court_code: ScraperClass}.

    Delegates to run_scraper.SCRAPERS (the canonical registry) so there is
    exactly one place to add/remove scrapers.
    """
    from run_scraper import SCRAPERS

    registry: dict[str, type] = {}
    for court_code, (module_name, class_name) in sorted(SCRAPERS.items()):
        try:
            module = importlib.import_module(module_name)
            registry[court_code] = getattr(module, class_name)
        except Exception as e:
            logger.debug(f"Skipping {court_code}: {e}")
    return registry


__all__ = ["get_registry"]

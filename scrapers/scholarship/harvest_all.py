#!/usr/bin/env python3
"""Run all active OA legal scholarship harvesters.

Walks the source registry in `scrapers/scholarship/sources.py` and dispatches
each active source either to the generic OAI-PMH harvester or to a custom
adapter module.

Usage:
    python -m scrapers.scholarship.harvest_all
    python -m scrapers.scholarship.harvest_all --only sui_generis
    python -m scrapers.scholarship.harvest_all --max-records 50
"""
from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys

from . import oai_pmh
from .sources import active_sources, by_key

log = logging.getLogger("scholarship.harvest_all")


def run_source(src, *, max_records: int | None = None) -> dict:
    if src.kind == "oai_pmh":
        if not src.base_url:
            return {"source": src.key, "skipped": "no base_url"}
        license_override = None
        if src.license_authoritative and src.license_default:
            license_override = (src.license_default, src.license_url_default or "")
        return oai_pmh.harvest(
            src.base_url,
            src.key,
            set_spec=src.set_spec,
            metadata_prefix=src.metadata_prefix,
            rate_limit=src.rate_limit,
            windowed=src.windowed,
            max_records=max_records,
            subject_filter=list(src.subject_filter) if src.subject_filter else None,
            license_override=license_override,
        )
    if src.kind == "custom":
        if not src.custom_module:
            return {"source": src.key, "skipped": "no custom_module"}
        try:
            mod = importlib.import_module(src.custom_module)
        except ModuleNotFoundError:
            return {"source": src.key, "skipped": f"module {src.custom_module} not yet implemented"}
        if not hasattr(mod, "harvest"):
            return {"source": src.key, "skipped": "custom_module has no harvest()"}
        return mod.harvest(max_records=max_records)
    return {"source": src.key, "skipped": f"unknown kind {src.kind}"}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--only", default=None,
                   help="Run only the named source (e.g. 'sui_generis')")
    p.add_argument("--max-records", type=int, default=None,
                   help="Cap records per source (for smoke tests)")
    args = p.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    targets = []
    if args.only:
        s = by_key(args.only)
        if s is None:
            log.error("Unknown source key: %s", args.only)
            return 2
        targets = [s]
    else:
        targets = active_sources()

    if not targets:
        log.warning("No active sources configured.")
        return 0

    summaries = []
    for src in targets:
        log.info("=== Harvest: %s (%s) ===", src.key, src.kind)
        try:
            s = run_source(src, max_records=args.max_records)
        except Exception as e:
            log.exception("Harvest failed for %s", src.key)
            s = {"source": src.key, "error": str(e)}
        summaries.append(s)

    print(json.dumps({"summaries": summaries}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

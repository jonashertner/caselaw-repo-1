"""Offline test for the ESBK scraper (scrapers/esbk.py).

Golden HTML fixture of the admin.ch download-item component (invariant #8 — no live
network): asserts discover_new extracts docket (= the h4 title), decision date (from the
description "... vom DD. Monat YYYY"), and decision type, and dedups by DAM content hash.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scrapers.esbk import ESBKScraper  # noqa: E402

FIXTURE = """<html><body>
 <a class="download-item" aria-label="Download 62-2021-021-01"
    href="https://www.esbk.admin.ch/dam/de/sd-web/AAAAAAAA/62-2021-021-01-d.pdf"
    download="62-2021-021-01-d.pdf">
   <h4 class="download-item__title">62-2021-021-01</h4>
   <p class="download-item__description">Strafbescheid der ESBK vom 20. August 2021</p></a>
 <!-- same decision, fr variant (same DAM hash) -> must dedup -->
 <a class="download-item"
    href="https://www.esbk.admin.ch/dam/fr/sd-web/AAAAAAAA/62-2021-021-01-f.pdf"
    download="62-2021-021-01-f.pdf">
   <h4 class="download-item__title">62-2021-021-01</h4>
   <p class="download-item__description">Prononcé pénal</p></a>
 <a class="download-item"
    href="https://www.esbk.admin.ch/dam/de/sd-web/BBBBBBBB/62-2022-077-01-d.pdf"
    download="62-2022-077-01-d.pdf">
   <h4 class="download-item__title">62-2022-077-01</h4>
   <p class="download-item__description">Verfügung der ESBK vom 3. Februar 2023</p></a>
</body></html>"""


class _Resp:
    text = FIXTURE


def test_esbk_discover(monkeypatch, tmp_path):
    s = ESBKScraper(state_dir=tmp_path)
    monkeypatch.setattr(s, "get", lambda url, **k: _Resp())
    stubs = list(s.discover_new())

    # two distinct decisions (the de/fr variants of 62-2021-021-01 dedup by DAM hash)
    assert len(stubs) == 2
    by = {x["docket_number"]: x for x in stubs}
    assert set(by) == {"62-2021-021-01", "62-2022-077-01"}
    assert by["62-2021-021-01"]["decision_date"] == "20. August 2021"
    assert by["62-2021-021-01"]["decision_type"] == "Strafbescheid"
    assert by["62-2021-021-01"]["pdf_url"].endswith("62-2021-021-01-d.pdf")
    assert by["62-2022-077-01"]["decision_date"] == "3. Februar 2023"
    assert by["62-2022-077-01"]["decision_type"] == "Verfügung"


def test_esbk_since_filter(monkeypatch, tmp_path):
    from datetime import date
    s = ESBKScraper(state_dir=tmp_path)
    monkeypatch.setattr(s, "get", lambda url, **k: _Resp())
    stubs = list(s.discover_new(since_date=date(2022, 1, 1)))
    # only the 2023 decision survives the since-filter
    assert {x["docket_number"] for x in stubs} == {"62-2022-077-01"}

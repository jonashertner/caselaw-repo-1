import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.bge_egmr import BGEEGMRScraper


def test_fetch_decision_keeps_missing_date_none(tmp_path, monkeypatch):
    scraper = BGEEGMRScraper(state_dir=tmp_path)

    monkeypatch.setattr(
        scraper,
        "_safe_get",
        lambda url: type("Response", (), {"text": "<html></html>"})(),
    )
    monkeypatch.setattr(
        scraper,
        "_parse_egmr_document",
        lambda html: {"text": "Le recours est rejete."},
    )

    decision = scraper.fetch_decision(
        {
            "docket_number": "12345/67",
            "url": "https://example.com/egmr",
            "decision_date": "not-a-date",
            "case_name": "Example v. Switzerland",
        }
    )

    assert decision is not None
    assert decision.decision_date is None

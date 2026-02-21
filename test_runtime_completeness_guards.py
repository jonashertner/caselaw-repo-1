from __future__ import annotations

from pathlib import Path

import pipeline
import run_scraper
from base_scraper import BaseScraper


class _NoneReturningBaseScraper(BaseScraper):
    REQUEST_DELAY = 0.0
    MAX_NONE_RETURNS = 2

    def __init__(self, state_dir: Path):
        super().__init__(state_dir=state_dir)
        self.fetch_calls = 0

    @property
    def court_code(self) -> str:
        return "dummy_none_base"

    def discover_new(self, since_date=None):
        yield {"docket_number": "A-1"}
        yield {"docket_number": "A-2"}
        yield {"docket_number": "A-3"}

    def fetch_decision(self, stub: dict):
        self.fetch_calls += 1
        return None


class _NoneReturningPersistScraper(BaseScraper):
    REQUEST_DELAY = 0.0

    def __init__(self, state_dir: Path):
        super().__init__(state_dir=state_dir)

    @property
    def court_code(self) -> str:
        return "dummy_none_persist"

    def discover_new(self, since_date=None):
        yield {"docket_number": "B-1"}
        yield {"docket_number": "B-2"}
        yield {"docket_number": "B-3"}

    def fetch_decision(self, stub: dict):
        return None


class _PipelineDummyScraper:
    instances: list["_PipelineDummyScraper"] = []

    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.mark_called = False
        self.last_run_errors = 0
        self.__class__.instances.append(self)

    def run(self, since_date=None, max_decisions=None):
        return [object()]

    def mark_run_complete(self, decisions: list) -> None:
        self.mark_called = True


def test_base_scraper_stops_after_max_none_returns(tmp_path: Path):
    scraper = _NoneReturningBaseScraper(state_dir=tmp_path / "state")
    decisions = scraper.run()
    assert decisions == []
    # MAX_NONE_RETURNS=2, so after 2 None returns it stops (doesn't reach stub A-3).
    assert scraper.fetch_calls == 2


def test_run_scraper_does_not_count_none_returns_as_errors(tmp_path: Path, monkeypatch):
    """NoneReturns are expected for portals with broken entries.
    They should not increment the error count or cause exit code 1."""
    monkeypatch.setattr(
        run_scraper,
        "SCRAPERS",
        {"dummy_none_persist": (__name__, "_NoneReturningPersistScraper")},
    )
    errors = run_scraper.run_with_persistence(
        scraper_key="dummy_none_persist",
        output_dir=tmp_path / "output",
        state_dir=tmp_path / "state",
    )
    assert errors == 0


def test_pipeline_marks_court_failed_if_parquet_write_returns_none(tmp_path: Path, monkeypatch):
    _PipelineDummyScraper.instances.clear()
    monkeypatch.setattr(
        pipeline,
        "get_scraper_registry",
        lambda: {"dummy": _PipelineDummyScraper},
    )
    monkeypatch.setattr(
        pipeline,
        "write_parquet_shard",
        lambda decisions, output_dir, court_code: None,
    )

    results = pipeline.run_pipeline(
        courts=["dummy"],
        output_dir=tmp_path / "output",
        state_dir=tmp_path / "state",
        fail_on_any_error=False,
    )
    assert results["dummy"] == -1
    assert _PipelineDummyScraper.instances
    assert _PipelineDummyScraper.instances[0].mark_called is False


def test_pipeline_flags_court_failed_when_scraper_reports_run_errors(tmp_path: Path, monkeypatch):
    class _PipelineErrorReportingScraper(_PipelineDummyScraper):
        def run(self, since_date=None, max_decisions=None):
            self.last_run_errors = 2
            return [object()]

    _PipelineErrorReportingScraper.instances.clear()
    monkeypatch.setattr(
        pipeline,
        "get_scraper_registry",
        lambda: {"dummy": _PipelineErrorReportingScraper},
    )
    monkeypatch.setattr(
        pipeline,
        "write_parquet_shard",
        lambda decisions, output_dir, court_code: output_dir / "data" / "daily" / "dummy.parquet",
    )

    results = pipeline.run_pipeline(
        courts=["dummy"],
        output_dir=tmp_path / "output",
        state_dir=tmp_path / "state",
        fail_on_any_error=False,
    )
    assert results["dummy"] == -1
    assert _PipelineErrorReportingScraper.instances
    assert _PipelineErrorReportingScraper.instances[0].mark_called is True

from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path

import run_scraper
from models import Decision


class _DummyCoverageScraper:
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir

        class _State:
            def count(self) -> int:
                return 0

            def mark_scraped(self, decision_id: str) -> None:
                return None

        self.state = _State()

    def discover_new(self, since_date=None):
        yield {"docket_number": "A.2024.1"}
        yield {"docket_number": "A.2025.2"}

    def fetch_decision(self, stub: dict):
        docket = stub["docket_number"]
        year = 2024 if "2024" in docket else 2025
        return Decision(
            decision_id=f"dummy_cov_{docket.replace('.', '_')}",
            court="dummy_cov",
            canton="CH",
            docket_number=docket,
            decision_date=date(year, 1, 1),
            language="de",
            full_text="Lorem ipsum " * 20,
            source_url=f"https://example.test/{docket}",
        )


class _NoopCoverageScraper:
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir

        class _State:
            def count(self) -> int:
                return 0

            def mark_scraped(self, decision_id: str) -> None:
                return None

        self.state = _State()

    def discover_new(self, since_date=None):
        if False:
            yield {}
        return

    def fetch_decision(self, stub: dict):
        return None


class _EventAndGapScraper:
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir

        class _State:
            def count(self) -> int:
                return 0

            def mark_scraped(self, decision_id: str) -> None:
                return None

        self.state = _State()

    def discover_new(self, since_date=None):
        yield {"decision_id": "dummy_cov_GAP_2024_1", "docket_number": "GAP.2024.1"}
        yield {"decision_id": "dummy_cov_OK_2024_2", "docket_number": "OK.2024.2"}

    def fetch_decision(self, stub: dict):
        if stub["docket_number"] == "GAP.2024.1":
            return None
        return Decision(
            decision_id=stub["decision_id"],
            court="dummy_cov",
            canton="CH",
            docket_number=stub["docket_number"],
            decision_date=date(2024, 2, 1),
            language="de",
            full_text="Lorem ipsum " * 20,
            source_url=f"https://example.test/{stub['docket_number']}",
        )


def test_run_scraper_auto_writes_snapshots(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        run_scraper,
        "SCRAPERS",
        {"dummy_cov": (__name__, "_DummyCoverageScraper")},
    )

    errors = run_scraper.run_with_persistence(
        scraper_key="dummy_cov",
        output_dir=tmp_path / "output",
        state_dir=tmp_path / "state",
    )
    assert errors == 0

    db_path = tmp_path / "output" / "decisions.db"
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        """
        SELECT snapshot_year, expected_count
        FROM source_snapshots
        WHERE source_key = 'dummy_cov'
        ORDER BY snapshot_year
        """
    ).fetchall()
    conn.close()

    assert rows == [(2024, 1), (2025, 1)]


def test_run_scraper_snapshot_backfills_from_existing_jsonl(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        run_scraper,
        "SCRAPERS",
        {"dummy_cov": (__name__, "_NoopCoverageScraper")},
    )

    decisions_dir = tmp_path / "output" / "decisions"
    decisions_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = decisions_dir / "dummy_cov.jsonl"
    jsonl_path.write_text(
        json.dumps(
            {
                "decision_id": "dummy_cov_A_2023_9",
                "docket_number": "A.2023.9",
                "decision_date": "2023-09-01",
            }
        ) + "\n",
        encoding="utf-8",
    )

    errors = run_scraper.run_with_persistence(
        scraper_key="dummy_cov",
        output_dir=tmp_path / "output",
        state_dir=tmp_path / "state",
    )
    assert errors == 0

    conn = sqlite3.connect(str(tmp_path / "output" / "decisions.db"))
    row = conn.execute(
        """
        SELECT expected_count
        FROM source_snapshots
        WHERE source_key = 'dummy_cov' AND snapshot_year = 2023
        """
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == 1


def test_run_scraper_writes_discovery_fetch_events_and_gap_queue(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        run_scraper,
        "SCRAPERS",
        {"dummy_cov": (__name__, "_EventAndGapScraper")},
    )

    errors = run_scraper.run_with_persistence(
        scraper_key="dummy_cov",
        output_dir=tmp_path / "output",
        state_dir=tmp_path / "state",
    )
    assert errors == 0

    conn = sqlite3.connect(str(tmp_path / "output" / "decisions.db"))
    discoveries = conn.execute(
        "SELECT COUNT(*) FROM source_discoveries WHERE source_key = 'dummy_cov'"
    ).fetchone()[0]
    statuses = conn.execute(
        "SELECT status FROM source_fetch_attempts WHERE source_key = 'dummy_cov' ORDER BY id"
    ).fetchall()
    gap_rows = conn.execute(
        """
        SELECT decision_id, status, retry_count
        FROM gap_queue
        WHERE source_key = 'dummy_cov'
        ORDER BY decision_id
        """
    ).fetchall()
    conn.close()

    assert discoveries == 2
    assert [row[0] for row in statuses] == ["none", "success"]
    assert gap_rows == [("dummy_cov_GAP_2024_1", "retrying", 1)]

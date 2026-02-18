from __future__ import annotations

import sys

import pytest

import run_scraper


def test_run_scraper_list_prints_available_codes(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["run_scraper.py", "--list"])
    run_scraper.main()
    out_lines = capsys.readouterr().out.splitlines()
    assert "bger" in out_lines
    assert "ow_gerichte" in out_lines


def test_run_scraper_requires_scraper_when_not_listing(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_scraper.py"])
    with pytest.raises(SystemExit) as exc:
        run_scraper.main()
    assert exc.value.code == 2

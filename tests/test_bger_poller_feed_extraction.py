"""bger_poller feed extraction: only aza://-linked documents are new
decisions. Docket strings inside Revisions-/Erläuterungsgesuch TITLES are
references to attacked judgments and must not become phantom fetch targets
(2026-07-01/02: 5A_402/2026 x7 + three 1C phantoms produced false
doc-service-failure alarms). Fixture = the real 2026-07-02 Neuheiten page."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO / "scripts"))

import bger_poller  # noqa: E402

FIXTURE = (REPO / "tests" / "fixtures" / "bger_neuheiten_20260702.html").read_text(
    errors="ignore")


def test_real_feed_extracts_only_linked_decisions():
    dockets = bger_poller._extract_feed_dockets(FIXTURE)
    assert len(dockets) == 42  # 42 aza ids on the page; 45 raw docket strings
    # phantoms: referenced in an Erläuterungsgesuch title, not linked
    assert "1C_733/2025" not in dockets
    assert "1C_734/2025" not in dockets
    # real decisions of the day are present
    assert "1G_1/2026" in dockets
    assert all("/" in d for d in dockets)


def test_fallback_when_markup_changes():
    # dockets present but no aza ids -> broad regex keeps discovery alive
    text = "<td>5A_123/2026</td><td>anderes</td>"
    assert bger_poller._extract_feed_dockets(text) == {"5A_123/2026"}


def test_empty_page_yields_empty_set():
    assert bger_poller._extract_feed_dockets("<html><body/></html>") == set()

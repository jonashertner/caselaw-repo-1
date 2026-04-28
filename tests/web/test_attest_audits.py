"""Closing-audit tests for the four hallucination classes that
attest_response now defends against:

  case      — citation exists in corpus, pinpoint resolves
  statute   — Art. X LAW reference resolves in statutes.db
  quote     — "..." appears verbatim in a cited decision
  date      — "vom DD.MM.YYYY" matches the actual decision date

These tests exercise the parser/auditor helpers in isolation so they
do not depend on the full corpus DB. The case-citation existence path
is covered by the integration test that hits a live decisions.db.
"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture(scope="module")
def m():
    return importlib.import_module("mcp_server")


# ── Quote audit ────────────────────────────────────────────────────

def test_quote_in_source_passes(m):
    src = [{
        "regeste": "Der Vermieter haftet für Mängel an der Sache.",
        "full_text": "",
        "paragraphs": [],
    }]
    draft = (
        'Das Bundesgericht hielt fest: '
        '\u201eDer Vermieter haftet f\u00fcr M\u00e4ngel an der Sache.\u201c'
    )
    issues = m._audit_quotes(draft, src)
    assert issues == []


def test_quote_not_in_source_flagged(m):
    src = [{"regeste": "Etwas ganz anderes.", "full_text": "", "paragraphs": []}]
    draft = '\u201eDieser Satz steht so nicht im Entscheid und ist erfunden.\u201c'
    issues = m._audit_quotes(draft, src)
    assert len(issues) == 1
    assert issues[0]["category"] == "quote"
    assert issues[0]["problem"] == "quote_not_in_cited_sources"


def test_short_quote_skipped(m):
    """Quotes under 30 chars are noise (article labels, names) — not audited."""
    src = []
    draft = '\u201ekurz\u201c und ein anderes \u201eauch zu kurz\u201c'
    assert m._audit_quotes(draft, src) == []


def test_french_quotes_handled(m):
    src = [{
        "regeste": "Le bailleur est responsable des d\u00e9fauts de la chose lou\u00e9e.",
        "full_text": "", "paragraphs": [],
    }]
    draft = '\u00abLe bailleur est responsable des d\u00e9fauts de la chose lou\u00e9e.\u00bb'
    assert m._audit_quotes(draft, src) == []


def test_whitespace_normalisation_in_quote_match(m):
    """Source has line breaks; draft has the same text with single spaces."""
    src = [{
        "regeste": "Der Vermieter haftet \n\n  f\u00fcr M\u00e4ngel\n  an der Sache.",
        "full_text": "", "paragraphs": [],
    }]
    draft = '\u201eDer Vermieter haftet f\u00fcr M\u00e4ngel an der Sache.\u201c'
    assert m._audit_quotes(draft, src) == []


# ── Date audit ─────────────────────────────────────────────────────

def test_date_match_no_issue(m):
    cits = [{
        "span": (0, 14), "full_match": "BGer 4A_1/2024",
        "_decision_date": "2024-03-12",
    }]
    draft = "BGer 4A_1/2024 vom 12.03.2024 hielt fest"
    assert m._audit_dates(draft, cits) == []


def test_date_mismatch_flagged(m):
    cits = [{
        "span": (0, 14), "full_match": "BGer 4A_1/2024",
        "_decision_date": "2024-03-12",
    }]
    draft = "BGer 4A_1/2024 vom 15.03.2024 hielt fest"
    issues = m._audit_dates(draft, cits)
    assert len(issues) == 1
    assert issues[0]["category"] == "date"
    assert issues[0]["claimed_date"] == "2024-03-15"
    assert issues[0]["actual_date"] == "2024-03-12"


def test_no_adjacent_date_no_issue(m):
    cits = [{
        "span": (0, 14), "full_match": "BGer 4A_1/2024",
        "_decision_date": "2024-03-12",
    }]
    draft = "BGer 4A_1/2024 hielt fest, dass ..."
    assert m._audit_dates(draft, cits) == []


def test_unverified_citation_skipped_by_date_audit(m):
    """A citation without _decision_date (failed existence check) is not
    date-audited — we cannot know what the right date would have been."""
    cits = [{"span": (0, 14), "full_match": "BGer X", "_decision_date": ""}]
    draft = "BGer X vom 99.99.9999"
    assert m._audit_dates(draft, cits) == []


# ── Statute audit guard ────────────────────────────────────────────

def test_statute_audit_noop_without_db(m, monkeypatch, tmp_path):
    """When statutes.db is missing, the audit must NOT flag every Art. ref."""
    monkeypatch.setattr(m, "STATUTES_DB_PATH", tmp_path / "nope.db")
    draft = "Siehe Art. 41 OR und Art. 256 OR sowie Art. 999 ZZZ."
    assert m._audit_statutes(draft) == []


# ── Top-level handler shape ────────────────────────────────────────

def test_attest_empty_draft_clean(m):
    res = m._handle_attest_response(draft_text="Pure prose, no claims.")
    assert res["ok"] is True
    assert res["citations_found"] == 0
    assert res["issues_count"] == 0
    assert res["issues_by_category"] == {
        "case": 0, "statute": 0, "quote": 0, "date": 0,
    }
    # New shape promises both rails even on the no-citation path
    assert "annotated_text" in res
    assert "linked_text" in res


def test_attest_no_case_but_unsourced_quote_flagged(m):
    draft = '\u201eDies ist ein erfundenes Zitat von mehr als drei\u00dfig Zeichen.\u201c'
    res = m._handle_attest_response(draft_text=draft)
    assert res["ok"] is False
    assert res["issues_by_category"]["quote"] == 1


def test_attest_returns_required_keys(m):
    res = m._handle_attest_response(draft_text="Nothing to audit.")
    for key in (
        "ok", "citations_found", "citations_ok", "issues_count",
        "issues_by_category", "annotated_text", "linked_text",
        "issues", "_note",
    ):
        assert key in res, f"missing key: {key}"


# ── Quote-normalisation helper (unit) ──────────────────────────────

def test_normalise_collapses_whitespace_and_quotes(m):
    raw = '  \u00abHello\u00bb   \nworld\u2014today\u2019s  '
    out = m._normalise_for_quote_match(raw)
    # « » → "; — → -; ’ → '; whitespace collapsed; lowercased
    assert out == '"hello" world-today\'s'

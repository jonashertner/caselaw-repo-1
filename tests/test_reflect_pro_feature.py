"""Tests for the Pro Reflect feature.

The handler delegates to the Claude API for the actual literary
synthesis, so the unit tests mock httpx and exercise:
  • input validation (empty / too-short / unknown language)
  • LLM response parsing (well-formed JSON + raw-text fallback)
  • disclaimer wiring across locales
  • the PII guard contract (via the REST shape)

An end-to-end test against the live API is deliberately NOT included:
each call is ~$0.02 of Sonnet, and the prompt drives the model into
literary recall, which is hard to assert on programmatically. The
integration smoke that matters is "does the endpoint return the
expected JSON shape with non-empty fields", which is exercised by
the mocked tests below.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import mock

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import mcp_server


# ── Input validation ────────────────────────────────────────────────────


def test_reflect_rejects_empty_input():
    r = mcp_server._handle_reflect(redacted_text="")
    assert r["error"] == "empty_text"


def test_reflect_rejects_too_short_input():
    r = mcp_server._handle_reflect(redacted_text="Hallo Welt.")
    assert r["error"] == "too_short"


def test_reflect_rejects_missing_api_key(monkeypatch):
    monkeypatch.setattr(mcp_server, "ANTHROPIC_API_KEY", "")
    text = ("Ein Streit über das Sorgerecht für ein gemeinsames Kind. "
            "Beide Parteien beanspruchen die alleinige Obhut. " * 4)
    r = mcp_server._handle_reflect(redacted_text=text)
    assert r["error"] == "llm_unavailable"


# ── Language handling ───────────────────────────────────────────────────


@pytest.mark.parametrize("lang,expected_word", [
    ("de", "Reflexionswerkzeug"),
    ("fr", "réflexion"),
    ("it", "riflessione"),
    ("en", "Reflective"),
])
def test_reflect_disclaimer_matches_language(lang, expected_word, monkeypatch):
    """The disclaimer baked into the response must match the target
    language so the lawyer doesn't see a German legal-advice
    disclaimer on a French motion."""
    monkeypatch.setattr(mcp_server, "ANTHROPIC_API_KEY", "fake-key")

    canned = {
        "content": [{"text": json.dumps({
            "legal_issue": "Issue X",
            "literary_reference": {
                "work": "Hamlet", "author": "Shakespeare",
                "scene_or_theme": "Act III scene I",
            },
            "summary_markdown": "## Reflection\n\nbody",
            "question_for_reflection": "Q?",
        })}],
    }

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass

        def post(self, *a, **kw):
            class _R:
                status_code = 200
                def raise_for_status(self): pass
                def json(self_inner): return canned
            return _R()

    monkeypatch.setattr(mcp_server, "ANTHROPIC_API_KEY", "fake-key")
    with mock.patch("httpx.Client", _FakeClient):
        r = mcp_server._handle_reflect(
            redacted_text="A redacted legal-document chunk." * 4,
            lang=lang,
        )
    assert r["language"] == lang
    assert expected_word in r["disclaimer"]


def test_reflect_unknown_language_defaults_to_de(monkeypatch):
    """An unexpected language code must NOT crash — fall back to de."""
    monkeypatch.setattr(mcp_server, "ANTHROPIC_API_KEY", "fake")

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass

        def post(self, *a, **kw):
            class _R:
                status_code = 200
                def raise_for_status(self): pass
                def json(self_inner):
                    return {"content": [{"text": json.dumps({
                        "legal_issue": "x",
                        "literary_reference": {"work": "", "author": "",
                                                "scene_or_theme": ""},
                        "summary_markdown": "ok",
                        "question_for_reflection": "Q",
                    })}]}
            return _R()

    with mock.patch("httpx.Client", _FakeClient):
        r = mcp_server._handle_reflect(
            redacted_text="A document of reasonable length." * 4,
            lang="zz",
        )
    assert r["language"] == "de"
    assert "Reflexionswerkzeug" in r["disclaimer"]


# ── LLM response parsing ────────────────────────────────────────────────


def test_reflect_returns_structured_shape(monkeypatch):
    monkeypatch.setattr(mcp_server, "ANTHROPIC_API_KEY", "fake")

    payload = {
        "legal_issue": "Streit um die Auslegung einer Vertragsklausel.",
        "literary_reference": {
            "work": "Der Kaufmann von Venedig",
            "author": "Shakespeare",
            "scene_or_theme": "Antonio + Shylock vor dem Dogen",
        },
        "summary_markdown": "## Reflexion\n\nKörperlich ein Pfund Fleisch …",
        "question_for_reflection": "Worauf legt die Vertragstreue heute Gewicht?",
    }

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass

        def post(self, *a, **kw):
            class _R:
                status_code = 200
                def raise_for_status(self): pass
                def json(self_inner):
                    return {"content": [{"text": json.dumps(payload)}]}
            return _R()

    with mock.patch("httpx.Client", _FakeClient):
        r = mcp_server._handle_reflect(
            redacted_text=("Eine Klage wegen Vertragsverletzung "
                           "auf Lieferung einer Sache, " * 5),
            lang="de",
        )

    assert r["legal_issue"] == payload["legal_issue"]
    assert r["literary_reference"]["work"] == "Der Kaufmann von Venedig"
    assert r["literary_reference"]["author"] == "Shakespeare"
    assert "Reflexion" in r["summary_markdown"]
    assert r["question_for_reflection"].endswith("?")
    assert "Reflexionswerkzeug" in r["disclaimer"]
    assert r["_document_chars"] > 0


def test_reflect_falls_back_when_llm_returns_non_json(monkeypatch):
    """Sonnet occasionally drops the JSON contract and returns plain
    markdown. The handler must surface that text rather than 500."""
    monkeypatch.setattr(mcp_server, "ANTHROPIC_API_KEY", "fake")

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass

        def post(self, *a, **kw):
            class _R:
                status_code = 200
                def raise_for_status(self): pass
                def json(self_inner):
                    return {"content": [{"text": "## Just markdown, no JSON."}]}
            return _R()

    with mock.patch("httpx.Client", _FakeClient):
        r = mcp_server._handle_reflect(
            redacted_text=("A document worth reflecting on. " * 5), lang="en",
        )
    assert r["summary_markdown"] == "## Just markdown, no JSON."
    assert "_parse_error" in r
    # Even on parse failure, the disclaimer + language must still be set.
    assert "Reflective" in r["disclaimer"]


def test_reflect_strips_markdown_fences(monkeypatch):
    """Sonnet sometimes wraps JSON in ```json fences — handler must
    unwrap them before parsing."""
    monkeypatch.setattr(mcp_server, "ANTHROPIC_API_KEY", "fake")

    fenced = ("```json\n"
              + json.dumps({
                  "legal_issue": "x",
                  "literary_reference": {"work": "Antigone",
                                          "author": "Sophocles",
                                          "scene_or_theme": "burial conflict"},
                  "summary_markdown": "## Reflection\n\nbody",
                  "question_for_reflection": "?",
              })
              + "\n```")

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass

        def post(self, *a, **kw):
            class _R:
                status_code = 200
                def raise_for_status(self): pass
                def json(self_inner):
                    return {"content": [{"text": fenced}]}
            return _R()

    with mock.patch("httpx.Client", _FakeClient):
        r = mcp_server._handle_reflect(
            redacted_text=("A document " * 20),
        )
    assert r["literary_reference"]["work"] == "Antigone"
    assert "_parse_error" not in r


# ── Anthropic billing error surfacing ───────────────────────────────────


def test_reflect_low_credit_balance_surfaces_friendly_message(monkeypatch):
    """When Anthropic returns 400 with 'credit balance too low', the
    handler must NOT bubble the raw HTTPStatusError. The operator
    needs to top up credits; the end-user needs a friendly message.
    """
    monkeypatch.setattr(mcp_server, "ANTHROPIC_API_KEY", "fake")

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass

        def post(self, *a, **kw):
            class _R:
                status_code = 400
                def raise_for_status(self):
                    raise RuntimeError("should not be called when 400 handled")
                def json(self_inner):
                    return {
                        "type": "error",
                        "error": {
                            "type": "invalid_request_error",
                            "message": "Your credit balance is too low to access the Anthropic API.",
                        },
                    }
            return _R()

    with mock.patch("httpx.Client", _FakeClient):
        r = mcp_server._handle_reflect(
            redacted_text=("A document worth reflecting on. " * 5),
        )
    assert r["error"] == "llm_quota_exhausted"
    assert "guthaben" in r["message"].lower() or "credit" in r["message"].lower()
    assert r["_upstream_status"] == 400


def test_reflect_other_4xx_falls_through_to_generic_error(monkeypatch):
    """Non-credit-balance 4xx (e.g., rate limit, malformed) goes
    through the generic llm_request_failed path with the upstream
    message preserved for debugging."""
    monkeypatch.setattr(mcp_server, "ANTHROPIC_API_KEY", "fake")

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass

        def post(self, *a, **kw):
            class _R:
                status_code = 429
                def raise_for_status(self): raise RuntimeError("unused")
                def json(self_inner):
                    return {"error": {"type": "rate_limit_error",
                                       "message": "Rate limit exceeded."}}
            return _R()

    with mock.patch("httpx.Client", _FakeClient):
        r = mcp_server._handle_reflect(
            redacted_text=("A document worth reflecting on. " * 5),
        )
    assert r["error"] == "llm_request_failed"
    assert "429" in r["message"]
    assert "rate limit" in r["message"].lower()


# ── Network failure handling ────────────────────────────────────────────


def test_reflect_network_failure_returns_error(monkeypatch):
    monkeypatch.setattr(mcp_server, "ANTHROPIC_API_KEY", "fake")

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass

        def post(self, *a, **kw):
            raise RuntimeError("connection refused")

    with mock.patch("httpx.Client", _FakeClient):
        r = mcp_server._handle_reflect(
            redacted_text=("A document worth reflecting on. " * 5),
        )
    assert r["error"] == "llm_request_failed"
    assert "connection refused" in r["message"]


# ── REST request model shape ────────────────────────────────────────────


def test_reflect_request_model_max_length():
    """The Pydantic ReflectRequest should refuse >30KB payloads —
    the lawyer's whole document, not a 10MB attachment."""
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        mcp_server.ReflectRequest(
            license_key="key", redacted_text="x" * 30_001, lang="de",
        )


def test_reflect_request_model_accepts_valid():
    req = mcp_server.ReflectRequest(
        license_key="key", redacted_text="x" * 1000, lang="de",
    )
    assert req.lang == "de"
    assert len(req.redacted_text) == 1000

"""Static lint: the Word add-in must NOT call Anthropic from the browser.

Background: an early "Tier B" verification path in the Word add-in fetched
https://api.anthropic.com/v1/messages directly from JS using the user's
personal Anthropic API key (with header
`anthropic-dangerous-direct-browser-access: true`). That path was removed
2026-04-25 because:

  - It required pasting an LLM key into a browser (security liability —
    leaks via DevTools, browser extensions, screenshots, error reports).
  - It bypassed our billing / auth boundary.
  - The Pro flow (`verifyReferencePro()` → POST /api/billing/verify) is the
    canonical, server-side, license-based verification path.

This test guards against accidental reintroduction of the direct-browser
Anthropic call. It scans every JS file loaded by the add-in for any of the
forbidden tokens, plus the index.html itself in case someone inlines a
script tag.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ADDIN_DIR = Path(__file__).parent.parent.parent / "tools" / "word-addin"
JS_DIR = ADDIN_DIR / "js"
INDEX_HTML = ADDIN_DIR / "index.html"

# Forbidden patterns: tokens that only appear when something is calling
# Anthropic directly from the browser.
FORBIDDEN_TOKENS = (
    "api.anthropic.com",
    "anthropic-dangerous-direct-browser-access",
)

# `x-api-key` is forbidden specifically as an HTTP request header name in
# any fetch/XHR call — that's the Anthropic header. Other uses of the
# bare phrase 'x-api-key' (e.g. in comments) are tolerated as long as
# they are not part of an actual request header literal. The simplest
# robust check: if the token appears at all in the live add-in scripts,
# we fail. Comments explaining why we don't use it are fine because they
# live in this test file, not in the add-in scripts.
HEADER_TOKEN = "x-api-key"


def _addin_files() -> list[Path]:
    files: list[Path] = []
    if JS_DIR.is_dir():
        files.extend(sorted(JS_DIR.glob("*.js")))
    if INDEX_HTML.is_file():
        files.append(INDEX_HTML)
    return files


def test_addin_dir_present():
    """Sanity: the test would silently pass if the add-in dir moved."""
    assert JS_DIR.is_dir(), f"Word add-in JS dir missing: {JS_DIR}"
    assert INDEX_HTML.is_file(), f"Word add-in index.html missing: {INDEX_HTML}"
    assert _addin_files(), "No add-in JS files found — test would be a no-op"


@pytest.mark.parametrize("path", _addin_files(), ids=lambda p: p.name)
def test_no_direct_anthropic_call(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    found = [token for token in FORBIDDEN_TOKENS if token in text]
    assert not found, (
        f"{path.name} contains forbidden token(s) {found}. "
        f"Direct browser-side Anthropic API calls were removed 2026-04-25 — "
        f"use verifyReferencePro() in api.js (server-side, license-based) "
        f"instead. If you genuinely need the token in a comment, scrub it "
        f"or add a clearly-marked OK-DOC exception and update this test."
    )


@pytest.mark.parametrize("path", _addin_files(), ids=lambda p: p.name)
def test_no_x_api_key_header(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    # Match the token only when it looks like an HTTP header literal:
    # quoted, possibly mixed case, used inside a fetch/headers context.
    pattern = re.compile(r"['\"]x-api-key['\"]", re.IGNORECASE)
    assert not pattern.search(text), (
        f"{path.name} sets an 'x-api-key' header — that's the Anthropic "
        f"browser-call signature. Removed 2026-04-25; use the Pro flow."
    )

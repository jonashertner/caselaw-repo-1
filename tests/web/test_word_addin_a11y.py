"""Static lint: the Word add-in must keep its accessibility contract.

The add-in renders result cards as ``<div tabindex="0" role="button">``
rather than native ``<button>`` (the surrounding markup makes the click
target a multi-line block, which buttons render badly). That divides
labour:

  - **Mouse** users get the click via the delegated ``handleAppClick``
    listener bound on ``#app``.
  - **Keyboard / AT** users need an explicit Enter/Space-to-activate
    handler that mirrors the click — *without* it those cards are
    advertised as buttons but are silently inert.

This test guards three things, each of which has been broken at least
once in the past:

  1. The aria-live region that announces dynamic state changes
     (``#a11y-live`` in ``index.html``) is present.
  2. ``app.js`` registers a keydown listener on ``#app`` that delegates
     to ``handleAppClick`` for Enter / Space.
  3. The ``manifest.xml`` AppDomains allowlist no longer references
     ``api.anthropic.com`` (cross-checks the
     ``test_word_addin_no_browser_anthropic`` invariant from the egress
     side — both files have to drift back together for a regression).
"""
from __future__ import annotations

from pathlib import Path

ADDIN_DIR = Path(__file__).parent.parent.parent / "tools" / "word-addin"
INDEX_HTML = ADDIN_DIR / "index.html"
APP_JS = ADDIN_DIR / "js" / "app.js"
MANIFEST = ADDIN_DIR / "manifest.xml"


def test_aria_live_region_present():
    html = INDEX_HTML.read_text(encoding="utf-8")
    assert 'id="a11y-live"' in html, (
        "The polite aria-live region that announces dynamic status to AT "
        "users is missing from index.html. Re-add the <div id=\"a11y-live\" "
        "class=\"sr-only\" role=\"status\" aria-live=\"polite\"> element."
    )
    assert 'aria-live="polite"' in html, (
        "Aria-live region exists but is not marked polite — assertive would "
        "interrupt the user mid-typing on every keystroke-driven search."
    )


def test_keyboard_activation_handler():
    js = APP_JS.read_text(encoding="utf-8")
    # The keydown handler must:
    #  - listen on the #app element
    #  - check for Enter or Space
    #  - dispatch into handleAppClick (same code path as mouse)
    assert "addEventListener('keydown'" in js, (
        "app.js no longer registers a keydown listener — result cards "
        "(tabindex=0, role=button) become inert for keyboard users. "
        "Re-add the keydown delegation in initApp()."
    )
    # The handler may guard with `===` or `!==`; check both patterns,
    # and require both Enter and Space to be referenced explicitly.
    assert "'Enter'" in js, (
        "Keydown handler does not check 'Enter'. WAI-ARIA buttons must "
        "activate on Enter."
    )
    assert "' '" in js or "'Spacebar'" in js, (
        "Keydown handler does not check Space. WAI-ARIA buttons must "
        "activate on Space."
    )
    assert "handleAppClick(e)" in js, (
        "Keydown handler exists but doesn't delegate to handleAppClick — "
        "keyboard activation must follow the same path as mouse clicks."
    )


def test_manifest_does_not_allow_anthropic():
    xml = MANIFEST.read_text(encoding="utf-8")
    assert "api.anthropic.com" not in xml, (
        "manifest.xml AppDomains still allows api.anthropic.com — that "
        "egress was removed when Tier B was killed (2026-04-25). Drop the "
        "AppDomain entry to keep the iframe sandbox tight."
    )


def test_render_has_error_boundary():
    js = APP_JS.read_text(encoding="utf-8")
    # Make sure render() catches exceptions so a single bad branch can't
    # blank the whole task pane.
    assert "function render()" in js
    rs = js.index("function render()")
    body = js[rs:rs + 4000]
    assert "try" in body and "catch" in body, (
        "render() lost its try/catch boundary — a single render exception "
        "now kills the entire task pane with no recovery surface."
    )

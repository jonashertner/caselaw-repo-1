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


def test_pinpoint_label_centralised():
    """No file should hard-code the German 'E. ' label in user-facing UI.

    Pinpoint references (Erwägung / consid. / para.) localise per user
    language: DE → E., FR/IT → consid., EN → para. Centralising via
    citation.js::pinpointLabel() makes sure FR/IT/EN users don't see
    German labels next to French law text.
    """
    app = APP_JS.read_text(encoding="utf-8")
    citation = (ADDIN_DIR / "js" / "citation.js").read_text(encoding="utf-8")
    assert "function pinpointLabel" in citation, (
        "citation.js::pinpointLabel() missing — without it every UI surface "
        "has to re-derive the per-language pinpoint abbreviation, and they "
        "drift out of sync."
    )
    # The hard-coded German label `'E. '` (with trailing space — matches a
    # display-prefix rather than a regex literal) must not appear in
    # app.js. The regex on line ~1187 uses `E\.` (escaped dot, no space)
    # and is intentionally exempt. Comments that *talk about* the
    # pattern (e.g. explaining why we don't render "E. ?") are also
    # exempt — the lint targets executable code, not prose.
    import re
    def _is_comment_line(s: str) -> bool:
        stripped = s.strip()
        return stripped.startswith("//") or stripped.startswith("*") or stripped.startswith("/*")
    bad_lines = [
        (i + 1, line)
        for i, line in enumerate(app.split("\n"))
        if re.search(r"['\"]E\. ", line) and not _is_comment_line(line)
    ]
    assert not bad_lines, (
        f"app.js still hard-codes the German pinpoint label 'E. ' on "
        f"{len(bad_lines)} line(s): "
        f"{[(n, l.strip()[:80]) for n, l in bad_lines[:3]]}. "
        f"Use pinpointLabel(lang) from citation.js so FR/IT/EN users "
        f"see consid./para. instead of a German label."
    )


def test_office_locale_autodetect():
    """First-launch language must follow the user's Office display locale."""
    js = APP_JS.read_text(encoding="utf-8")
    assert "_detectInitialLang" in js, (
        "app.js no longer detects Office.context.displayLanguage on first "
        "launch — French/Italian/English users see a German UI by default, "
        "which is a major adoption blocker outside the German-speaking "
        "cantons."
    )
    assert "Office.context.displayLanguage" in js, (
        "_detectInitialLang() doesn't read Office.context.displayLanguage — "
        "the autodetect is incomplete."
    )


def test_split_erwaegung_guards_against_null_number():
    """Older BGE case-briefs come back with `number: null` for every
    Erwägung — the structured extractor couldn't parse the original.

    Without an explicit guard, splitErwaegung() builds an invalid
    regex (`/(?(?:\\.\\d+)+)/`) from the placeholder mainNum, which
    throws SyntaxError mid-render. The render() try/catch boundary
    then surfaces it as a generic "display error" — which is exactly
    what the user reported on 2026-04-27.

    The guard regex `/^\\d+(?:\\.\\d+)*$/` must stay; without it the
    BGE detail view crashes again.
    """
    js = APP_JS.read_text(encoding="utf-8")
    assert "function splitErwaegung" in js, "splitErwaegung went missing"
    # Pull just the function body so we can test the guard precisely.
    start = js.index("function splitErwaegung")
    body = js[start:start + 2500]
    assert "/^\\d+(?:\\.\\d+)*$/" in body, (
        "splitErwaegung() lost its non-numeric mainNum guard. "
        "BGE case-briefs ship with `number: null` for every Erwägung; "
        "without the guard the regex constructor throws and the global "
        "render() error boundary surfaces a generic 'display error' "
        "instead of the decision body. Re-add the early return that "
        "treats any non-numeric mainNum as a single un-split block."
    )


def test_render_skips_pinpoint_button_for_unnumbered_erwaegung():
    """When an Erwägung has no usable number, we mustn't render a
    pinpoint label / insert button — they'd produce a non-functional
    'E. ?' UI affordance and an unciteable insert action.
    """
    js = APP_JS.read_text(encoding="utf-8")
    # The hasRealNum gate must drive whether the .erwaegung-header
    # block (which contains the pinpoint label + insert button) renders.
    assert "hasRealNum" in js, (
        "renderDetail no longer distinguishes numbered vs. unnumbered "
        "Erwägungen — every entry would render an 'E. ?' label."
    )
    assert "subHasNum" in js, (
        "renderDetail no longer guards the pinpoint label/button on a "
        "per-sub-section basis — unnumbered sub-sections leak the "
        "non-functional affordance."
    )


def test_multi_select_contract():
    """Multi-citation cluster wiring must stay intact end-to-end.

    Three things have to line up: the renderer emits the toggle action,
    the click handler intercepts it before the card-level 'detail'
    action, and the keyboard shortcut for batch insert exists.
    """
    js = APP_JS.read_text(encoding="utf-8")
    citation_js = (ADDIN_DIR / "js" / "citation.js").read_text(encoding="utf-8")

    assert "data-action=\"toggle-select\"" in js, (
        "Result-card renderer no longer emits the toggle-select checkbox — "
        "users can't build a multi-citation cluster."
    )
    assert "case 'toggle-select':" in js, (
        "handleAppClick lost the toggle-select branch — checkbox clicks "
        "would fall through to the card 'detail' action and open the "
        "decision instead of selecting it."
    )
    assert "case 'multi-insert':" in js, (
        "handleAppClick lost the multi-insert branch — the 'Insert N "
        "selected' button in the floating bar wouldn't do anything."
    )
    assert "function insertMultiCitation" in js, (
        "insertMultiCitation() is gone — no implementation behind the "
        "Insert-all action."
    )
    assert "e.shiftKey && e.key === 'Enter'" in js, (
        "Ctrl/Cmd + Shift + Enter shortcut for batch insert is missing "
        "from the global keyboard handler."
    )
    assert "function formatMultiCitation" in citation_js, (
        "citation.js::formatMultiCitation is missing — the multi-bar "
        "would have no way to format the cluster."
    )

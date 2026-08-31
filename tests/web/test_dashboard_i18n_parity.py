"""Static lint: every i18n reference in the dashboard must resolve.

Background. ``docs/index.html`` is the public landing page rendered in
five languages (DE / FR / IT / RM / EN). Translation lookup is done at
runtime by ``t(key)`` which falls back to ``I18N.de[key]`` and ultimately
to ``key`` itself. A missing entry therefore renders as the literal key
name ("didyou_materialien_label") in the user's browser — silently
broken UI that telemetry doesn't catch unless someone happens to look.

This test enforces three invariants, mirroring the Word add-in's
``test_word_addin_i18n_parity.py``:

  1. **Every ``data-t="..."`` and ``t('...')`` reference has a key
     defined in *all five* languages.** A typo or a forgotten
     translation falls back to German, breaking the four-language
     contract for users in the other locales.
  2. **No key falls back silently.** The cross-product of
     (referenced keys) × (languages) must be 0 gaps — with one
     documented exception: the register-of-holdings namespace
     (``g_*`` / ``r_*``, added 2026-08) deliberately falls back to
     German for Romansh; the page itself says so ("rm falls back to
     de via t()"). For those keys rm needs no own entry, but the DE
     entry they fall back to must exist.
  3. **Soft cap on dead keys.** Defined-but-unreferenced keys >
     threshold suggest someone removed a feature without sweeping
     its strings.

The page defines its strings in TWO literals that are merged at
runtime before ``applyI18n()`` runs: the base ``var I18N={...}`` and
the JSON-shaped ``var I18N_EXT={...}`` extension from the 2026-08
register redesign. The parser below reads both; a key defined in
either is a defined key. (Found 2026-08-31: this test was red for
weeks because it parsed only the base object and reported 47 keys as
missing that the browser resolved fine.)
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

INDEX = (
    Path(__file__).parent.parent.parent / "docs" / "index.html"
)
LANGS = ("de", "fr", "it", "rm", "en")
MAX_DEAD_KEYS = 200  # current state ~135; bumped only intentionally


def _rm_may_fall_back_to_de(key: str) -> bool:
    """The register-of-holdings namespace is DE-fallback for Romansh by design.

    The page's own comment above I18N_EXT reads "rm falls back to de via
    t()". Encoding the namespace rather than a key snapshot mirrors that
    decision: new register rows inherit it, everything outside the register
    keeps strict five-language parity.
    """
    return key.startswith(("g_", "r_"))


def _src() -> str:
    return INDEX.read_text(encoding="utf-8")


def _referenced_keys(src: str) -> set[str]:
    """Collect every key referenced via ``data-t="..."`` or ``t('...')``."""
    keys: set[str] = set()
    keys.update(re.findall(r"data-t=\"([a-zA-Z_][\w]*)\"", src))
    keys.update(re.findall(r"\bt\(\s*'([a-zA-Z_][\w]*)'", src))
    return keys


def _defined_per_lang(src: str) -> dict[str, set[str]]:
    """Parse ``const I18N = { de: {...}, fr: {...}, ... }`` and return
    ``{lang: {keys defined for that lang}}``.
    """
    # The redesigned dashboard declares `var I18N={...}` (was `const I18N = {`).
    m = re.search(r"(?:const|var|let)\s+I18N\s*=\s*\{", src)
    assert m, "I18N dictionary not found in docs/index.html"
    i, depth, start = m.end(), 1, m.end()
    while depth > 0:
        ch = src[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        i += 1
    body = src[start : i - 1]

    out: dict[str, set[str]] = {}
    for lm in re.finditer(r"^\s*([a-z]{2}):\s*\{", body, re.MULTILINE):
        lang = lm.group(1)
        si, d = lm.end(), 1
        while d > 0:
            ch = body[si]
            if ch == "{":
                d += 1
            elif ch == "}":
                d -= 1
            si += 1
        block = body[lm.end() : si - 1]
        # The redesigned dict packs several keys per line
        # (`cta_connect:'...',cta_search:'...'`), so match each key after the
        # block start or a comma rather than only at line-start.
        out[lang] = set(
            re.findall(r"(?:^|,)\s*([a-zA-Z_][\w]*)\s*:", block)
        )

    # Second literal: the register-of-holdings extension, JSON-shaped and
    # merged into I18N at runtime before applyI18n(). Keys defined there are
    # as real as base keys.
    em = re.search(r"(?:const|var|let)\s+I18N_EXT\s*=\s*\{", src)
    if em:
        # Ext keys only count because the page merges them into I18N before
        # applyI18n() runs. If that merge loop ever disappears, the browser
        # stops resolving these keys and this parser must not keep counting
        # them — fail loudly instead of going green on a broken page.
        assert re.search(r"for\s*\(\s*var\s+\w+\s+in\s+I18N_EXT\s*\)", src), (
            "I18N_EXT exists but its merge-into-I18N loop is gone; the ext "
            "keys no longer reach t() at runtime. Restore the merge or "
            "update this parser."
        )
        i, depth = em.end(), 1
        while depth > 0:
            ch = src[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
            i += 1
        try:
            ext = json.loads(src[em.end() - 1 : i])
        except json.JSONDecodeError as exc:  # pragma: no cover - lint guidance
            pytest.fail(
                f"I18N_EXT in docs/index.html is no longer JSON-shaped; "
                f"update this parser to match the page: {exc}"
            )
        for lang, entries in ext.items():
            out.setdefault(lang, set()).update(entries)
    return out


@pytest.fixture(scope="module")
def src() -> str:
    return _src()


@pytest.fixture(scope="module")
def referenced(src: str) -> set[str]:
    return _referenced_keys(src)


@pytest.fixture(scope="module")
def defined(src: str) -> dict[str, set[str]]:
    return _defined_per_lang(src)


def test_parser_is_not_vacuous(
    referenced: set[str], defined: dict[str, set[str]]
) -> None:
    """Guard against the parity check passing because a parser found nothing.

    The page references ~98 keys and defines dozens per language today. If a
    page restructure changes the ``data-t`` or dictionary syntax, the regexes
    would match nothing and every test below would pass on empty sets — the
    exact silent-green failure this file exists to prevent (and the way it
    was itself broken until 2026-08-31, in the other direction).
    """
    assert len(referenced) >= 50, (
        f"only {len(referenced)} referenced i18n keys found — the data-t/t() "
        f"extraction no longer matches the page; update _referenced_keys."
    )
    for lang in LANGS:
        assert len(defined.get(lang, set())) >= 40, (
            f"only {len(defined.get(lang, set()))} keys parsed for '{lang}' — "
            f"the dictionary parser no longer matches the page; update "
            f"_defined_per_lang."
        )


def test_all_five_languages_present(defined: dict[str, set[str]]) -> None:
    missing = sorted(set(LANGS) - set(defined))
    assert not missing, (
        f"language block(s) missing from I18N: {missing}. The dashboard "
        f"is supposed to ship in all five (DE/FR/IT/RM/EN)."
    )


def test_all_referenced_keys_are_defined_in_all_languages(
    referenced: set[str], defined: dict[str, set[str]]
) -> None:
    """The cross-product of referenced keys × languages must be 0 gaps.

    Exception: register keys (``g_*``/``r_*``) need no ``rm`` entry — they
    fall back to German by documented design — but the German entry they
    fall back to must exist.
    """
    gaps_by_lang: dict[str, list[str]] = {}
    for lang in LANGS:
        gaps = referenced - defined.get(lang, set())
        if lang == "rm":
            gaps = {
                k for k in gaps
                if not (_rm_may_fall_back_to_de(k) and k in defined.get("de", set()))
            }
        gaps = sorted(gaps)
        if gaps:
            gaps_by_lang[lang] = gaps
    if gaps_by_lang:
        lines = [
            f"  [{lang}] missing {len(gaps)} key(s); first ten: "
            f"{gaps[:10]}{'...' if len(gaps) > 10 else ''}"
            for lang, gaps in gaps_by_lang.items()
        ]
        report = "\n".join(lines)
        pytest.fail(
            "Some referenced keys are not defined in every language. "
            "They will render as the literal key name in the user's "
            "browser (or fall back to DE for non-DE users):\n" + report
        )


def test_dead_keys_under_threshold(
    referenced: set[str], defined: dict[str, set[str]]
) -> None:
    union = set().union(*defined.values()) if defined else set()
    dead = sorted(union - referenced)
    assert len(dead) <= MAX_DEAD_KEYS, (
        f"{len(dead)} dead i18n keys defined but never referenced "
        f"(threshold {MAX_DEAD_KEYS}). Sample: {dead[:10]}. "
        f"Either remove or raise the cap."
    )


# NOTE: test_didyou_intro_says_eight was removed 2026-06-21. The world-class
# dashboard redesign reworded the "did you know" section and dropped the explicit
# "N facts" count phrasing (the facts now render as cards without a counted intro),
# so asserting "Acht Fakten"/"Eight facts" tested copy that no longer exists. The
# i18n-parity tests above already guarantee every facts key is translated in all
# five languages, which is the durable invariant.

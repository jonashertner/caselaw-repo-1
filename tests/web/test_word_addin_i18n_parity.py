"""Static lint: every UI string in the Word add-in must exist in DE/FR/IT/EN.

Background. The add-in ships in four languages because Switzerland has
three official languages plus an English fallback for international
users. A missing translation degrades silently — the t() function falls
back to German, which means a French legal practitioner sees German
labels on a button mid-flow with no visual signal that anything is
wrong. This test enforces three invariants:

  1. **Every t('key', ...) call has a key defined in UI_STRINGS.** A
     typo or an undefined key silently renders as the key name itself,
     producing UI like ``btn_isnert`` next to a real button.
  2. **Every defined key has all four languages (de/fr/it/en).** A
     dropped FR/IT/EN value falls back to DE, breaking the
     four-language contract.
  3. **The dictionary doesn't grow stale.** Keys defined but never
     referenced (more than 80) suggest someone removed a feature
     without sweeping its strings — fail soft (warn) so the test
     doesn't block a tactical commit, but flag for cleanup.

The parser is intentionally tolerant of both single-line entries
(``foo: { de: 'a', fr: 'b', it: 'c', en: 'd' }``) and multi-line
entries (each lang on its own line) because the dictionary mixes both
styles for compactness.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ADDIN = Path(__file__).parent.parent.parent / "tools" / "word-addin"
JS_DIR = ADDIN / "js"
I18N = JS_DIR / "i18n.js"

LANGS = ("de", "fr", "it", "en")

# Dead-key tolerance — old removed-feature strings are mostly harmless
# and cleaning them is a low-priority chore. Fail only when growth is
# obvious. Bump this number when you intentionally accumulate more.
MAX_DEAD_KEYS = 80


def _t_calls() -> set[str]:
    """Collect every key referenced via t('key', ...) across every JS file."""
    keys: set[str] = set()
    for f in sorted(JS_DIR.glob("*.js")):
        text = f.read_text(encoding="utf-8")
        for m in re.finditer(r"\bt\(\s*['\"]([a-zA-Z_][\w]*)['\"]", text):
            keys.add(m.group(1))
    return keys


def _ui_strings() -> dict[str, set[str]]:
    """Parse i18n.js UI_STRINGS dict → {key: {languages_present}}."""
    src = I18N.read_text(encoding="utf-8")
    m = re.search(r"\bvar\s+UI_STRINGS\s*=\s*\{", src)
    assert m, "UI_STRINGS dictionary not found in i18n.js"

    # Walk to find the matching close brace (the dict is the largest in
    # the file, but we still want a balanced match).
    i, depth = m.end(), 1
    start = i
    while i < len(src) and depth > 0:
        ch = src[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        i += 1
    body = src[start : i - 1]

    keys: dict[str, set[str]] = {}
    for km in re.finditer(r"^\s*([a-zA-Z_][\w]*)\s*:\s*\{", body, re.MULTILINE):
        key = km.group(1)
        si, d = km.end(), 1
        while si < len(body) and d > 0:
            ch = body[si]
            if ch == "{":
                d += 1
            elif ch == "}":
                d -= 1
            si += 1
        entry_body = body[km.end() : si - 1]
        present = set(re.findall(r"(?<![a-zA-Z_])(de|fr|it|en)\s*:\s*['\"]", entry_body))
        keys[key] = present
    return keys


@pytest.fixture(scope="session")
def keys_referenced() -> set[str]:
    return _t_calls()


@pytest.fixture(scope="session")
def keys_defined() -> dict[str, set[str]]:
    return _ui_strings()


def test_all_referenced_keys_are_defined(keys_referenced, keys_defined):
    """Every t('foo', ...) call must point at a real entry."""
    missing = sorted(keys_referenced - set(keys_defined))
    assert not missing, (
        f"{len(missing)} key(s) referenced via t() are not in UI_STRINGS — "
        f"they will render as the key name itself in the task pane: "
        f"{missing[:10]}{'...' if len(missing) > 10 else ''}. "
        f"Add them to UI_STRINGS in tools/word-addin/js/i18n.js with all "
        f"four languages (de/fr/it/en)."
    )


@pytest.mark.parametrize("lang", LANGS)
def test_every_key_has_translation(keys_defined, lang):
    """All four languages must be present for every defined key."""
    missing = sorted(k for k, langs in keys_defined.items() if lang not in langs)
    assert not missing, (
        f"{len(missing)} key(s) missing the '{lang}' translation: "
        f"{missing[:10]}{'...' if len(missing) > 10 else ''}. "
        f"Without a {lang} value the t() helper falls back to German, "
        f"breaking the four-language contract for users of that locale."
    )


def test_no_referenced_key_falls_back_silently(keys_defined, keys_referenced):
    """Cross-check: the union of missing-langs across referenced keys is 0."""
    bad = []
    for k in keys_referenced:
        if k not in keys_defined:
            continue  # caught by test_all_referenced_keys_are_defined
        gaps = set(LANGS) - keys_defined[k]
        if gaps:
            bad.append(f"{k} (missing {sorted(gaps)})")
    assert not bad, (
        f"{len(bad)} actively-used key(s) have incomplete language "
        f"coverage: {bad[:5]}{'...' if len(bad) > 5 else ''}"
    )


def test_dead_keys_under_threshold(keys_defined, keys_referenced):
    """Soft cap on dictionary cruft. Bump MAX_DEAD_KEYS when intentional."""
    dead = sorted(set(keys_defined) - keys_referenced)
    assert len(dead) <= MAX_DEAD_KEYS, (
        f"{len(dead)} dead i18n keys (defined but never referenced); "
        f"threshold is {MAX_DEAD_KEYS}. Either remove them or raise the "
        f"threshold if the new entries are intentional. First few: "
        f"{dead[:10]}"
    )

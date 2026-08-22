"""No top-level definition may be silently overwritten by a later one.

On 2026-08-22 a helper added for GitHub #89 was named `_term_coverage`, a name
already taken at line 7173 by the search reranker's scoring function. Python
keeps the *last* definition, so all five `_rerank_rows` call sites began calling
a function with a different signature. `search_decisions` and
`/api/decisions?query=` returned 500 for roughly two hours — the most-used path
in the product, at ~0.9M tool calls a day.

Nothing caught it. The full suite passed 2003 tests before and after, because no
test exercises `_rerank_rows`; only a live smoke call surfaced it. The failure
mode is invisible to review too: the two definitions were 4,800 lines apart in a
22,000-line module.

This guard is cheap, runs offline, and would have failed within a second of the
name being introduced. It is deliberately structural rather than behavioural —
it cannot tell you a function is wrong, only that two of them are fighting over
one name, which in this codebase has already proven sufficient.
"""
from __future__ import annotations

import ast
import collections
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

# Single-module files where a duplicate name is always a mistake. Keeping the
# list explicit (rather than globbing the repo) means adding a module here is a
# deliberate act, and the scripts/ one-offs — where redefinition is sometimes
# intentional — stay out of scope.
GUARDED = [
    "mcp_server.py",
    "publish.py",
    "build_fts5.py",
    "base_scraper.py",
]


def _shadowed(path: Path) -> dict[str, list[int]]:
    """Top-level def/class names bound more than once, with their line numbers."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    seen: dict[str, list[int]] = collections.defaultdict(list)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            seen[node.name].append(node.lineno)
    return {name: lines for name, lines in seen.items() if len(lines) > 1}


@pytest.mark.parametrize("filename", GUARDED)
def test_no_top_level_definition_is_shadowed(filename):
    path = REPO / filename
    if not path.exists():
        pytest.skip(f"{filename} not present")
    dupes = _shadowed(path)
    assert not dupes, (
        f"{filename} binds these names more than once at module level — only the "
        f"LAST definition is reachable, and every earlier caller silently gets it "
        f"instead:\n"
        + "\n".join(
            f"  {name}: lines {lines} (line {lines[-1]} wins)"
            for name, lines in sorted(dupes.items())
        )
        + "\n\nRename the later definition. See this file's docstring for the "
          "outage this check exists to prevent."
    )


def test_the_guard_detects_a_planted_collision(tmp_path):
    """A guard that cannot fail is not a guard."""
    planted = tmp_path / "planted.py"
    planted.write_text(
        "def f():\n    return 1\n\n\ndef g():\n    return 2\n\n\ndef f():\n    return 3\n"
    )
    dupes = _shadowed(planted)
    assert dupes == {"f": [1, 9]}


def test_methods_sharing_a_name_across_classes_are_not_flagged():
    """Two classes may each define `run` — only module-level collisions matter."""
    src = "class A:\n    def run(self): ...\n\n\nclass B:\n    def run(self): ...\n"
    tree = ast.parse(src)
    names = [n.name for n in tree.body
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
    assert names == ["A", "B"]

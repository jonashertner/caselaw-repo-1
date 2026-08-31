"""The site generator must never silently overwrite a hand-edited page.

Found 2026-08-31: tools/build_docs.py rebuilt docs/ from src/pages/ with an
unconditional out.write_text(). But src/pages/ covers only 5 of the 26 pages
under docs/, and all 5 had drifted — the 2026-08 homepage redesign (register of
holdings, hanko) was made directly in docs/index.html while src/pages/index.html
still held the April version. Nothing invoked the script (not publish.py, not
the Makefile, not CI), so the drift went unnoticed; anyone running it in good
faith, following tools/README.md, would have destroyed the live homepage.

The fix makes docs/ authoritative: a plain run refuses to overwrite any page
whose content differs from what src/pages/ would produce, and exits non-zero.
--force still overwrites, for whoever resyncs src/pages/ properly.

These tests pin that contract. If someone restores the unconditional write,
they fail.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "tools" / "build_docs.py"

LAYOUT = "<html><head><title>t</title></head><body>{{ CONTENT }}</body></html>"


def _run(cwd: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=cwd, capture_output=True, text=True,
    )


@pytest.fixture
def site(tmp_path):
    """A miniature repo: one source page, one drifted output page."""
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools" / "layout.html").write_text(LAYOUT)
    pages = tmp_path / "src" / "pages"
    pages.mkdir(parents=True)
    (pages / "index.html").write_text("<p>the April version</p>")
    docs = tmp_path / "docs"
    docs.mkdir()
    # The live page, redesigned by hand and never fed back into src/pages/.
    (docs / "index.html").write_text("<p>the redesigned homepage</p>")
    return tmp_path


def _build_module(site_root):
    """Import build_docs with its paths rebound to the fixture repo."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("build_docs_under_test", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.REPO = site_root
    mod.LAYOUT_PATH = site_root / "tools" / "layout.html"
    mod.PAGES_DIR = site_root / "src" / "pages"
    mod.OUTPUT_DIR = site_root / "docs"
    return mod


def test_plain_run_does_not_overwrite_a_diverged_page(site):
    """The core guarantee: a naive run leaves the live page byte-identical."""
    mod = _build_module(site)
    live = site / "docs" / "index.html"
    before = live.read_bytes()

    written, blocked = mod.build()

    assert live.read_bytes() == before, "the hand-edited page was clobbered"
    assert written == 0
    assert blocked == 1


def test_force_does_overwrite(site):
    """--force stays available for whoever resyncs src/pages/ deliberately."""
    mod = _build_module(site)
    live = site / "docs" / "index.html"

    written, blocked = mod.build(force=True)

    assert blocked == 0
    assert written == 1
    assert "the April version" in live.read_text()


def test_run_writes_pages_that_do_not_exist_yet(site):
    """Refusing to clobber must not block genuinely new pages."""
    mod = _build_module(site)
    (site / "src" / "pages" / "neu.html").write_text("<p>new page</p>")

    written, blocked = mod.build()

    assert (site / "docs" / "neu.html").exists()
    assert written == 1   # only the new page
    assert blocked == 1   # index.html still refused


def test_run_is_a_noop_when_already_in_sync(site):
    """A synced page is rewritten identically, not refused."""
    mod = _build_module(site)
    layout = (site / "tools" / "layout.html").read_text()
    src = (site / "src" / "pages" / "index.html").read_text()
    (site / "docs" / "index.html").write_text(mod.render_page(layout, src))

    written, blocked = mod.build()

    assert blocked == 0
    assert written == 1


def test_cli_refuses_end_to_end_in_a_sandboxed_repo(site):
    """Exercises the real CLI, exit status included, without risking docs/.

    The script derives its paths from its own location, so copying it into the
    fixture repo makes it operate entirely inside tmp_path. Deliberately NOT
    run against the real repo: if the guard regressed, a write-mode run there
    would destroy the developer's working tree as a test side effect.
    """
    import shutil

    shutil.copy(SCRIPT, site / "tools" / "build_docs.py")
    live = site / "docs" / "index.html"
    before = live.read_bytes()

    plain = subprocess.run(
        [sys.executable, str(site / "tools" / "build_docs.py")],
        cwd=site, capture_output=True, text=True,
    )

    assert live.read_bytes() == before, "a plain run rewrote the hand-edited page"
    assert plain.returncode != 0, "the refusal must be visible in the exit code"
    assert "REFUSED" in plain.stdout


def test_check_mode_never_writes_in_the_real_repo():
    """--check must be side-effect-free regardless of drift state.

    Intentionally agnostic about WHETHER the real repo is drifted: today it is
    (the 2026-08 redesign lives only in docs/), but once src/pages/ is resynced
    this must keep passing. Only the no-write property is the invariant.
    """
    live = REPO / "docs" / "index.html"
    before = live.read_bytes()
    proc = _run(REPO, "--check")
    assert live.read_bytes() == before, "--check wrote to docs/"
    assert proc.returncode in (0, 1)

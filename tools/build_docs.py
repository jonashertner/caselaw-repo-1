#!/usr/bin/env python3
"""
Build opencaselaw.ch static site.

Reads layout template and page content sources, produces the rendered
HTML files that GitHub Pages serves from docs/.

Layout: tools/layout.html — contains {{ CONTENT }} placeholder.
Pages:  src/pages/**/*.html — each file becomes a single page.

Output layout mirrors src/pages/:
  src/pages/index.html           → docs/index.html
  src/pages/entscheide/index.html → docs/entscheide/index.html
  src/pages/gesetze/index.html    → docs/gesetze/index.html
  ...

Usage:
  python3 tools/build_docs.py          # build all pages
  python3 tools/build_docs.py --check  # verify output matches what's already in docs/
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LAYOUT_PATH = REPO / "tools" / "layout.html"
PAGES_DIR = REPO / "src" / "pages"
OUTPUT_DIR = REPO / "docs"
PLACEHOLDER = "{{ CONTENT }}"


TITLE_DIRECTIVE_RE = re.compile(r"^<!--\s*title:\s*(.+?)\s*-->\s*\n?")
DESCRIPTION_DIRECTIVE_RE = re.compile(r"^<!--\s*description:\s*(.+?)\s*-->\s*\n?", re.MULTILINE)
TITLE_TAG_RE = re.compile(r"<title>[^<]*</title>")
DESCRIPTION_META_RE = re.compile(
    r'<meta name="description" content="[^"]*">'
)


def render_page(layout: str, content: str) -> str:
    """Substitute the {{ CONTENT }} placeholder in the layout with the page content.

    Supports per-page title and description overrides via leading HTML comments:
      <!-- title: Gesetze — OpenCaseLaw.ch -->
      <!-- description: Direkter Zugriff auf ... -->
    These comments are stripped from the output.
    """
    if PLACEHOLDER not in layout:
        raise ValueError(f"Layout is missing placeholder {PLACEHOLDER!r}")

    out_layout = layout
    body = content

    # Extract and strip title directive
    m = TITLE_DIRECTIVE_RE.match(body)
    if m:
        title = m.group(1)
        body = body[m.end():]
        out_layout = TITLE_TAG_RE.sub(f"<title>{title}</title>", out_layout, count=1)

    # Extract and strip description directive (anywhere near the top)
    m = DESCRIPTION_DIRECTIVE_RE.match(body)
    if m:
        desc = m.group(1)
        body = body[m.end():]
        out_layout = DESCRIPTION_META_RE.sub(
            f'<meta name="description" content="{desc}">', out_layout, count=1
        )

    return out_layout.replace(PLACEHOLDER, body)


def iter_pages() -> list[Path]:
    """Return a list of source page paths in deterministic order."""
    return sorted(PAGES_DIR.rglob("*.html"))


def output_path_for(src: Path) -> Path:
    """Map a source file path to its output path under docs/."""
    rel = src.relative_to(PAGES_DIR)
    return OUTPUT_DIR / rel


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:12]


def build(check_only: bool = False) -> int:
    """Build all pages. Returns the number of pages written (or mismatches in check mode)."""
    if not LAYOUT_PATH.exists():
        print(f"error: layout not found at {LAYOUT_PATH}", file=sys.stderr)
        return -1
    if not PAGES_DIR.exists():
        print(f"error: pages directory not found at {PAGES_DIR}", file=sys.stderr)
        return -1

    layout = LAYOUT_PATH.read_text()
    pages = iter_pages()
    if not pages:
        print(f"warning: no pages found in {PAGES_DIR}", file=sys.stderr)
        return 0

    mismatches = 0
    for src in pages:
        content = src.read_text()
        rendered = render_page(layout, content)
        out = output_path_for(src)
        rel_src = src.relative_to(REPO)
        rel_out = out.relative_to(REPO)
        if check_only:
            if not out.exists():
                print(f"MISSING  {rel_out}")
                mismatches += 1
                continue
            existing = out.read_text()
            if existing != rendered:
                print(f"DIFFERS  {rel_out}  (src {sha(existing)} != built {sha(rendered)})")
                mismatches += 1
            else:
                print(f"ok       {rel_out}  ({sha(rendered)})")
        else:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(rendered)
            print(f"built    {rel_src} -> {rel_out}  ({sha(rendered)})")

    return mismatches if check_only else len(pages)


def main():
    parser = argparse.ArgumentParser(description="Build opencaselaw.ch static site")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify output matches current docs/ without writing",
    )
    args = parser.parse_args()

    result = build(check_only=args.check)
    if args.check:
        if result == 0:
            print("\nAll pages match.")
            return 0
        else:
            print(f"\n{result} page(s) differ from current docs/", file=sys.stderr)
            return 1
    else:
        print(f"\nBuilt {result} page(s).")
        return 0


if __name__ == "__main__":
    sys.exit(main())

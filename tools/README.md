# Dashboard build

The opencaselaw.ch landing page and sub-pages are built from a shared layout template plus per-page content sources, and emitted into `docs/` which GitHub Pages serves.

## Layout

```
tools/
  build_docs.py      # the build script
  layout.html        # shared chrome (head, nav, footer, scripts) with {{ CONTENT }} placeholder
  partials/          # extracted partials (added in later commits)

src/pages/
  index.html         # landing page content
  entscheide/        # (future) case law explorer
  gesetze/           # (future) legislation browser
  mcp/               # (future) MCP connection docs
  studium/           # (future) study section
  ueber/             # (future) about + methodology + changelog
```

## Build

```bash
# From the repo root:
python3 tools/build_docs.py          # build all pages into docs/
python3 tools/build_docs.py --check   # verify docs/ matches what would be built (no writes)
```

The build script is idempotent. Running it without changing sources produces no diff.

## Before committing

Run `python3 tools/build_docs.py` to regenerate `docs/` if you've edited `tools/layout.html` or any `src/pages/**/*.html` file. The generated `docs/` files are committed to the repo because GitHub Pages serves them directly — we don't use Jekyll, Astro, or any server-side build step.

## Why a Python build script (not Jekyll)

GitHub Pages supports Jekyll out of the box, but this project already has Python everywhere and adding a Ruby toolchain for a ~7-page site is more ceremony than value. The build script is ~100 lines, has no dependencies beyond Python stdlib, and local preview is `python3 tools/build_docs.py && python3 -m http.server -d docs`.

## Byte-compatibility

As of the introduction of this build pipeline, running the script against the current sources produces a `docs/index.html` that is **byte-identical** to the previously-committed single-file version. Subsequent commits will incrementally split content into sub-pages and evolve the layout, but the initial commit is purely a refactor: no user-visible change.

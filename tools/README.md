# Dashboard build

**`docs/` is the source of truth for opencaselaw.ch. Edit it directly.**

The site was meant to be generated from a shared layout plus per-page content
sources (`tools/layout.html` + `src/pages/**`) into `docs/`, which GitHub Pages
serves. That pipeline was never finished, and the site has been maintained by
hand in `docs/` ever since. Treat `tools/build_docs.py` as dormant.

## Current state (2026-08-31)

- `src/pages/` holds 5 pages; `docs/` serves 26. The generator covers under a
  fifth of the site.
- All 5 have drifted. The 2026-08 homepage redesign (register of holdings,
  hanko, no completeness claim) was made directly in `docs/index.html`, while
  `src/pages/index.html` still holds the April version.
- Nothing invokes the script: not `publish.py`, not the `Makefile`, not CI.

Because of that, `build_docs.py` **refuses to overwrite any page that has
diverged** from what it would generate, and exits non-zero. Before that guard
existed, a well-meaning `python3 tools/build_docs.py` would have silently
destroyed the live homepage. `tests/test_build_docs_no_clobber.py` pins the
guard.

Note that `publish.py` separately runs `scripts/sync_homepage_fallbacks.py`,
which rewrites the hydration numbers inside `docs/index.html` every night. That
is a different mechanism and is working as intended.

## Commands

```bash
python3 tools/build_docs.py --check   # report drift between src/pages/ and docs/
python3 tools/build_docs.py           # build; refuses to clobber drifted pages
python3 tools/build_docs.py --force   # overwrite anyway — DESTRUCTIVE
```

Local preview of what is actually deployed:

```bash
python3 -m http.server -d docs
```

## Reviving the generator

The direction sketched in `docs/superpowers/specs/2026-06-19-redesign-KICKOFF.md`
is for every page to be built from `src/pages/` under one layout, with
`--check` gating drift in CI so no page can become a hand-edited island again.
Getting there means resyncing `src/pages/**` and `tools/layout.html` from the
current `docs/` output, then bringing the remaining 21 pages in. Until someone
does that, `--force` will discard live pages, so do not reach for it.

## Why a Python build script (not Jekyll)

GitHub Pages supports Jekyll out of the box, but this project already has Python
everywhere and adding a Ruby toolchain for a small site is more ceremony than
value. The script has no dependencies beyond the standard library.

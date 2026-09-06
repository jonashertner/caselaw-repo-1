# Windows installer for the OpenCaseLaw CLI

Courts install the offline draft check on managed Windows machines where no
Python exists and none will be installed. This directory builds a normal
Windows installer (`OpenCaseLaw-CLI-<version>-setup.exe`) that ships:

- the **python.org embeddable runtime** (`python-3.13.7-embed-amd64.zip`,
  about 11 MB) byte for byte as the PSF publishes it: `python.exe`,
  `pythonw.exe`, `python3.dll`, `python313.dll`, the extension modules
  (`_sqlite3.pyd`, `_ssl.pyd`, ...) and their DLLs (`sqlite3.dll`,
  `libssl-3.dll`, `libcrypto-3.dll`, `libffi-8.dll`, `vcruntime140*.dll`).
  These are the only executables in the installation and they carry the PSF
  Authenticode signature. Nothing is compiled or repackaged by us: no
  PyInstaller, no Nuitka, no self-extracting bundle in a user-writable
  directory;
- the **`opencaselaw_cli` package** unzipped from the wheel that `python -m
  build` produced (pure standard library) under `Lib\site-packages`;
- the **`pypdf` package** (pure Python, BSD-3-Clause, about 390 KB) unzipped
  the same way from the wheel `pypdf-6.17.0-py3-none-any.whl` that the
  workflow downloads from PyPI and checks against the SHA-256 pinned in the
  workflow, so filings that arrive as PDF are read out of the box: court IT
  installs nothing with pip. Its licence sits next to the client's as
  `LICENSE-pypdf.txt`; `TREE.json` records the wheel's file name and digest;
- a **`python313._pth`** listing exactly the standard library zip, the
  installation directory and `Lib\site-packages`, with `import site` left
  disabled, so the runtime sees nothing else on the machine (no user
  site-packages, no `PYTHONPATH`);
- three **launchers** (plain batch files, ASCII, CRLF):
  `ocl.cmd` runs `python.exe -m opencaselaw_cli` with the arguments it is
  given; `check-draft.cmd` is the "Send to" target "Eingabe oder Entwurf
  prüfen (offline)": one file gives `ocl check FILE --local` and opens
  `FILE.check.html`; several selected files (Explorer passes each as its own
  argument) give one `ocl check FILE... --local` run and open the index
  report `check-index.html` next to the first file, and a client that takes
  one file per call (exit 2, "unrecognized arguments") is called once per
  file instead; `pull-pack.cmd` is the Start-menu entry that runs `ocl pack
  pull` and keeps the window open.

## Files

| File | Role |
|---|---|
| `build_tree.py` | Lays out the tree from the embeddable zip, the wheel and any `--extra-wheel` (repeatable; pure `-none-any.whl` wheels only, unzipped with the same traversal, binary and overwrite checks as the client's wheel, a declared dependency must be another extra wheel; stdlib only, runs on any OS). Writes `TREE.json` with what went in, including each extra wheel's file name and SHA-256. |
| `ocl.iss` | Inno Setup 6.3+ script. Per-machine (`{autopf}\OpenCaseLaw`, admin) by default; `/CURRENTUSER` or the dialog gives `{localappdata}\OpenCaseLaw`. Start-menu group, "Send to" shortcut (task, on by default), optional PATH entry (task, off by default), uninstaller. UTF-8 with BOM because the shortcut names carry umlauts. |
| `ocl.cmd`, `check-draft.cmd`, `pull-pack.cmd` | Launchers, copied into the tree. |
| `smoke/make_fixture.py` | Builds the fixture pack, the fixture draft and a hand-written one-page PDF filing citing BGE 136 III 513 (no PDF library on the writing side) that the workflow checks the installed tree against (reuses the client test suite's helpers). |

The workflow is `.github/workflows/installer-cli.yml`: on every `cli-v*` tag
and on demand, on `windows-latest`, it builds the wheel, runs the client tests
against it, downloads the embeddable zip and checks its SHA-256 against the
pinned digest (taken from python.org's SPDX document for the file), downloads
the pinned pypdf wheel with `pip download pypdf==<version> --no-deps
--only-binary=:all:` and checks its SHA-256 against the digest pinned in the
workflow (printed in the log), lays out the tree, compiles with the
preinstalled Inno Setup (`C:\Program Files (x86)\Inno Setup 6\ISCC.exe`;
Chocolatey fallback), installs the result silently, runs `ocl.cmd --version`,
`ocl.cmd check --help`, an offline `ocl check` against the fixture pack,
`import pypdf` through the installed `python.exe` (it must resolve inside the
installation), an offline `ocl check` of the PDF filing (exit 0 or 4, a
report, no traceback, the cited decision in the rows) and `ocl doctor
--local`, uninstalls, writes
`<exe>.sha256`, attests build provenance, uploads both as workflow artifacts
and, on tags, attaches them to the GitHub release. It needs no secret beyond
`GITHUB_TOKEN`. A SignPath step is present but disabled; see the comments in
the workflow and `docs/court-it-install.md` for what allow-listing looks like
until signing is in place.

## Building by hand (Windows)

```
python -m pip install build
python -m build --wheel --outdir dist-wheel clients\python
curl.exe -sSfLo python-embed.zip https://www.python.org/ftp/python/3.13.7/python-3.13.7-embed-amd64.zip
certutil -hashfile python-embed.zip SHA256    # must print f6cca216a359be84797cabb54149ce5e062afb16cc7567eb7fc51cacb2d86b65
python -m pip download pypdf==6.17.0 --no-deps --only-binary=:all: -d wheels
certutil -hashfile wheels\pypdf-6.17.0-py3-none-any.whl SHA256    # must print 5bd827266a21553b74d910e350131a6227b72f2ab4209bf372814b8195fa11c5
python clients\python\installer\build_tree.py --embed-zip python-embed.zip --wheel dist-wheel\opencaselaw_cli-0.9.0-py3-none-any.whl --extra-wheel wheels\pypdf-6.17.0-py3-none-any.whl --out build\tree
"C:\Program Files (x86)\Inno Setup 6\ISCC.exe" /DAppVersion=0.9.0 /DSourceDir=%CD%\build\tree /O%CD%\dist clients\python\installer\ocl.iss
```

`build_tree.py` also runs on macOS and Linux (the tree is inert there);
`tests/test_installer_tree.py` exercises it with a fake runtime zip, a wheel
made from the source tree and fake extra wheels (accepted, recorded, imported
from the tree; refused when not pure, traversing, overwriting or unmet), and
runs the package from the tree's `Lib\site-packages` alone.
`tests/test_installer_smoke_fixture.py` checks the hand-written PDF's
structure and, where pypdf happens to be importable, that the citation comes
back from its text.

## Updating the runtime

Change `PYTHON_EMBED_VERSION` and `PYTHON_EMBED_SHA256` together in the
workflow. The digest is in `python-<version>-embed-amd64.zip.spdx.json` next
to the zip on python.org (also signed with Sigstore as `.sigstore`); do not
copy it from a third party. `build_tree.py` finds the `python3XX._pth` and
`python3XX.zip` by pattern, so a new minor version needs no other change; the
docs mention `python313` where the file name matters.

## Updating pypdf

Change `PYPDF_VERSION` and `PYPDF_SHA256` together in the workflow. The
digest is the `sha256` under `digests` of the `-py3-none-any.whl` file in
`https://pypi.org/pypi/pypdf/<version>/json`; read it there, not from a
third party, and confirm it on a download. Stay on a `py3-none-any` wheel
whose `Requires-Python` covers the embedded runtime; `build_tree.py` refuses
anything else and prints the wheel's environment-marked dependencies
(`typing_extensions` for Python < 3.11, irrelevant on 3.13) for the builder
to judge. Mention the new version in `docs/court-it-install.md`.

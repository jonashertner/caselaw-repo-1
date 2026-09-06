"""Lay out the Windows installation tree for the OpenCaseLaw CLI.

The tree is what Inno Setup packages (installer/ocl.iss) and what ends up
under Program Files: the python.org embeddable runtime as shipped by the
PSF (nothing recompiled, nothing bundled by PyInstaller), the pure-stdlib
`opencaselaw_cli` package unzipped from its wheel, a `python3XX._pth` that
makes the runtime see exactly that package, and the launcher scripts.

    python build_tree.py --embed-zip python-3.13.7-embed-amd64.zip \
        --wheel opencaselaw_cli-0.8.0-py3-none-any.whl --out build/tree

Standard library only, so the same script runs on the build runner and on a
developer machine of any OS (the tree is inert without Windows).
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
LAUNCHERS = ("ocl.cmd", "check-draft.cmd", "pull-pack.cmd")
# Files the runtime cannot work without; their absence means a wrong or
# truncated embeddable zip.
REQUIRED_RUNTIME = ("python.exe", "python3.dll", "_sqlite3.pyd", "sqlite3.dll", "_ssl.pyd", "libssl-3.dll", "libcrypto-3.dll")
_PTH = re.compile(r"^python\d+\._pth$")
_STDLIB_ZIP = re.compile(r"^python\d+\.zip$")
_WHEEL_NAME = re.compile(r"^opencaselaw_cli-(?P<version>[^-]+)-py3-none-any\.whl$")

PTH_TEMPLATE = """{stdlib_zip}
.
Lib\\site-packages
# `import site` stays disabled on purpose: no user site-packages, no PYTHONPATH,
# no .pth scanning. The runtime sees the standard library and opencaselaw_cli only.
"""


def build(embed_zip: Path, wheel: Path, out: Path, *, launcher_dir: Path = HERE) -> dict:
    match = _WHEEL_NAME.match(wheel.name)
    if not match:
        raise SystemExit(f"not an opencaselaw_cli wheel: {wheel.name}")
    version = match.group("version")
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    # 1. The PSF runtime, byte for byte.
    with zipfile.ZipFile(embed_zip) as zf:
        names = zf.namelist()
        if any("/" in name or "\\" in name or name.startswith("..") for name in names):
            raise SystemExit("the embeddable zip is flat by construction; refusing a nested or traversing entry")
        zf.extractall(out)
    missing = [name for name in REQUIRED_RUNTIME if not (out / name).is_file()]
    if missing:
        raise SystemExit(f"embeddable runtime incomplete, missing {', '.join(missing)}")
    pth_files = [name for name in names if _PTH.match(name)]
    stdlib_zips = [name for name in names if _STDLIB_ZIP.match(name)]
    if len(pth_files) != 1 or len(stdlib_zips) != 1:
        raise SystemExit(f"expected one python3XX._pth and one python3XX.zip, found {pth_files} and {stdlib_zips}")
    pth = out / pth_files[0]
    pth.write_text(PTH_TEMPLATE.format(stdlib_zip=stdlib_zips[0]), encoding="ascii", newline="\r\n")

    # 2. The package, unzipped from the wheel (no pip, no site).
    site = out / "Lib" / "site-packages"
    site.mkdir(parents=True)
    licence = None
    with zipfile.ZipFile(wheel) as zf:
        for info in zf.infolist():
            name = info.filename
            top = name.split("/", 1)[0]
            if name.startswith("..") or "\\" in name or Path(name).is_absolute():
                raise SystemExit(f"refusing wheel entry {name!r}")
            if top.endswith(".data"):
                continue  # scripts/ entry points: the launcher replaces them
            target = site / name
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
            if top.endswith(".dist-info") and Path(name).name == "LICENSE":
                licence = target
    package = site / "opencaselaw_cli"
    for required in ("__init__.py", "__main__.py", "cli.py", "local.py", "AGENTS.md"):
        if not (package / required).is_file():
            raise SystemExit(f"wheel did not provide opencaselaw_cli/{required}")
    if licence is not None:
        shutil.copyfile(licence, out / "LICENSE-opencaselaw-cli.txt")
    if (out / "LICENSE.txt").is_file():
        (out / "LICENSE.txt").rename(out / "LICENSE-python.txt")

    # 3. Launchers.
    for name in LAUNCHERS:
        source = launcher_dir / name
        if not source.is_file():
            raise SystemExit(f"launcher missing: {source}")
        # Batch files want CRLF; git may have checked them out with LF.
        text = source.read_text(encoding="ascii").replace("\r\n", "\n")
        (out / name).write_bytes(text.replace("\n", "\r\n").encode("ascii"))

    summary = {
        "version": version,
        "tree": str(out),
        "pth": pth.name,
        "runtime_files": sorted(p.name for p in out.iterdir() if p.is_file() and p.suffix.lower() in (".exe", ".dll", ".pyd")),
        "package_files": sorted(str(p.relative_to(site)).replace("\\", "/") for p in package.rglob("*") if p.is_file()),
    }
    (out / "TREE.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return summary


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--embed-zip", type=Path, required=True, help="python-3.13.x-embed-amd64.zip from python.org")
    ap.add_argument("--wheel", type=Path, required=True, help="opencaselaw_cli-<version>-py3-none-any.whl")
    ap.add_argument("--out", type=Path, required=True, help="output directory (replaced)")
    args = ap.parse_args(argv)
    summary = build(args.embed_zip.resolve(), args.wheel.resolve(), args.out.resolve())
    print(json.dumps({k: summary[k] for k in ("version", "tree", "pth")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Lay out the Windows installation tree for the OpenCaseLaw CLI.

The tree is what Inno Setup packages (installer/ocl.iss) and what ends up
under Program Files: the python.org embeddable runtime as shipped by the
PSF (nothing recompiled, nothing bundled by PyInstaller), the pure-stdlib
`opencaselaw_cli` package unzipped from its wheel, any extra pure-Python
wheels the court needs out of the box (pypdf, so filings arriving as PDF are
read without pip), a `python3XX._pth` that makes the runtime see exactly
those packages, and the launcher scripts.

    python build_tree.py --embed-zip python-3.13.7-embed-amd64.zip \
        --wheel opencaselaw_cli-0.9.0-py3-none-any.whl \
        --extra-wheel pypdf-6.17.0-py3-none-any.whl --out build/tree

Standard library only, so the same script runs on the build runner and on a
developer machine of any OS (the tree is inert without Windows).
"""
from __future__ import annotations

import argparse
import hashlib
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
# PEP 427 file name of a pure wheel: {dist}-{version}[-{build}]-{python}-none-any.whl.
# "none-any" (no ABI, any platform) is what makes a wheel installable by unzipping;
# anything else carries compiled code and is refused.
_PURE_WHEEL_NAME = re.compile(r"^(?P<dist>[A-Za-z0-9][A-Za-z0-9_.]*)-(?P<version>[A-Za-z0-9_.!+]+)"
                              r"(?:-(?P<build>\d[A-Za-z0-9_.]*))?-(?P<py>[A-Za-z0-9_.]+)-none-any\.whl$")
# Entries a pure wheel never carries; their presence means the name lies.
_BINARY_SUFFIXES = (".pyd", ".so", ".dll", ".dylib", ".exe")

PTH_TEMPLATE = """{stdlib_zip}
.
Lib\\site-packages
# `import site` stays disabled on purpose: no user site-packages, no PYTHONPATH,
# no .pth scanning. The runtime sees the standard library and opencaselaw_cli only.
"""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalise(dist: str) -> str:
    """PEP 503 name normalisation, so `typing_extensions`, `typing-extensions` and `Typing.Extensions` agree."""
    return re.sub(r"[-_.]+", "-", dist).lower()


def _unpack_wheel(wheel: Path, site: Path, *, label: str) -> tuple[list[str], Path | None, str | None]:
    """Unzip a wheel's packages into site-packages the way pip would, without pip.

    Returns the files written (relative to site-packages, forward slashes), the
    LICENSE file of the .dist-info if there is one, and the METADATA text. Every
    entry is checked against traversal, absolute paths and backslashes; the
    .data directory (entry-point scripts, headers) is skipped; a file another
    wheel already put in place is refused rather than overwritten.
    """
    written: list[str] = []
    licence = None
    metadata = None
    with zipfile.ZipFile(wheel) as zf:
        for info in zf.infolist():
            name = info.filename
            top = name.split("/", 1)[0]
            if ".." in Path(name).parts or "\\" in name or Path(name).is_absolute() or ":" in name or not name.strip():
                raise SystemExit(f"refusing {label} entry {name!r}")
            if top.endswith(".data"):
                continue  # scripts/ entry points: the launcher replaces them
            if name.lower().endswith(_BINARY_SUFFIXES):
                raise SystemExit(f"{label} {wheel.name} is not pure Python: it carries {name!r}")
            target = site / name
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if target.exists():
                raise SystemExit(f"{label} {wheel.name} would overwrite {name!r} placed by an earlier wheel")
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
            written.append(name)
            if top.endswith(".dist-info"):
                if Path(name).name.upper().startswith("LICENSE"):
                    licence = target
                elif Path(name).name == "METADATA":
                    metadata = target.read_text(encoding="utf-8", errors="replace")
    return written, licence, metadata


def _hard_requirements(metadata: str | None) -> list[str]:
    """Requires-Dist lines that are not behind an `extra ==` marker."""
    if not metadata:
        return []
    found = []
    for line in metadata.splitlines():
        if not line.startswith("Requires-Dist:"):
            continue
        requirement = line.split(":", 1)[1].strip()
        marker = requirement.split(";", 1)[1] if ";" in requirement else ""
        if "extra" in marker:
            continue
        found.append(requirement)
    return found


def build(embed_zip: Path, wheel: Path, out: Path, *, extra_wheels: tuple[Path, ...] | list[Path] = (),
          launcher_dir: Path = HERE) -> dict:
    match = _WHEEL_NAME.match(wheel.name)
    if not match:
        raise SystemExit(f"not an opencaselaw_cli wheel: {wheel.name}")
    version = match.group("version")
    extras = []
    for extra in extra_wheels:
        extra_match = _PURE_WHEEL_NAME.match(extra.name)
        if not extra_match:
            raise SystemExit(f"not a pure wheel (the name must end in -none-any.whl): {extra.name}")
        if _normalise(extra_match.group("dist")) == "opencaselaw-cli":
            raise SystemExit(f"{extra.name} is the client itself; pass it as --wheel")
        extras.append((extra, extra_match.group("dist"), extra_match.group("version")))
    if out.exists():
        if any(out.iterdir()) and not (out / "TREE.json").is_file():
            raise SystemExit(f"{out} is not empty and holds no TREE.json from an earlier build; refusing to delete it")
        shutil.rmtree(out)
    out.mkdir(parents=True)
    # Claim the directory at once: a build that stops half-way (a refused wheel)
    # leaves a tree the next build recognises as its own and replaces.
    (out / "TREE.json").write_text(json.dumps({"status": "incomplete", "version": version}) + "\n", encoding="utf-8")

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
    _, licence, _ = _unpack_wheel(wheel, site, label="wheel")
    package = site / "opencaselaw_cli"
    for required in ("__init__.py", "__main__.py", "cli.py", "local.py", "AGENTS.md"):
        if not (package / required).is_file():
            raise SystemExit(f"wheel did not provide opencaselaw_cli/{required}")
    if licence is not None:
        shutil.copyfile(licence, out / "LICENSE-opencaselaw-cli.txt")
    if (out / "LICENSE.txt").is_file():
        (out / "LICENSE.txt").rename(out / "LICENSE-python.txt")

    # 2b. Extra pure wheels (pypdf), unzipped the same way. The tree has no pip,
    # so a dependency the wheel declares must itself be one of the extra wheels;
    # a dependency behind an environment marker (typing_extensions on Python
    # < 3.11) is reported for the builder to judge against the runtime's version.
    extra_summaries = []
    provided = {_normalise(dist) for _, dist, _ in extras}
    for extra, dist, extra_version in extras:
        written, extra_licence, metadata = _unpack_wheel(extra, site, label="extra wheel")
        top_level = sorted({name.split("/", 1)[0] for name in written if not name.split("/", 1)[0].endswith(".dist-info")})
        importable = [t for t in top_level if (site / t / "__init__.py").is_file() or (t.endswith(".py") and (site / t).is_file())]
        if not importable:
            raise SystemExit(f"extra wheel {extra.name} provides no importable package or module")
        requirements = _hard_requirements(metadata)
        for requirement in requirements:
            name = re.split(r"[\s;<>=!~\[(]", requirement, 1)[0]
            if ";" not in requirement and _normalise(name) not in provided:
                raise SystemExit(f"extra wheel {extra.name} requires {name}, which no --extra-wheel provides")
            if ";" in requirement:
                print(f"note: {extra.name} declares {requirement!r}; the embedded runtime is the python3XX in {pth.name}", file=sys.stderr)
        licence_name = None
        if extra_licence is not None:
            licence_name = f"LICENSE-{_normalise(dist)}.txt"
            shutil.copyfile(extra_licence, out / licence_name)
        extra_summaries.append({
            "file": extra.name,
            "distribution": dist,
            "version": extra_version,
            "sha256": _sha256(extra),
            "top_level": top_level,
            "files": len(written),
            "licence": licence_name,
            "requires": requirements,
        })

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
        "wheel": {"file": wheel.name, "sha256": _sha256(wheel)},
        "runtime_files": sorted(p.name for p in out.iterdir() if p.is_file() and p.suffix.lower() in (".exe", ".dll", ".pyd")),
        "package_files": sorted(str(p.relative_to(site)).replace("\\", "/") for p in package.rglob("*") if p.is_file()),
        "extra_wheels": extra_summaries,
    }
    (out / "TREE.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return summary


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--embed-zip", type=Path, required=True, help="python-3.13.x-embed-amd64.zip from python.org")
    ap.add_argument("--wheel", type=Path, required=True, help="opencaselaw_cli-<version>-py3-none-any.whl")
    ap.add_argument("--extra-wheel", type=Path, action="append", default=[], metavar="PATH",
                    help="a pure-Python wheel (-none-any.whl) to unzip into Lib\\site-packages as well, e.g. pypdf; repeatable")
    ap.add_argument("--out", type=Path, required=True, help="output directory (replaced)")
    args = ap.parse_args(argv)
    summary = build(args.embed_zip.resolve(), args.wheel.resolve(), args.out.resolve(),
                    extra_wheels=[p.resolve() for p in args.extra_wheel])
    printed = {k: summary[k] for k in ("version", "tree", "pth")}
    printed["extra_wheels"] = [{k: e[k] for k in ("file", "sha256")} for e in summary["extra_wheels"]]
    print(json.dumps(printed))
    return 0


if __name__ == "__main__":
    sys.exit(main())

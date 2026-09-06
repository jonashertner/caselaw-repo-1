"""The Windows installation tree (installer/build_tree.py) is laid out from a wheel
and the python.org embeddable zip. Neither is at hand offline, so both are faked:
the wheel is the source package zipped the way `python -m build` would, the runtime
is a flat zip with placeholder binaries. The resulting tree is then run with this
machine's Python to prove the package works from `Lib\\site-packages` alone."""
import importlib.util
import os
import subprocess
import sys
import zipfile
from pathlib import Path

CLIENT = Path(__file__).resolve().parents[1]
SRC = CLIENT / "src" / "opencaselaw_cli"
INSTALLER = CLIENT / "installer"


def _load_build_tree():
    spec = importlib.util.spec_from_file_location("build_tree", INSTALLER / "build_tree.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fake_wheel(path: Path, version: str) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        for file in sorted(SRC.rglob("*")):
            if file.is_file() and "__pycache__" not in file.parts:
                zf.write(file, f"opencaselaw_cli/{file.relative_to(SRC).as_posix()}")
        info = f"opencaselaw_cli-{version}.dist-info"
        zf.writestr(f"{info}/METADATA", f"Metadata-Version: 2.4\nName: opencaselaw-cli\nVersion: {version}\n")
        zf.writestr(f"{info}/licenses/LICENSE", (CLIENT / "LICENSE").read_text(encoding="utf-8"))
        zf.writestr(f"opencaselaw_cli-{version}.data/scripts/ocl", "#!python\n")  # must be skipped
    return path


def _fake_embed_zip(path: Path) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        for name in ("python.exe", "pythonw.exe", "python313.dll", "python3.dll", "_sqlite3.pyd", "sqlite3.dll",
                     "_ssl.pyd", "libssl-3.dll", "libcrypto-3.dll", "python313.zip", "python.cat"):
            zf.writestr(name, b"MZ" if name.endswith((".exe", ".dll", ".pyd")) else b"")
        zf.writestr("python313._pth", "python313.zip\n.\n\n# Uncomment to run site.main() automatically\n#import site\n")
        zf.writestr("LICENSE.txt", "PSF licence\n")
    return path


def test_tree_layout_pth_and_launchers(tmp_path):
    build_tree = _load_build_tree()
    from opencaselaw_cli._version import __version__
    wheel = _fake_wheel(tmp_path / f"opencaselaw_cli-{__version__}-py3-none-any.whl", __version__)
    embed = _fake_embed_zip(tmp_path / "python-3.13.7-embed-amd64.zip")
    out = tmp_path / "tree"
    summary = build_tree.build(embed, wheel, out)

    assert summary["version"] == __version__ and summary["pth"] == "python313._pth"
    pth = (out / "python313._pth").read_bytes()
    assert pth.startswith(b"python313.zip\r\n.\r\nLib\\site-packages\r\n")
    assert b"\nimport site" not in pth  # only the comment mentions it
    package = out / "Lib" / "site-packages" / "opencaselaw_cli"
    for name in ("__init__.py", "__main__.py", "cli.py", "local.py", "AGENTS.md", "skills/citation-check/SKILL.md"):
        assert (package / name).is_file(), name
    assert not list((out / "Lib" / "site-packages").glob("*.data")), "entry-point scripts must not be installed"
    assert (out / "LICENSE-opencaselaw-cli.txt").is_file() and (out / "LICENSE-python.txt").is_file()
    for launcher in ("ocl.cmd", "check-draft.cmd", "pull-pack.cmd"):
        body = (out / launcher).read_bytes()
        assert body.startswith(b"@echo off\r\n") and b"\n" not in body.replace(b"\r\n", b""), launcher
        body.decode("ascii")  # cmd.exe reads the OEM code page; keep the scripts ASCII
    assert b'"%OCL_HOME%python.exe" -m opencaselaw_cli %*' in (out / "ocl.cmd").read_bytes()
    assert b'check "%DRAFT%" --local' in (out / "check-draft.cmd").read_bytes()
    assert b"pack pull" in (out / "pull-pack.cmd").read_bytes()


def test_package_runs_from_the_tree_alone(tmp_path):
    """`python -m opencaselaw_cli` from the tree's site-packages, with the source
    checkout kept off the path, is what ocl.cmd does on Windows."""
    build_tree = _load_build_tree()
    from opencaselaw_cli._version import __version__
    wheel = _fake_wheel(tmp_path / f"opencaselaw_cli-{__version__}-py3-none-any.whl", __version__)
    embed = _fake_embed_zip(tmp_path / "python-embed.zip")
    out = tmp_path / "tree"
    build_tree.build(embed, wheel, out)
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}
    env["PYTHONPATH"] = str(out / "Lib" / "site-packages")
    result = subprocess.run([sys.executable, "-s", "-m", "opencaselaw_cli", "--version"],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"ocl {__version__}"
    result = subprocess.run([sys.executable, "-s", "-m", "opencaselaw_cli", "check", "--help"],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0 and "--report" in result.stdout


def test_wheel_name_and_runtime_are_checked(tmp_path):
    build_tree = _load_build_tree()
    import pytest
    embed = _fake_embed_zip(tmp_path / "python-embed.zip")
    with pytest.raises(SystemExit, match="not an opencaselaw_cli wheel"):
        build_tree.build(embed, tmp_path / "other-1.0-py3-none-any.whl", tmp_path / "t1")
    wheel = _fake_wheel(tmp_path / "opencaselaw_cli-9.9.9-py3-none-any.whl", "9.9.9")
    with zipfile.ZipFile(tmp_path / "short.zip", "w") as zf:
        zf.writestr("python.exe", b"MZ"); zf.writestr("python313._pth", "python313.zip\n.\n"); zf.writestr("python313.zip", b"")
    with pytest.raises(SystemExit, match="runtime incomplete"):
        build_tree.build(tmp_path / "short.zip", wheel, tmp_path / "t2")

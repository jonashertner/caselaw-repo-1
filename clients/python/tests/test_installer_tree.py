"""The Windows installation tree (installer/build_tree.py) is laid out from a wheel
and the python.org embeddable zip. Neither is at hand offline, so both are faked:
the wheel is the source package zipped the way `python -m build` would, the runtime
is a flat zip with placeholder binaries. The resulting tree is then run with this
machine's Python to prove the package works from `Lib\\site-packages` alone."""
import importlib.util
import json
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
    send_to = (out / "check-draft.cmd").read_bytes()
    assert b'set "LOCALFLAG=--local"' in send_to and b'check "%DRAFT%" %LOCALFLAG%' in send_to
    assert b"check %* %LOCALFLAG%" in send_to and b'if not "%RC%"=="2" goto :multi_done' in send_to  # batch, then one per file
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


def _fake_pure_wheel(path: Path, dist: str, version: str, files: dict[str, str], *, requires: tuple[str, ...] = ()) -> Path:
    """A pure wheel the way flit or setuptools would write it: packages plus a .dist-info."""
    with zipfile.ZipFile(path, "w") as zf:
        for name, body in files.items():
            zf.writestr(name, body)
        info = f"{dist}-{version}.dist-info"
        metadata = f"Metadata-Version: 2.4\nName: {dist}\nVersion: {version}\nLicense-Expression: BSD-3-Clause\n"
        metadata += "".join(f"Requires-Dist: {r}\n" for r in requires)
        zf.writestr(f"{info}/METADATA", metadata)
        zf.writestr(f"{info}/WHEEL", "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        zf.writestr(f"{info}/licenses/LICENSE", f"{dist} licence text\n")
        zf.writestr(f"{info}/RECORD", "")
    return path


def _inputs(tmp_path):
    build_tree = _load_build_tree()
    from opencaselaw_cli._version import __version__
    wheel = _fake_wheel(tmp_path / f"opencaselaw_cli-{__version__}-py3-none-any.whl", __version__)
    embed = _fake_embed_zip(tmp_path / "python-3.13.7-embed-amd64.zip")
    return build_tree, wheel, embed


def test_extra_wheel_is_unpacked_recorded_and_importable(tmp_path):
    """pypdf stands in for any pure wheel: its package lands in Lib\\site-packages next to
    the client, its licence is copied out for court IT, TREE.json records file and digest,
    and the package imports from the tree alone."""
    build_tree, wheel, embed = _inputs(tmp_path)
    extra = _fake_pure_wheel(tmp_path / "fakepdf-1.2.3-py3-none-any.whl", "fakepdf", "1.2.3",
                             {"fakepdf/__init__.py": "__version__ = '1.2.3'\n", "fakepdf/reader.py": "def pages():\n    return 1\n",
                              "fakepdf-1.2.3.data/scripts/fakepdf": "#!python\n"},
                             requires=('typing_extensions>=4.0; python_version < "3.11"', 'Pillow>=8.0.0; extra == "image"'))
    out = tmp_path / "tree"
    summary = build_tree.build(embed, wheel, out, extra_wheels=[extra])

    site = out / "Lib" / "site-packages"
    assert (site / "fakepdf" / "__init__.py").is_file() and (site / "fakepdf" / "reader.py").is_file()
    assert (site / "fakepdf-1.2.3.dist-info" / "METADATA").is_file()
    assert not list(site.glob("*.data")), "entry-point scripts of extra wheels must not be installed"
    assert (out / "LICENSE-fakepdf.txt").read_text() == "fakepdf licence text\n"
    assert (out / "LICENSE-opencaselaw-cli.txt").is_file()
    [recorded] = summary["extra_wheels"]
    assert recorded["file"] == "fakepdf-1.2.3-py3-none-any.whl" and recorded["distribution"] == "fakepdf" and recorded["version"] == "1.2.3"
    assert recorded["sha256"] == __import__("hashlib").sha256(extra.read_bytes()).hexdigest()
    assert recorded["top_level"] == ["fakepdf"] and recorded["files"] == 6 and recorded["licence"] == "LICENSE-fakepdf.txt"
    assert recorded["requires"] == ['typing_extensions>=4.0; python_version < "3.11"']  # the extra-only Pillow is not a requirement
    on_disk = json.loads((out / "TREE.json").read_text(encoding="utf-8"))
    assert on_disk["extra_wheels"] == [recorded] and on_disk["wheel"]["file"] == wheel.name
    assert summary["package_files"] and all(f.startswith("opencaselaw_cli/") or "/" not in f for f in summary["package_files"])

    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}
    env["PYTHONPATH"] = str(site)
    result = subprocess.run([sys.executable, "-s", "-c", "import fakepdf, fakepdf.reader, opencaselaw_cli; print(fakepdf.__version__, fakepdf.reader.pages())"],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0 and result.stdout.split() == ["1.2.3", "1"], result.stderr


def test_extra_wheels_from_the_command_line(tmp_path, capsys):
    build_tree, wheel, embed = _inputs(tmp_path)
    one = _fake_pure_wheel(tmp_path / "one-1.0-py3-none-any.whl", "one", "1.0", {"one/__init__.py": ""})
    two = _fake_pure_wheel(tmp_path / "two-2.0-py2.py3-none-any.whl", "two", "2.0", {"two.py": "VALUE = 2\n"}, requires=("one>=1.0",))
    code = build_tree.main(["--embed-zip", str(embed), "--wheel", str(wheel), "--out", str(tmp_path / "tree"),
                            "--extra-wheel", str(one), "--extra-wheel", str(two)])
    assert code == 0
    printed = json.loads(capsys.readouterr().out)
    assert [e["file"] for e in printed["extra_wheels"]] == ["one-1.0-py3-none-any.whl", "two-2.0-py2.py3-none-any.whl"]
    assert (tmp_path / "tree" / "Lib" / "site-packages" / "two.py").is_file()


def test_extra_wheels_that_are_not_pure_or_not_safe_are_refused(tmp_path):
    import pytest
    build_tree, wheel, embed = _inputs(tmp_path)
    tree = tmp_path / "tree"
    files = {"fakepdf/__init__.py": ""}
    for name in ("fakepdf-1.0-cp313-cp313-win_amd64.whl", "fakepdf-1.0-py3-none-win_amd64.whl", "fakepdf-1.0-cp313-abi3-any.whl", "fakepdf-1.0.tar.gz"):
        with pytest.raises(SystemExit, match="not a pure wheel"):
            build_tree.build(embed, wheel, tree, extra_wheels=[_fake_pure_wheel(tmp_path / name, "fakepdf", "1.0", files)])
    binary = _fake_pure_wheel(tmp_path / "sneaky-1.0-py3-none-any.whl", "sneaky", "1.0", {"sneaky/__init__.py": "", "sneaky/_speed.pyd": "MZ"})
    with pytest.raises(SystemExit, match="not pure Python"):
        build_tree.build(embed, wheel, tree, extra_wheels=[binary])
    traversing = _fake_pure_wheel(tmp_path / "trav-1.0-py3-none-any.whl", "trav", "1.0", {"trav/__init__.py": "", "../escape.py": ""})
    with pytest.raises(SystemExit, match="refusing extra wheel entry"):
        build_tree.build(embed, wheel, tree, extra_wheels=[traversing])
    clobbering = _fake_pure_wheel(tmp_path / "clob-1.0-py3-none-any.whl", "clob", "1.0", {"opencaselaw_cli/cli.py": "print('owned')\n"})
    with pytest.raises(SystemExit, match="would overwrite"):
        build_tree.build(embed, wheel, tree, extra_wheels=[clobbering])
    empty = _fake_pure_wheel(tmp_path / "empty-1.0-py3-none-any.whl", "empty", "1.0", {})
    with pytest.raises(SystemExit, match="no importable package"):
        build_tree.build(embed, wheel, tree, extra_wheels=[empty])
    needy = _fake_pure_wheel(tmp_path / "needy-1.0-py3-none-any.whl", "needy", "1.0", {"needy/__init__.py": ""}, requires=("somelib>=2",))
    with pytest.raises(SystemExit, match="requires somelib, which no --extra-wheel provides"):
        build_tree.build(embed, wheel, tree, extra_wheels=[needy])
    from opencaselaw_cli._version import __version__
    with pytest.raises(SystemExit, match="pass it as --wheel"):
        build_tree.build(embed, wheel, tree, extra_wheels=[_fake_wheel(tmp_path / f"opencaselaw_cli-{__version__}-py3-none-any.whl", __version__)])
    # A clean build still works afterwards: every refusal happened before or during unpacking, and the tree is rebuilt from scratch.
    summary = build_tree.build(embed, wheel, tree)
    assert summary["extra_wheels"] == []

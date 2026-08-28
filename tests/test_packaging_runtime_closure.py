"""Keep local runtime imports inside the published distribution.

Source-tree tests can import an undeclared top-level module directly from the
checkout, while the same import fails after installing the wheel.  This test
walks every shipped Python source and requires each local absolute import to be
declared as a setuptools ``py-module``.
"""

from __future__ import annotations

import ast
from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def _absolute_import_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.partition(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imports.add(node.module.partition(".")[0])
    return imports


def test_declared_modules_cover_local_runtime_imports() -> None:
    config = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    setuptools = config["tool"]["setuptools"]
    declared = set(setuptools["py-modules"])
    local_modules = {
        path.stem
        for path in ROOT.glob("*.py")
        if not path.name.startswith("test_")
    }

    shipped_sources = {ROOT / f"{module}.py" for module in declared}
    include_patterns = setuptools["packages"]["find"]["include"]
    for pattern in include_patterns:
        package_root = ROOT / pattern.removesuffix("*")
        shipped_sources.update(package_root.rglob("*.py"))

    missing: dict[str, list[str]] = {}
    for source in sorted(shipped_sources):
        for imported in sorted(_absolute_import_roots(source) & local_modules - declared):
            missing.setdefault(imported, []).append(str(source.relative_to(ROOT)))

    assert not missing, (
        "local runtime imports are absent from tool.setuptools.py-modules: "
        + "; ".join(
            f"{module} (imported by {', '.join(importers)})"
            for module, importers in sorted(missing.items())
        )
    )

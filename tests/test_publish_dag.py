"""Tests for publish_dag.py — the DAG runner for the publish pipeline.

Phase B v0.1 — covers:
  • Topological sort correctness (deps respected, stable ordering)
  • Cycle detection
  • Unknown-dep rejection
  • Closure (rebuild only what's needed)
  • Cascade-skip semantics (failure propagation through deps)
  • non_fatal short-circuit (failed dep doesn't cascade)
  • opt_in skip behaviour
  • The shipped REGISTRY validates cleanly
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

# Tests live alongside the repo root so just import the module directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import publish_dag  # noqa: E402
from publish_dag import (  # noqa: E402
    FAILED,
    OK,
    REGISTRY,
    SKIPPED_CASCADE,
    SKIPPED_OPTIN,
    Target,
    closure,
    run_targets,
    topological_sort,
    validate_dag,
)


# ── Helpers ────────────────────────────────────────────────────────────


def _mk(*, deps=None, **kw) -> Target:
    """Compact factory for one-off Target objects in tests."""
    return Target(name=kw.pop("name"), deps=deps or [], **kw)


def _registry(*targets: Target) -> dict[str, Target]:
    return {t.name: t for t in targets}


# ── Topological sort ───────────────────────────────────────────────────


def test_topo_sort_linear_chain() -> None:
    reg = _registry(
        _mk(name="a"),
        _mk(name="b", deps=["a"]),
        _mk(name="c", deps=["b"]),
    )
    assert topological_sort(reg) == ["a", "b", "c"]


def test_topo_sort_diamond() -> None:
    # a → {b, c} → d
    reg = _registry(
        _mk(name="a"),
        _mk(name="b", deps=["a"]),
        _mk(name="c", deps=["a"]),
        _mk(name="d", deps=["b", "c"]),
    )
    order = topological_sort(reg)
    # a before b, c, d ; b, c before d
    assert order.index("a") < order.index("b")
    assert order.index("a") < order.index("c")
    assert order.index("b") < order.index("d")
    assert order.index("c") < order.index("d")


def test_topo_sort_stable_alphabetical_tiebreak() -> None:
    reg = _registry(
        _mk(name="zulu"),
        _mk(name="alpha"),
        _mk(name="mike"),
    )
    # No deps → all roots → alphabetical
    assert topological_sort(reg) == ["alpha", "mike", "zulu"]


def test_topo_sort_rejects_cycle() -> None:
    reg = _registry(
        _mk(name="a", deps=["b"]),
        _mk(name="b", deps=["a"]),
    )
    try:
        topological_sort(reg)
    except ValueError as e:
        assert "cyclic" in str(e).lower()
    else:
        raise AssertionError("expected ValueError on cyclic graph")


def test_topo_sort_rejects_unknown_dep() -> None:
    reg = _registry(_mk(name="a", deps=["nonexistent"]))
    try:
        topological_sort(reg)
    except ValueError as e:
        assert "unknown" in str(e).lower()
    else:
        raise AssertionError("expected ValueError on unknown dep")


# ── Closure ────────────────────────────────────────────────────────────


def test_closure_returns_only_requested_subgraph() -> None:
    reg = _registry(
        _mk(name="a"),
        _mk(name="b", deps=["a"]),
        _mk(name="c", deps=["a"]),
        _mk(name="d", deps=["b"]),
        _mk(name="e", deps=["c"]),
    )
    # Asking for d should rebuild a + b + d, NOT c or e.
    sub = closure(reg, ["d"])
    assert sub == ["a", "b", "d"]


def test_closure_dedups_overlapping_requests() -> None:
    reg = _registry(
        _mk(name="a"),
        _mk(name="b", deps=["a"]),
        _mk(name="c", deps=["b"]),
    )
    sub = closure(reg, ["b", "c"])
    assert sub == ["a", "b", "c"]


# ── Validation ─────────────────────────────────────────────────────────


def test_validate_dag_clean() -> None:
    reg = _registry(_mk(name="a"), _mk(name="b", deps=["a"]))
    ok, errs = validate_dag(reg)
    assert ok and errs == []


def test_validate_dag_reports_unknown_dep() -> None:
    reg = _registry(_mk(name="c", deps=["nonexistent"]))
    ok, errs = validate_dag(reg)
    assert not ok
    assert any("unknown" in e.lower() for e in errs)


def test_validate_dag_reports_cycle() -> None:
    reg = _registry(
        _mk(name="a", deps=["b"]),
        _mk(name="b", deps=["a"]),
    )
    ok, errs = validate_dag(reg)
    assert not ok
    assert any("cyclic" in e.lower() for e in errs)


# ── Runner: cascade-skip semantics ─────────────────────────────────────


def _builder_returning(value: bool):
    """Return a builder fn that always returns `value` and records calls."""
    calls: list[str] = []

    def b(args, *, dry_run=False, full_rebuild=False):
        calls.append("called")
        return value

    b.calls = calls  # type: ignore[attr-defined]
    return b


def test_run_targets_happy_path() -> None:
    reg = _registry(
        _mk(name="a"),
        _mk(name="b", deps=["a"]),
    )
    builders = {"a": _builder_returning(True), "b": _builder_returning(True)}
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    results = run_targets(reg, builders, args)
    assert results == {"a": OK, "b": OK}


def test_run_targets_failure_cascades_to_dependents() -> None:
    reg = _registry(
        _mk(name="a"),
        _mk(name="b", deps=["a"]),
        _mk(name="c", deps=["b"]),
        _mk(name="d", deps=["a"]),  # also depends on a; should also skip
    )
    builders = {
        "a": _builder_returning(False),  # fails
        "b": _builder_returning(True),
        "c": _builder_returning(True),
        "d": _builder_returning(True),
    }
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    results = run_targets(reg, builders, args)
    assert results["a"] == FAILED
    assert results["b"] == SKIPPED_CASCADE
    assert results["c"] == SKIPPED_CASCADE
    assert results["d"] == SKIPPED_CASCADE
    # b/c/d builders never called
    assert builders["b"].calls == []  # type: ignore[attr-defined]
    assert builders["c"].calls == []  # type: ignore[attr-defined]
    assert builders["d"].calls == []  # type: ignore[attr-defined]


def test_run_targets_non_fatal_failure_does_not_cascade() -> None:
    reg = _registry(
        _mk(name="a"),
        _mk(name="b", deps=["a"], non_fatal=True),  # non-fatal!
        _mk(name="c", deps=["b"]),
    )
    builders = {
        "a": _builder_returning(True),
        "b": _builder_returning(False),  # fails BUT non-fatal
        "c": _builder_returning(True),
    }
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    results = run_targets(reg, builders, args)
    assert results["a"] == OK
    assert results["b"] == FAILED
    assert results["c"] == OK  # ran despite b failing
    assert builders["c"].calls == ["called"]  # type: ignore[attr-defined]


def test_run_targets_independent_branches_isolated_on_failure() -> None:
    """Key property: failure in branch X must NOT skip branch Y when
    Y doesn't depend on X. This is the localised-cascade behaviour
    that today's GUARDED_STEPS global skip can't express."""
    reg = _registry(
        _mk(name="root"),
        _mk(name="branch_x", deps=["root"]),
        _mk(name="x_child", deps=["branch_x"]),
        _mk(name="branch_y", deps=["root"]),
        _mk(name="y_child", deps=["branch_y"]),
    )
    builders = {
        "root": _builder_returning(True),
        "branch_x": _builder_returning(False),  # x branch fails
        "x_child": _builder_returning(True),
        "branch_y": _builder_returning(True),
        "y_child": _builder_returning(True),
    }
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    results = run_targets(reg, builders, args)
    assert results["branch_x"] == FAILED
    assert results["x_child"] == SKIPPED_CASCADE
    # y branch runs to completion despite x failure
    assert results["branch_y"] == OK
    assert results["y_child"] == OK


# ── Opt-in semantics ───────────────────────────────────────────────────


def test_run_targets_skips_opt_in_unless_requested() -> None:
    reg = _registry(
        _mk(name="ingest", opt_in=True),
        _mk(name="build", deps=["ingest"]),
    )
    builders = {
        "ingest": _builder_returning(True),
        "build": _builder_returning(True),
    }
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    # Without --target=ingest, ingest skips; build runs (opt-in skip
    # counts as 'effectively succeeded' for cascade purposes).
    results = run_targets(reg, builders, args)
    assert results["ingest"] == SKIPPED_OPTIN
    assert results["build"] == OK
    assert builders["ingest"].calls == []  # type: ignore[attr-defined]


def test_run_targets_runs_opt_in_when_requested() -> None:
    reg = _registry(_mk(name="ingest", opt_in=True))
    builders = {"ingest": _builder_returning(True)}
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    results = run_targets(reg, builders, args, requested=["ingest"])
    assert results["ingest"] == OK


# ── Builders missing from the map are no-ops ──────────────────────────


def test_run_targets_missing_builder_treated_as_ok() -> None:
    reg = _registry(_mk(name="a"))
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    results = run_targets(reg, builder_map={}, args=args)
    assert results == {"a": OK}


# ── Builder exception handling ────────────────────────────────────────


def test_run_targets_builder_exception_is_failed() -> None:
    def boom(args, **kw):
        raise RuntimeError("kaboom")

    reg = _registry(_mk(name="a"))
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    results = run_targets(reg, {"a": boom}, args)
    assert results["a"] == FAILED


# ── Closure restricts the run ─────────────────────────────────────────


def test_run_targets_with_requested_runs_only_closure() -> None:
    reg = _registry(
        _mk(name="a"),
        _mk(name="b", deps=["a"]),
        _mk(name="c", deps=["a"]),  # NOT requested
    )
    builders = {
        "a": _builder_returning(True),
        "b": _builder_returning(True),
        "c": _builder_returning(True),
    }
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    results = run_targets(reg, builders, args, requested=["b"])
    assert "c" not in results
    assert results == {"a": OK, "b": OK}
    assert builders["c"].calls == []  # type: ignore[attr-defined]


# ── The shipped REGISTRY ──────────────────────────────────────────────


def test_shipped_registry_validates() -> None:
    """The REGISTRY shipped in publish_dag.py must form a valid DAG."""
    ok, errs = validate_dag(REGISTRY)
    assert ok, f"shipped REGISTRY has issues: {errs}"


def test_shipped_registry_topological_starts_with_ingest() -> None:
    order = topological_sort(REGISTRY)
    # ingest is the only root (everything else has at least build_fts5
    # somewhere upstream)
    assert order[0] == "ingest"


def test_shipped_registry_health_check_is_terminal() -> None:
    """health_check should have nothing depending on it."""
    health_dependents = [
        n for n, t in REGISTRY.items() if "health_check" in t.deps
    ]
    assert health_dependents == []


def test_shipped_registry_qc_gate_failure_skips_uploads_and_pushes() -> None:
    """Verify the dep graph captures the safety-critical relationship:
    if QC gate fails, HF upload and final git push must skip."""
    builders = {
        "build_fts5": _builder_returning(True),
        "stats_early": _builder_returning(True),
        "rss_feeds": _builder_returning(True),
        "qc_gate": _builder_returning(False),  # QC FAILS
        "release_manifest": _builder_returning(True),
        "git_push_early": _builder_returning(True),
        "enrich_quality": _builder_returning(True),
        "anwaltsrecht_tags": _builder_returning(True),
        "quality_report": _builder_returning(True),
        "reference_graph": _builder_returning(True),
        "materialien_build": _builder_returning(True),
        "decision_structure": _builder_returning(True),
        "export_parquet": _builder_returning(True),
        "upload_hf": _builder_returning(True),
        "publish_delta": _builder_returning(True),
        "stats_final": _builder_returning(True),
        "git_push_final": _builder_returning(True),
        "health_check": _builder_returning(True),
    }
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    results = run_targets(REGISTRY, builders, args)
    assert results["qc_gate"] == FAILED
    # release_manifest depends on qc_gate → cascades
    assert results["release_manifest"] == SKIPPED_CASCADE
    # upload_hf depends on qc_gate → cascades (the safety-critical case)
    assert results["upload_hf"] == SKIPPED_CASCADE
    # git_push_final depends on qc_gate → cascades
    assert results["git_push_final"] == SKIPPED_CASCADE
    # The data-build branch (enrich_quality / reference_graph / etc.)
    # is independent of qc_gate, so it should run to completion. This
    # is the localised-cascade win over today's global GUARDED_STEPS.
    assert results["enrich_quality"] == OK
    assert results["reference_graph"] == OK
    assert results["materialien_build"] == OK
    assert results["decision_structure"] == OK


def test_shipped_registry_build_fts5_failure_cascades_widely() -> None:
    """If build_fts5 fails, everything downstream must skip — there's
    no decisions.db to read from."""
    builders = {name: _builder_returning(True) for name in REGISTRY}
    builders["build_fts5"] = _builder_returning(False)  # fails
    args = SimpleNamespace(dry_run=False, full_rebuild=False)
    results = run_targets(REGISTRY, builders, args)
    assert results["build_fts5"] == FAILED
    # Every target other than ingest (which is opt-in) and build_fts5
    # itself depends transitively on build_fts5 → all skip.
    for name, status in results.items():
        if name in ("build_fts5", "ingest"):
            continue
        assert status == SKIPPED_CASCADE, (
            f"{name} did not cascade-skip on build_fts5 failure (got {status})"
        )

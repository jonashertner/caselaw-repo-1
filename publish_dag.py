"""DAG-based runner for the OpenCaseLaw publish pipeline.

Replaces the linear for-loop + cascade-skip globals in publish.py with
an explicit dependency graph. Each target declares its inputs; the
runner topologically sorts and runs them; failures cascade *only* down
the dependency chain instead of triggering a global GUARDED-step skip.

Phase B v0.1 (2026-05-05): standalone module, NOT yet wired into
publish.py. Use this CLI to inspect and validate the DAG today; the
next commit will add `OCL_USE_DAG=1 python publish.py` fallthrough so
publish.py optionally hands its STEPS over to this runner without
changing default behaviour.

Why phase B exists: every recent publish failure (2026-05-04 build_fts5
timeout, ENOSPC, optimize-stall) cascaded the entire downstream chain.
The cascade is global today (CRITICAL_STEPS / GUARDED_STEPS sets in
publish.py); the DAG localises it — only the targets that genuinely
depend on the failed step skip, not every "guarded" step in the file.

CLI:
  python publish_dag.py --validate     # cycle + dep-resolution check
  python publish_dag.py --list         # show every target with its deps
  python publish_dag.py --dry-run      # print the topological order
  python publish_dag.py --target X     # show the rebuild closure of X
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger("publish_dag")


# ── Target schema ─────────────────────────────────────────────────────


@dataclass
class Target:
    """One node in the build DAG.

    A target is the unit of rebuild — typically the production of one
    output file (decisions.db, reference_graph.db, hf_dataset/, …) or
    one side-effect action (git push, health check). Each target lists
    the *names* of upstream targets it consumes; the runner topo-sorts
    and propagates failure only along those edges.

    Attributes:
        name: unique identifier within the registry; used in deps lists.
        deps: names of targets that must succeed before this one runs.
            If any dep failed AND that dep is not ``non_fatal``, this
            target is cascade-skipped.
        non_fatal: when True, a failure of this target does NOT cascade
            to downstream targets that depend on it (they still run).
            Mirrors publish.py's NON_FATAL_STEPS today.
        parallel_safe: marker for a future parallel scheduler — siblings
            (same depth in the DAG) that are all parallel_safe can run
            concurrently. v0.1 runner is sequential; the flag is captured
            but unused.
        opt_in: only run when explicitly requested via --target. Used
            for ingest steps that we don't want to fire on every cycle.
        description: human-readable label, shown in --list output.
    """

    name: str
    deps: list[str] = field(default_factory=list)
    non_fatal: bool = False
    parallel_safe: bool = False
    opt_in: bool = False
    description: str = ""


# Result statuses returned by run_targets.
OK = True
FAILED = False
SKIPPED_CASCADE = "skipped_cascade"
SKIPPED_OPTIN = "skipped_optin"


# ── Registry ──────────────────────────────────────────────────────────
#
# Mirrors today's publish.py STEPS list but with explicit deps and
# semantic names instead of step numbers. Numbers are noted in the
# description so the cross-reference is obvious during the wiring
# transition. NO BUILDER FUNCTIONS HERE — the registry is pure metadata
# so this module stays decoupled from publish.py and its dependencies.
# When publish.py wires the runner in, it will pass its own builder map.

REGISTRY: dict[str, Target] = {}


def _register(t: Target) -> None:
    if t.name in REGISTRY:
        raise ValueError(f"duplicate target {t.name!r} in registry")
    REGISTRY[t.name] = t


_register(Target(
    name="ingest",
    opt_in=True,
    description="Step 1 — pull entscheidsuche shards (manual / weekly only)",
))

_register(Target(
    name="build_fts5",
    deps=["ingest"],
    description="Step 2 — full FTS5 rebuild over decisions.db (atomic swap)",
))

# ── Fast tier: site shows today's date immediately after build_fts5 ──

_register(Target(
    name="stats_early",
    deps=["build_fts5"],
    description="Step 5a — early stats.json (kicks off the homepage update)",
))

_register(Target(
    name="rss_feeds",
    deps=["build_fts5"],
    description="Step 5b — generate decision RSS feeds",
))

_register(Target(
    name="qc_gate",
    deps=["build_fts5"],
    description="Step 5c — quality-control gate (blocks publish on CRITICAL regression)",
))

_register(Target(
    name="release_manifest",
    deps=["qc_gate"],
    non_fatal=True,
    description="Step 5d — release manifest (audit trail; failure doesn't block push)",
))

_register(Target(
    name="git_push_early",
    deps=["stats_early", "rss_feeds", "qc_gate", "release_manifest"],
    description="Step 6a — early git push of stats + feeds + manifest",
))

# ── Slow tier: enrichment, graphs, exports ──

_register(Target(
    name="enrich_quality",
    deps=["build_fts5"],
    description="Step 2d — quality enrichment of decisions.db rows",
))

_register(Target(
    name="anwaltsrecht_tags",
    deps=["enrich_quality"],
    parallel_safe=True,
    non_fatal=True,
    description="Step 2e — Anwaltsrecht topic tags (best-effort)",
))

_register(Target(
    name="quality_report",
    deps=["enrich_quality"],
    parallel_safe=True,
    description="Step 2b — quality_report.json + dashboard data",
))

_register(Target(
    name="reference_graph",
    deps=["enrich_quality"],
    parallel_safe=True,
    description="Step 2c — citation + statute reference graph (~75 min)",
))

_register(Target(
    name="materialien_build",
    deps=["enrich_quality"],
    parallel_safe=True,
    description="Step 2f — materialien.db rebuild from raw scrape stores",
))

_register(Target(
    name="decision_structure",
    deps=["enrich_quality"],
    parallel_safe=True,
    description="Step 2g — Sachverhalt/Erwägungen/Dispositiv sidecar (~75 min)",
))

_register(Target(
    name="export_parquet",
    deps=["enrich_quality"],
    parallel_safe=True,
    description="Step 3 — export decisions.db → dataset/*.parquet",
))

_register(Target(
    name="upload_hf",
    deps=["export_parquet", "qc_gate"],
    description="Step 4 — push parquet shards to HuggingFace dataset mirror",
))

_register(Target(
    name="publish_delta",
    deps=["build_fts5"],
    description="Step 7 — publish delta JSONL bundle (env-gated)",
))

_register(Target(
    name="stats_final",
    deps=["reference_graph"],
    description="Step 5 — final stats (includes citation-graph counts)",
))

_register(Target(
    name="git_push_final",
    deps=["stats_final", "qc_gate"],
    description="Step 6 — final git push of stats + dashboard",
))

_register(Target(
    name="health_check",
    deps=["git_push_final"],
    description="Step 6b — post-publish smoke check (alerts on failure)",
))


# ── Topological sort ──────────────────────────────────────────────────


def topological_sort(targets: dict[str, Target]) -> list[str]:
    """Kahn's algorithm. Stable: ties broken alphabetically so the same
    DAG always produces the same order.

    Raises ValueError on cycle or unknown dep.
    """
    in_count: dict[str, int] = {name: 0 for name in targets}
    for name, t in targets.items():
        for dep in t.deps:
            if dep not in targets:
                raise ValueError(f"target {name!r} depends on unknown {dep!r}")
        in_count[name] = len(t.deps)

    # Reverse adjacency: who depends on me?
    dependents: dict[str, list[str]] = defaultdict(list)
    for name, t in targets.items():
        for dep in t.deps:
            dependents[dep].append(name)

    ready: list[str] = sorted([n for n, c in in_count.items() if c == 0])
    order: list[str] = []

    while ready:
        ready.sort()
        current = ready.pop(0)
        order.append(current)
        for dep_name in dependents[current]:
            in_count[dep_name] -= 1
            if in_count[dep_name] == 0:
                ready.append(dep_name)

    if len(order) != len(targets):
        unresolved = [n for n in targets if n not in order]
        raise ValueError(
            f"cyclic dependency detected; unresolved targets: {unresolved}"
        )
    return order


def closure(targets: dict[str, Target], requested: list[str]) -> list[str]:
    """Return the topological order of `requested` plus all transitive
    dependencies. Useful for `make X` semantics — rebuild the minimal
    subgraph needed to produce X.
    """
    keep: set[str] = set()

    def visit(name: str) -> None:
        if name in keep:
            return
        if name not in targets:
            raise ValueError(f"unknown target {name!r}")
        keep.add(name)
        for dep in targets[name].deps:
            visit(dep)

    for r in requested:
        visit(r)
    return [n for n in topological_sort(targets) if n in keep]


# ── Validation ────────────────────────────────────────────────────────


def validate_dag(targets: dict[str, Target]) -> tuple[bool, list[str]]:
    """Returns (ok, errors). Checks for unknown deps and cycles."""
    errors: list[str] = []
    for name, t in targets.items():
        for dep in t.deps:
            if dep not in targets:
                errors.append(f"target {name!r} depends on unknown {dep!r}")
    try:
        topological_sort(targets)
    except ValueError as e:
        errors.append(str(e))
    return (not errors), errors


# ── Runner ────────────────────────────────────────────────────────────


def run_targets(
    targets: dict[str, Target],
    builder_map: dict[str, Callable],
    args,
    *,
    requested: list[str] | None = None,
) -> dict[str, bool | str]:
    """Run targets in topological order. Cascade-skip when a non-non_fatal
    dep failed.

    Args:
        targets: the registry to execute.
        builder_map: name → callable. The callable is invoked as
            ``builder(args, dry_run=…, full_rebuild=…) -> bool``. Targets
            with no entry in this map are treated as no-ops (logged as
            OK so dependent targets don't cascade-skip).
        args: argparse.Namespace forwarded to each builder.
        requested: if given, run only these targets and their transitive
            dependencies. Otherwise run the entire registry.

    Returns:
        ``{target_name: True|False|"skipped_cascade"|"skipped_optin"}``.
    """
    order = closure(targets, requested) if requested else topological_sort(targets)

    results: dict[str, bool | str] = {}
    requested_set = set(requested or [])

    for name in order:
        tgt = targets[name]

        # Opt-in targets only run when explicitly requested.
        if tgt.opt_in and name not in requested_set:
            results[name] = SKIPPED_OPTIN
            logger.info(f"  {name}: SKIPPED (opt-in; pass --target to run)")
            continue

        # Cascade-skip if any dep failed AND that dep was not non_fatal.
        cascaded_from: str | None = None
        for dep in tgt.deps:
            dep_result = results.get(dep)
            if dep_result is True or dep_result == SKIPPED_OPTIN:
                continue
            if dep_result is False or dep_result == SKIPPED_CASCADE:
                if not targets[dep].non_fatal:
                    cascaded_from = dep
                    break
        if cascaded_from:
            results[name] = SKIPPED_CASCADE
            logger.warning(
                f"  {name}: SKIPPED (cascade from {cascaded_from!r})"
            )
            continue

        builder = builder_map.get(name)
        if builder is None:
            logger.info(f"  {name}: no builder registered — treating as OK")
            results[name] = OK
            continue

        start = time.time()
        try:
            ok = bool(builder(
                args,
                dry_run=getattr(args, "dry_run", False),
                full_rebuild=getattr(args, "full_rebuild", False),
            ))
        except Exception as exc:  # noqa: BLE001
            logger.error(f"  {name}: EXCEPTION — {exc}", exc_info=True)
            ok = False
        elapsed = time.time() - start
        status = "OK" if ok else "FAILED"
        logger.info(f"  {name}: → {status} ({elapsed:.1f}s)")
        results[name] = ok

    return results


# ── CLI ───────────────────────────────────────────────────────────────


def _print_target(name: str, t: Target) -> None:
    tags: list[str] = []
    if t.non_fatal:
        tags.append("non-fatal")
    if t.parallel_safe:
        tags.append("parallel-safe")
    if t.opt_in:
        tags.append("opt-in")
    tag_str = f"  [{', '.join(tags)}]" if tags else ""
    deps_str = f" ← {', '.join(t.deps)}" if t.deps else " (root)"
    print(f"  {name}{deps_str}{tag_str}")
    if t.description:
        print(f"      {t.description}")


def main() -> int:
    p = argparse.ArgumentParser(
        description="DAG runner for OpenCaseLaw publish pipeline (Phase B v0.1)"
    )
    p.add_argument("--validate", action="store_true",
                   help="Check the DAG (no cycles, all deps resolved).")
    p.add_argument("--list", action="store_true",
                   help="Print every registered target with its deps.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the topological order (or the rebuild "
                        "closure when --target is given).")
    p.add_argument("--target", action="append",
                   help="Restrict to this target and its ancestors. "
                        "Can be repeated. Implies --dry-run for v0.1.")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if args.validate:
        ok, errors = validate_dag(REGISTRY)
        if not ok:
            for e in errors:
                logger.error(e)
            return 1
        logger.info(
            f"DAG OK: {len(REGISTRY)} targets, no cycles, all deps resolved."
        )
        return 0

    if args.list:
        for name in topological_sort(REGISTRY):
            _print_target(name, REGISTRY[name])
        return 0

    if args.target:
        order = closure(REGISTRY, args.target)
        print(f"Rebuild closure for {args.target}:")
        for i, name in enumerate(order, 1):
            print(f"  {i:>2}. {name}")
        return 0

    if args.dry_run:
        order = topological_sort(REGISTRY)
        print("Topological order (full pipeline):")
        for i, name in enumerate(order, 1):
            print(f"  {i:>2}. {name}")
        return 0

    print(
        "publish_dag.py — Phase B v0.1 (registry only). "
        "Use --validate, --list, --dry-run, or --target to inspect.\n"
        "The actual pipeline still runs via publish.py; the next commit "
        "wires this runner in behind an OCL_USE_DAG=1 env flag."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

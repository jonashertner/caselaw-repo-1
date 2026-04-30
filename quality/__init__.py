"""OpenCaseLaw quality-control package.

Three-layer QC across function-level (pytest), dataset-level (this
package's check_* functions), and production-uptime (smoke.py) verifies
the corpus on every nightly publish and every push.

Mission-critical contract: a CRITICAL regression blocks the nightly
git push (publish.py Step 6c). A WARNING fires ntfy.sh and continues.
INFO is descriptive only.

Public surface:
  quality.types.CheckResult     — return type of every check
  quality.types.Severity        — CRITICAL | WARNING | INFO
  quality.runner.run()          — run all checks; returns aggregate
  quality.runner.run_critical() — fast subset (gate path)
  quality.cli                   — `python -m quality.cli run [...]`
"""
from quality.types import CheckResult, Severity

__all__ = ["CheckResult", "Severity"]

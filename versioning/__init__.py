"""Decision version history for OpenCaseLaw.

Design: docs/design/decision-versioning.md

The corpus serves one current text per decision (decisions.db, unchanged).
This package records what a decision said BEFORE it was replaced, so the
question "what did the official record say on date X" has a provable
answer. Superseded texts are stored as reverse diffs against the current
text, which keeps serving free and history cheap.
"""
from versioning.store import (  # noqa: F401
    VersionStore, apply_reverse_diff, classify_change, make_reverse_diff,
    content_hash,
)

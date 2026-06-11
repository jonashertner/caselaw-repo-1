from __future__ import annotations

import publish


def test_publish_weekly_enrichment_runs_before_quality_and_graph():
    steps = [num for num, _, _ in publish.STEPS]

    assert steps.index(2) < steps.index("2d")
    assert steps.index("2d") < steps.index("2b")
    assert steps.index("2d") < steps.index("2c")

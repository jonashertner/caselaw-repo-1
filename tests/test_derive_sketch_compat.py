"""Guard: the inline HLL in derive_cohorts_from_tier1.py and the shared
analytics_hll.HLL must produce byte-identical registers for the same input, so
a sketch persisted by derive is correctly readable + mergeable by the
active-client counter. If the two ever drift, windowed distinct counts silently
corrupt — this test fails loudly instead.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
for sub in ("scripts",):
    p = str(REPO / sub)
    if p not in sys.path:
        sys.path.insert(0, p)

from analytics_hll import HLL, serialize_registers  # noqa: E402

derive = importlib.import_module("derive_cohorts_from_tier1")


def test_inline_and_shared_hll_registers_match():
    a = derive.HLL(p=12)
    b = HLL(p=12)
    for i in range(2000):
        v = f"ip{i % 700}|ua{i % 13}|2026-06"
        a.add(v)
        b.add(v)
    assert a.p == b.p == 12
    assert bytes(a.registers) == bytes(b.registers)
    assert a.estimate() == b.estimate()


def test_derive_sketch_is_readable_by_counter():
    a = derive.HLL(p=12)
    for i in range(1500):
        a.add(f"cohort-{i}")
    blob = serialize_registers(a.registers, a.p)   # how derive persists it
    restored = HLL.deserialize(blob)               # how the counter reads it
    assert restored.estimate() == a.estimate()

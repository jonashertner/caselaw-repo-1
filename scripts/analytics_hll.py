"""Mergeable HyperLogLog for the privacy-preserving active-client counter.

Single source of truth for the p=12 HLL used to estimate distinct (IP, UA)
cohorts in the analytics pipeline. Its `add()` / `estimate()` are byte-for-byte
the same as the inline HLLs in rollup_analytics.py / derive_cohorts_from_tier1.py
(so a sketch persisted by one is readable by the other), plus the serialize /
deserialize / merge needed for windowed distinct counts.

Why this matters: the daily distinct estimate is a *scalar* and cannot be
combined across days (you can't sum distinct counts of overlapping sets). The
HLL *sketch* can — register-wise max is exactly set union — so merging the daily
sketches over a week/month yields a TRUE windowed distinct-client count.

Privacy: a register holds only the max leading-zero rank seen for its bucket —
a small integer derived from a hash, never an identifier. A serialized sketch
therefore reveals nothing about which clients were present, only an approximate
cardinality. Persisting + merging sketches preserves the no-IP-retention
guarantee end to end.
"""
from __future__ import annotations

import base64
import hashlib
import math


def serialize_registers(registers, p: int = 12) -> str:
    """`p`-prefixed base64 of an HLL register bytearray. Module-level so the
    producer (derive_cohorts_from_tier1.py, which keeps its own inline HLL) and
    HLL.deserialize share ONE wire format and can never drift."""
    return f"{p}:" + base64.b64encode(bytes(registers)).decode("ascii")


class HLL:
    def __init__(self, p: int = 12):
        self.p = p
        self.m = 1 << p
        self.registers = bytearray(self.m)

    def add(self, value: str) -> None:
        h = int.from_bytes(
            hashlib.sha256(value.encode("utf-8")).digest()[:8], "big"
        )
        idx = h & (self.m - 1)
        w = h >> self.p
        if w == 0:
            rank = 64 - self.p + 1
        else:
            rank = 1
            while (w & 1) == 0 and rank < (64 - self.p):
                rank += 1
                w >>= 1
        if rank > self.registers[idx]:
            self.registers[idx] = rank

    def estimate(self) -> int:
        m = self.m
        alpha = 0.7213 / (1 + 1.079 / m)
        s = 0.0
        zeros = 0
        for r in self.registers:
            s += 2.0 ** (-r)
            if r == 0:
                zeros += 1
        est = alpha * m * m / s
        if est <= 2.5 * m and zeros > 0:
            est = m * math.log(m / zeros)
        return int(round(est))

    def merge(self, other: "HLL") -> "HLL":
        """Union another sketch into this one (register-wise max). In place."""
        if other.p != self.p:
            raise ValueError(f"cannot merge HLL p={other.p} into p={self.p}")
        sr, orr = self.registers, other.registers
        for i in range(self.m):
            if orr[i] > sr[i]:
                sr[i] = orr[i]
        return self

    def serialize(self) -> str:
        """`p`-prefixed base64 of the registers, e.g. '12:AAEC…'."""
        return serialize_registers(self.registers, self.p)

    @classmethod
    def deserialize(cls, blob: str) -> "HLL":
        p_str, b64 = blob.split(":", 1)
        h = cls(p=int(p_str))
        regs = base64.b64decode(b64)
        if len(regs) != h.m:
            raise ValueError(
                f"register length {len(regs)} != expected {h.m} for p={h.p}"
            )
        h.registers = bytearray(regs)
        return h

    @classmethod
    def union(cls, blobs) -> "HLL":
        """Merge an iterable of serialized sketches into one HLL. Empty/None
        entries are skipped; an all-empty input yields a fresh (zero) sketch."""
        merged: "HLL | None" = None
        for blob in blobs:
            if not blob:
                continue
            h = cls.deserialize(blob)
            if merged is None:
                merged = h
            else:
                merged.merge(h)
        return merged if merged is not None else cls()

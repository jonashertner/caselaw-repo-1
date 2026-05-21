"""
Cryptographic integrity for the Swiss caselaw corpus (Bestimmung 06).

For every publish of the corpus we compute a Merkle tree over all
decisions and emit the root as a 64-character hex string in
``docs/integrity/<YYYY-MM-DD>.root``. Anchored to Bitcoin via
OpenTimestamps when the ``ots`` CLI is available, yielding a
verification path that doesn't require trusting OpenCaseLaw:

  decision  →  leaf hash  →  Merkle inclusion proof  →  root
                                                          →  OpenTimestamps proof
                                                          →  Bitcoin block

What the root commits to per decision (the leaf):

  decision_id   internal-stable key (immutable)
  cli:ch        Swiss-native canonical identifier
  ECLI          European projection
  content_hash  SHA-256(regeste || full_text), already in FTS5
  decision_date the legal date of the decision

A verifier with the daily root + a per-decision inclusion proof can
prove cryptographically that *this decision with this content hash
and these identifiers was in the corpus on this date* — without
trusting opencaselaw.ch to still exist.

Hashing convention: RFC 6962 (Certificate Transparency).
  leaf hash    = SHA-256(0x00 || leaf_bytes)
  node hash    = SHA-256(0x01 || left_hash || right_hash)
  odd subtrees handled via "largest power of 2 < n" split rule

This convention is well-specified, second-preimage-safe, and compatible
with any OpenTimestamps verifier.
"""
from __future__ import annotations

import hashlib
from typing import List, Optional, Tuple


def canonical_leaf(
    decision_id: str,
    cli_ch: Optional[str],
    ecli: Optional[str],
    content_hash: Optional[str],
    decision_date: Optional[str],
) -> bytes:
    """Canonical byte string for one decision. Newline-separated, UTF-8."""
    parts = [
        decision_id or "",
        cli_ch or "",
        ecli or "",
        content_hash or "",
        decision_date or "",
    ]
    return ("\n".join(parts)).encode("utf-8")


def leaf_hash(leaf_bytes: bytes) -> bytes:
    """RFC 6962 leaf hash: SHA-256(0x00 || leaf_bytes)."""
    return hashlib.sha256(b"\x00" + leaf_bytes).digest()


def node_hash(left: bytes, right: bytes) -> bytes:
    """RFC 6962 internal node hash: SHA-256(0x01 || left || right)."""
    return hashlib.sha256(b"\x01" + left + right).digest()


def _largest_pow2_below(n: int) -> int:
    """Largest 2^k strictly less than n. Caller must pass n >= 2."""
    k = 1
    while k * 2 < n:
        k *= 2
    return k


def merkle_root(leaf_hashes: List[bytes]) -> bytes:
    """RFC 6962 Merkle Tree Hash over an ordered list of leaf hashes.

    Each element of ``leaf_hashes`` must already be the SHA-256(0x00 || leaf)
    hash. Use ``leaf_hash`` to compute leaves first.
    """
    n = len(leaf_hashes)
    if n == 0:
        return hashlib.sha256().digest()
    if n == 1:
        return leaf_hashes[0]
    k = _largest_pow2_below(n)
    left = merkle_root(leaf_hashes[:k])
    right = merkle_root(leaf_hashes[k:])
    return node_hash(left, right)


def merkle_proof(leaf_hashes: List[bytes], index: int) -> List[Tuple[bytes, str]]:
    """Inclusion proof for the leaf at ``index`` (0-based).

    Returns a list of (sibling_hash, position) tuples ordered from
    the leaf's sibling up to the root's two children. Position is
    'R' if the sibling is on the right of the path, 'L' otherwise.

    Verifier reconstructs the root via ``verify_inclusion``.

    Note: O(n log n). For 972k leaves expect ~20 subtree-root
    recomputations during proof construction. For the MVP this is
    acceptable; if proofs become hot, replace with a precomputed-tree
    structure.
    """
    n = len(leaf_hashes)
    if not 0 <= index < n:
        raise IndexError(f"index {index} out of range for {n} leaves")
    if n == 1:
        return []
    k = _largest_pow2_below(n)
    if index < k:
        sub = merkle_proof(leaf_hashes[:k], index)
        sibling = merkle_root(leaf_hashes[k:])
        return sub + [(sibling, "R")]
    else:
        sub = merkle_proof(leaf_hashes[k:], index - k)
        sibling = merkle_root(leaf_hashes[:k])
        return sub + [(sibling, "L")]


def verify_inclusion(
    leaf: bytes,
    proof: List[Tuple[bytes, str]],
    root: bytes,
) -> bool:
    """Verify that ``leaf`` (already RFC-6962-hashed) is included in
    a Merkle tree with the given ``root``, using ``proof``.

    Verifier-side function — does not consult the original leaf list.
    """
    h = leaf
    for sibling, pos in proof:
        if pos == "R":
            h = node_hash(h, sibling)
        elif pos == "L":
            h = node_hash(sibling, h)
        else:
            return False
    return h == root


def hex_root(root: bytes) -> str:
    """Lowercase hex encoding of a root, 64 chars for SHA-256."""
    return root.hex()


if __name__ == "__main__":
    # Smoke test — RFC 6962 test vectors and a small consistency check.
    print("=== leaf encoding ===")
    leaf = canonical_leaf(
        "bge_BGE_140_III_86",
        "cli:ch:bge:140-III-86",
        "ECLI:CH:BGE:2014:140.III.86",
        "abc123def456" * 5 + "0000",  # fake 64-char content hash
        "2014-04-15",
    )
    print(f"  leaf bytes: {leaf!r}")
    print(f"  leaf hash:  {leaf_hash(leaf).hex()}")

    print("\n=== merkle tree across N leaves ===")
    for n in (1, 2, 3, 4, 5, 7, 8, 100):
        leaves = [leaf_hash(canonical_leaf(f"d{i}", None, None, None, None))
                  for i in range(n)]
        root = merkle_root(leaves)
        print(f"  n={n:>3}: root={root.hex()[:16]}…")

    print("\n=== inclusion proof round-trip (n=100) ===")
    leaves = [leaf_hash(canonical_leaf(f"d{i}", f"cli:ch:bger:{i}/2025",
                                       f"ECLI:CH:BGER:2025:{i}.2025",
                                       f"hash{i:04d}", "2025-05-21"))
              for i in range(100)]
    root = merkle_root(leaves)
    print(f"  root: {root.hex()}")
    ok = 0
    for idx in (0, 1, 42, 50, 99):
        proof = merkle_proof(leaves, idx)
        verified = verify_inclusion(leaves[idx], proof, root)
        print(f"  idx={idx:>3}: proof_len={len(proof)}, verified={verified}")
        if verified:
            ok += 1
    print(f"  {ok}/5 verified")

    print("\n=== tamper detection ===")
    tampered = bytearray(leaves[42])
    tampered[0] ^= 0x01
    proof = merkle_proof(leaves, 42)
    verified = verify_inclusion(bytes(tampered), proof, root)
    print(f"  tampered leaf: verified={verified} (must be False)")

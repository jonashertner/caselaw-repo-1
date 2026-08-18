"""Version store: reverse-diff history for decision texts.

Invariants this module must never break:

  1. decisions.db is not touched. The current text stays exactly where it
     is and keeps its shape, so the atomic-swap and immutable=1
     invariants are unaffected. History lives in a sidecar DB.
  2. Reconstruction is exact. Applying a stored reverse diff to the
     current text must reproduce the recorded content_hash, byte for
     byte. record_observation() verifies this before committing and
     refuses to store a diff that does not round-trip.
  3. Content hashing uses the SAME recipe as build_fts5._compute_content_hashes
     — SHA-256 of (regeste or "") + (full_text or "") — so hashes are
     comparable across the corpus and the Merkle root.
"""
from __future__ import annotations

import difflib
import json
import hashlib
import re
import sqlite3
import zlib
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS decision_versions (
  decision_id   TEXT    NOT NULL,
  version_no    INTEGER NOT NULL,
  content_hash  TEXT    NOT NULL,
  observed_at   TEXT    NOT NULL,
  superseded_at TEXT,
  reverse_diff  BLOB,
  char_delta    INTEGER,
  source_url    TEXT,
  merkle_leaf   TEXT,
  classification TEXT,
  PRIMARY KEY (decision_id, version_no)
);
CREATE INDEX IF NOT EXISTS idx_dv_hash ON decision_versions(content_hash);
CREATE INDEX IF NOT EXISTS idx_dv_observed ON decision_versions(observed_at);
CREATE INDEX IF NOT EXISTS idx_dv_current
  ON decision_versions(decision_id) WHERE superseded_at IS NULL;

-- Removal tombstones (governance): the content of a version may be
-- deleted on a court's request, but the fact of the removal is kept so
-- the audit trail never silently loses an entry.
CREATE TABLE IF NOT EXISTS version_removals (
  decision_id TEXT NOT NULL,
  version_no  INTEGER NOT NULL,
  removed_at  TEXT NOT NULL,
  authority   TEXT,
  reason      TEXT,
  PRIMARY KEY (decision_id, version_no)
);

-- Per-decision verification clock driving the monthly refresh rotation.
CREATE TABLE IF NOT EXISTS verification_log (
  decision_id  TEXT PRIMARY KEY,
  last_checked TEXT NOT NULL,
  last_changed TEXT,
  check_count  INTEGER NOT NULL DEFAULT 1
);
"""

_CITATION = re.compile(
    r"\b(?:BGE|ATF|DTF)\s+\d{1,3}\s+[IVX]+[ab]?\s+\d{1,4}"
    r"|\b\d{1,2}[A-Z]{0,2}[_ ]\d+/\d{2,4}\b"
    r"|\bArt\.\s*\d+[a-z]*\b")
_ANON = re.compile(r"\b(?:[A-Z]\.[_\s]*){1,3}|\bX\.\s*_+|\b_{3,}\b")


def content_hash(regeste: str | None, full_text: str | None) -> str:
    """SHA-256 over (regeste || full_text) — the recipe build_fts5 uses."""
    body = (regeste or "") + (full_text or "")
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


# Storage format for history. Deliberately NOT unified diff: that format
# cannot represent a missing trailing newline unambiguously, and parsing
# it back is the kind of fragility that silently corrupts an archive. We
# emit our own opcode list over LINES, where splitlines(keepends=True) is
# lossless for every input ("".join(lines) == text always), so the format
# is exact for any text including empty and newline-less ones.
#
#   ["=", i1, i2]        copy current_lines[i1:i2]
#   ["+", [lines, ...]]  literal lines from the older text
_DIFF_VERSION = 1


def make_reverse_diff(current: str, older: str) -> bytes:
    """Compressed opcode list that turns `current` back into `older`."""
    cur = current.splitlines(keepends=True)
    old = older.splitlines(keepends=True)
    ops: list = []
    sm = difflib.SequenceMatcher(a=cur, b=old, autojunk=False)
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            ops.append(["=", i1, i2])
        elif tag in ("replace", "insert"):
            ops.append(["+", old[j1:j2]])
        # "delete": nothing from `older`, so nothing to emit
    payload = json.dumps({"v": _DIFF_VERSION, "ops": ops},
                         ensure_ascii=False, separators=(",", ":"))
    return zlib.compress(payload.encode("utf-8"), 6)


def apply_reverse_diff(current: str, blob: bytes) -> str:
    """Reconstruct the older text exactly."""
    payload = json.loads(zlib.decompress(blob).decode("utf-8"))
    if payload.get("v") != _DIFF_VERSION:
        raise ValueError(f"unsupported diff version {payload.get('v')!r}")
    cur = current.splitlines(keepends=True)
    out: list[str] = []
    for op in payload["ops"]:
        if op[0] == "=":
            out.extend(cur[op[1]:op[2]])
        elif op[0] == "+":
            out.extend(op[1])
        else:
            raise ValueError(f"unknown opcode {op[0]!r}")
    return "".join(out)


def classify_change(old: str, new: str) -> str:
    """Coarse label for the review queue. Order matters: the most
    consequential classification wins, because the queue is triaged by a
    human who should see citation changes first."""
    if _CITATION.findall(old) != _CITATION.findall(new):
        return "citation_affecting"
    o_anon, n_anon = len(_ANON.findall(old)), len(_ANON.findall(new))
    if n_anon > o_anon:
        return "anonymisation"
    if " ".join(old.split()) != " ".join(new.split()):
        return "text_substantive"
    return "metadata_only"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class VersionStore:
    """Append-only history sidecar. Never writes to decisions.db."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # ── reads ────────────────────────────────────────────────────────
    def current_hash(self, decision_id: str) -> str | None:
        r = self.conn.execute(
            "SELECT content_hash FROM decision_versions "
            "WHERE decision_id=? AND superseded_at IS NULL", (decision_id,)
        ).fetchone()
        return r["content_hash"] if r else None

    def versions(self, decision_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM decision_versions WHERE decision_id=? "
            "ORDER BY version_no", (decision_id,)).fetchall()

    def reconstruct(self, decision_id: str, version_no: int,
                    current_text: str) -> str | None:
        r = self.conn.execute(
            "SELECT reverse_diff FROM decision_versions "
            "WHERE decision_id=? AND version_no=?",
            (decision_id, version_no)).fetchone()
        if r is None:
            return None
        if r["reverse_diff"] is None:       # the current version itself
            return current_text
        return apply_reverse_diff(current_text, r["reverse_diff"])

    def due_for_check(self, decision_ids: list[str], max_age_days: int = 30
                      ) -> list[str]:
        """Which of these have not been verified within the window."""
        if not decision_ids:
            return []
        cutoff = _now()[:10]
        seen = {}
        for chunk in range(0, len(decision_ids), 900):
            part = decision_ids[chunk:chunk + 900]
            ph = ",".join("?" * len(part))
            for row in self.conn.execute(
                    f"SELECT decision_id, last_checked FROM verification_log "
                    f"WHERE decision_id IN ({ph})", part):
                seen[row["decision_id"]] = row["last_checked"][:10]
        out = []
        for did in decision_ids:
            last = seen.get(did)
            if last is None:
                out.append(did)
                continue
            age = (datetime.fromisoformat(cutoff)
                   - datetime.fromisoformat(last)).days
            if age >= max_age_days:
                out.append(did)
        return out

    # ── writes ───────────────────────────────────────────────────────
    def record_observation(self, decision_id: str, *, regeste: str | None,
                           full_text: str | None,
                           previous_text: str | None = None,
                           source_url: str | None = None,
                           merkle_leaf: str | None = None,
                           observed_at: str | None = None) -> str:
        """Record what we just fetched. Returns 'unchanged', 'first_seen'
        or the classification of the change.

        `previous_text` is the text that was current before this fetch —
        i.e. what decisions.db holds. The store deliberately does NOT keep
        a copy of the current text (that would duplicate the corpus and
        risk the two drifting), so on a change the caller must supply it;
        without it the transition is still recorded, but the superseded
        text is unrecoverable and the row is flagged `text_unavailable`.

        On a change the previous text becomes a superseded version stored
        as a reverse diff against the new current text. The diff is
        verified to round-trip before anything is committed.
        """
        now = observed_at or _now()
        new_text = (regeste or "") + (full_text or "")
        new_hash = content_hash(regeste, full_text)

        cur = self.conn.execute(
            "SELECT version_no, content_hash FROM decision_versions "
            "WHERE decision_id=? AND superseded_at IS NULL",
            (decision_id,)).fetchone()

        if cur is None:
            self.conn.execute(
                "INSERT INTO decision_versions(decision_id, version_no, "
                "content_hash, observed_at, superseded_at, reverse_diff, "
                "char_delta, source_url, merkle_leaf, classification) "
                "VALUES (?,1,?,?,NULL,NULL,0,?,?,'first_seen')",
                (decision_id, new_hash, now, source_url, merkle_leaf))
            self._touch(decision_id, now, changed=False)
            self.conn.commit()
            return "first_seen"

        if cur["content_hash"] == new_hash:
            self._touch(decision_id, now, changed=False)
            self.conn.commit()
            return "unchanged"

        # Changed. The superseded text is whatever was current before this
        # fetch: the caller's previous_text (decisions.db). It is NOT
        # recoverable from this store — the current version is stored as a
        # hash only, by design — so without it we record the transition and
        # say plainly that the old text is gone.
        old_text = previous_text
        if old_text is not None and content_hash(None, old_text) != cur["content_hash"]:
            # The supplied text is not the version we have on record; storing
            # a diff against it would silently corrupt the chain.
            raise ValueError(
                f"previous_text hash mismatch for {decision_id}: caller "
                f"supplied text hashing to {content_hash(None, old_text)[:12]}, "
                f"store holds {cur['content_hash'][:12]}")

        diff = None
        if old_text is None:
            klass = "text_unavailable"
        else:
            klass = classify_change(old_text, new_text)
            diff = make_reverse_diff(new_text, old_text)
            if apply_reverse_diff(new_text, diff) != old_text:
                raise ValueError(
                    f"reverse diff does not round-trip for {decision_id}")

        nxt = cur["version_no"] + 1
        self.conn.execute(
            "UPDATE decision_versions SET superseded_at=?, reverse_diff=?, "
            "char_delta=?, classification=? "
            "WHERE decision_id=? AND version_no=?",
            (now, diff, len(new_text) - len(old_text or ""), klass,
             decision_id, cur["version_no"]))
        self.conn.execute(
            "INSERT INTO decision_versions(decision_id, version_no, "
            "content_hash, observed_at, superseded_at, reverse_diff, "
            "char_delta, source_url, merkle_leaf, classification) "
            "VALUES (?,?,?,?,NULL,NULL,0,?,?,NULL)",
            (decision_id, nxt, new_hash, now, source_url, merkle_leaf))
        self._touch(decision_id, now, changed=True)
        self.conn.commit()
        return klass

    def remove_version(self, decision_id: str, version_no: int, *,
                       authority: str, reason: str) -> None:
        """Governance: delete a version's CONTENT, keep the tombstone."""
        self.conn.execute(
            "UPDATE decision_versions SET reverse_diff=NULL "
            "WHERE decision_id=? AND version_no=?", (decision_id, version_no))
        self.conn.execute(
            "INSERT OR REPLACE INTO version_removals(decision_id, version_no, "
            "removed_at, authority, reason) VALUES (?,?,?,?,?)",
            (decision_id, version_no, _now(), authority, reason))
        self.conn.commit()

    # ── internals ────────────────────────────────────────────────────
    def _touch(self, decision_id: str, when: str, *, changed: bool) -> None:
        self.conn.execute(
            "INSERT INTO verification_log(decision_id, last_checked, "
            "last_changed, check_count) VALUES (?,?,?,1) "
            "ON CONFLICT(decision_id) DO UPDATE SET "
            "last_checked=excluded.last_checked, "
            "last_changed=COALESCE(excluded.last_changed, last_changed), "
            "check_count=check_count+1",
            (decision_id, when, when if changed else None))

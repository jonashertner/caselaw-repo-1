#!/usr/bin/env bash
# Re-link decision_structure.db to /mnt after a build that clobbered the
# symlink. Safe to run: idempotent — exits 0 immediately if already a
# symlink. Verifies byte-exact match before swapping. Run after any
# publish that finished without the post-2026-05-02 symlink-aware
# extract_decision_structure.py.
#
# Background: decision_structure.db (44 GB) MUST live on /mnt because
# /opt is on the 150 GB root disk and a 44 GB file there pushes /
# usage over the line during builds. See docs/disk_layout.md or the
# project memory for the full incident.
set -euo pipefail

REGULAR_FILE="/opt/caselaw/repo/output/decision_structure.db"
DATA_TARGET="/mnt/HC_Volume_104655575/output/decision_structure.db"
TMP_TARGET="${DATA_TARGET}.relinked"

if [ -L "$REGULAR_FILE" ]; then
  echo "Already a symlink — nothing to do."
  echo "  $REGULAR_FILE → $(readlink "$REGULAR_FILE")"
  exit 0
fi

if [ ! -f "$REGULAR_FILE" ]; then
  echo "ERROR: $REGULAR_FILE missing; aborting." >&2
  exit 1
fi

src_size=$(stat --format='%s' "$REGULAR_FILE")
if [ "$src_size" -lt 30000000000 ]; then
  echo "ERROR: $REGULAR_FILE is only $src_size bytes — too small to be the real DB. Aborting." >&2
  exit 1
fi

echo "Source: $REGULAR_FILE ($((src_size/1000000000)) GB)"
echo "Copying to $TMP_TARGET (10–15 min)..."
cp -p "$REGULAR_FILE" "$TMP_TARGET"

dst_size=$(stat --format='%s' "$TMP_TARGET")
if [ "$src_size" != "$dst_size" ]; then
  echo "ERROR: byte mismatch ($src_size vs $dst_size). Aborting; leaving original in place." >&2
  rm -f "$TMP_TARGET"
  exit 1
fi
echo "Bytes match: $src_size."

mv "$TMP_TARGET" "$DATA_TARGET"
rm "$REGULAR_FILE"
ln -s "$DATA_TARGET" "$REGULAR_FILE"

echo "Done. Now:"
ls -la "$REGULAR_FILE"
df -h / /mnt/HC_Volume_104655575 | head -3

#!/bin/bash
# Paragraph-embeddings top-up — armed 2026-08-22 by the maintenance session.
#
# Why: paragraph_embeddings.db was built 2026-05-13; ~48k decisions added since
# have no vectors, so find_relevant_erwaegung's semantic rescue silently
# degrades to lexical-only for the newest ~5% of the corpus.
#
# Why gated: the encode walks all of decision_structure.db (a full-table read,
# forbidden during the build window) and the live embeddings file is held by
# workers with immutable=1 (must never be written in place — copy, top-up,
# atomic rename).
#
# Timeline: waits for tonight's full publish AND the 20:00 incremental to
# finish (earliest 21:00 UTC), hard-stops at 03:15 UTC before the next build.
# The builder is resume-safe: rerunning this script continues where it left
# off (existing (decision_id, e_number) pairs are skipped).
set -u
LOG=/var/log/opencaselaw-embeddings-topup.log
exec >>"$LOG" 2>&1

REPO=/opt/caselaw/repo
LIVE=$(readlink -f "$REPO/output/paragraph_embeddings.db")
WORK="${LIVE}.topup"
START_DEADLINE=$(date -u -d "2026-08-23 03:00" +%s)
CUTOFF=$(date -u -d "2026-08-23 03:15" +%s)

echo "[$(date -u +%FT%TZ)] launcher armed (pid $$); live=$LIVE"

# ── gate ─────────────────────────────────────────────────────────────────────
while :; do
    now=$(date -u +%s)
    if [ "$now" -ge "$START_DEADLINE" ]; then
        echo "[$(date -u +%FT%TZ)] window missed (pipeline still busy at 03:00) — exiting; rerun tomorrow"
        exit 1
    fi
    # Window 21:00-03:00 UTC (the -lt 3 arm exists because the 2026-08-22
    # autobump deploy makes tonight's 20:00 incremental a ~5h double
    # bootstrap, ending ~01:00; the START_DEADLINE above still caps entry
    # at 03:00 and the CUTOFF still stops the encode at 03:15).
    H=$(date -u +%H)
    if [ ! -f /tmp/opencaselaw-publish.lock ] \
       && ! systemctl is-active --quiet opencaselaw-publish-incremental.service \
       && { [ "$H" -ge 21 ] || [ "$H" -lt 3 ]; }; then
        break
    fi
    sleep 300
done
echo "[$(date -u +%FT%TZ)] window open — copying live DB to working copy"

# ── copy (never write the immutable-held live file) ─────────────────────────
rm -f "$WORK"
cp "$LIVE" "$WORK" || { echo "copy failed"; exit 1; }
echo "[$(date -u +%FT%TZ)] copy done ($(du -h "$WORK" | cut -f1)) — encoding"

# ── encode (idle I/O, low CPU priority; --restart because the offset
#    watermark is meaningless across nightly structure rebuilds — the
#    existing-key filter does the real dedup) ────────────────────────────────
cd "$REPO"
nice -n 15 ionice -c 3 python3 -m search_stack.build_paragraph_embeddings \
    --structure-db output/decision_structure.db \
    --output-db "$WORK" \
    --restart --batch-size 64 &
BUILD_PID=$!

( while kill -0 "$BUILD_PID" 2>/dev/null; do
      if [ "$(date -u +%s)" -ge "$CUTOFF" ]; then
          echo "[$(date -u +%FT%TZ)] 03:15 cutoff — stopping encode (resume-safe)"
          kill "$BUILD_PID" 2>/dev/null
          break
      fi
      sleep 60
  done ) &
WATCHER=$!

wait "$BUILD_PID"; STATUS=$?
kill "$WATCHER" 2>/dev/null

# ── swap or keep for resume ─────────────────────────────────────────────────
if [ "$STATUS" -eq 0 ]; then
    mv -f "$WORK" "$LIVE"
    echo "[$(date -u +%FT%TZ)] SUCCESS — swapped into $LIVE"
    echo "  workers pick it up on the next semantic-rescue call (per-call connections, no restart needed)"
else
    echo "[$(date -u +%FT%TZ)] encode exited $STATUS — live DB untouched; partial work kept at $WORK"
    echo "  resume: rerun this script (or the builder against $WORK), then: mv -f $WORK $LIVE"
fi

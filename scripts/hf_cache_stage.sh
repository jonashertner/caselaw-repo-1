#!/usr/bin/env bash
# Part A+B — stage an mcp-readable OFFLINE HuggingFace cache for the pinpoint
# semantic model, prove it loads under the worker sandbox, then append the two
# env lines to .env.mcp. Run as root on the VPS. Touches NO running service:
# the env change is inert until the next worker restart (manual rolling
# restart, or publish.py's daily post-swap recycle at ~10:07 UTC).
#
# Idempotent: an already-staged cache (same refs/main) is reused, not copied
# again, so DO_ENV=0 re-runs the sandbox test on an already-staged host.
#
# Rollback: see docs/pinpoint_semantic_rollout.md "Deactivation" (gate on both
#           publish units first, then bash /opt/caselaw/repo/scripts/rolling_restart_workers.sh).
set -euo pipefail

MODEL_DIR=models--sentence-transformers--paraphrase-multilingual-MiniLM-L12-v2
SRC=/root/.cache/huggingface/hub/$MODEL_DIR
DST=/opt/caselaw/hf_cache
ENV=/opt/caselaw/repo/.env.mcp
BAK=/root/.env.mcp.bak-$(date -u +%F)-pre-hfcache
DO_ENV=${DO_ENV:-1}      # DO_ENV=0 → stage + test only, leave .env.mcp untouched

echo "== preflight"
[ -f "$SRC/refs/main" ] || { echo "source snapshot missing: $SRC"; exit 1; }
HASH=$(cat "$SRC/refs/main")
[ -f "$SRC/snapshots/$HASH/model.safetensors" ] || { echo "snapshot $HASH incomplete"; exit 1; }
staged=0
if [ -e "$DST" ]; then
  if [ -f "$DST/hub/$MODEL_DIR/refs/main" ] && [ "$(cat "$DST/hub/$MODEL_DIR/refs/main")" = "$HASH" ]; then
    staged=1
  else
    echo "$DST exists but $DST/hub/$MODEL_DIR/refs/main is missing or != $HASH — inspect/remove it first, refusing to overwrite"; exit 1
  fi
fi
if [ "$DO_ENV" = 1 ] && grep -qE '^HF_HOME=' "$ENV"; then
  echo "HF_HOME already set in $ENV — nothing to append (DO_ENV=0 re-runs the sandbox test only)"; exit 1
fi
id mcp >/dev/null
df -h /opt | tail -1

if [ "$staged" = 1 ]; then
  echo "== 1. cache already staged at $DST (refs/main = $HASH), skipping copy"
else
  echo "== 1. copy snapshot (cp -a keeps the relative snapshots/ -> blobs/ symlinks)"
  mkdir -p "$DST/hub"
  cp -a "$SRC" "$DST/hub/"
  chown -R mcp:mcp "$DST"
  chmod 755 "$DST" "$DST/hub"
fi
broken=$(find "$DST/hub/$MODEL_DIR/snapshots" -xtype l | wc -l)
[ "$broken" = 0 ] || { echo "$broken broken symlinks in the copy"; exit 1; }
[ "$(cat "$DST/hub/$MODEL_DIR/refs/main")" = "$HASH" ] || { echo "refs/main mismatch"; exit 1; }
du -sh "$DST"

echo "== 2. load test as mcp under the SAME sandbox as mcp-server@.service (read-only FS, offline)"
systemd-run --quiet --wait --pipe --collect \
  --uid=mcp --gid=mcp \
  -p ProtectSystem=strict -p PrivateTmp=true -p NoNewPrivileges=true \
  -p ReadWritePaths=/opt/caselaw/repo/output \
  -p ReadWritePaths=/opt/caselaw/repo/logs \
  -p ReadWritePaths=/mnt/HC_Volume_104655575 \
  -p WorkingDirectory=/opt/caselaw/repo \
  --setenv=HF_HOME="$DST" --setenv=HF_HUB_OFFLINE=1 \
  /usr/bin/python3 -c '
import os, time, resource
print("uid", os.getuid(), "HOME", os.environ.get("HOME"), "HF_HOME", os.environ.get("HF_HOME"), "HF_HUB_OFFLINE", os.environ.get("HF_HUB_OFFLINE"))
t = time.time()
from sentence_transformers import SentenceTransformer
m = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
print("dim", m.get_sentence_embedding_dimension(), "load_s", round(time.time() - t, 1))
e = m.encode(["Le législateur a maintenu le système du double délai de prescription"], normalize_embeddings=True)
print("encode ok", e.shape, "maxrss_mb", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024)
'
echo "   sandboxed load OK"

if [ "$DO_ENV" != 1 ]; then echo "== DO_ENV=0: leaving $ENV untouched"; exit 0; fi

echo "== 3. back up .env.mcp OUTSIDE the repo tree, then append"
cp -p "$ENV" "$BAK"
[ "$(tail -c1 "$ENV" | od -An -c | tr -d ' ')" = '\n' ] || echo >> "$ENV"
cat >> "$ENV" <<'EOT'

# 2026-09-03: workers run as User=mcp (no home dir, ProtectSystem=strict), so the
# HF cache under /root was unreadable and the pinpoint semantic model never
# loaded (journal: "pinpoint semantic model load failed: PermissionError").
# Offline snapshot copy, read-only for the workers; takes effect at the next
# worker restart. Rollback: delete these two lines + rolling restart.
HF_HOME=/opt/caselaw/hf_cache
HF_HUB_OFFLINE=1
EOT
ls -l "$ENV" "$BAK"
grep -nE '^HF_' "$ENV"
echo "== done. Workers still run the OLD env until restarted (see hf_cache_restart_verify.sh)."

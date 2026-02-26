#!/usr/bin/env bash
#
# scrape_ne_local.sh — Run ne_gerichte scraper locally and sync to VPS
#
# jurisprudence.ne.ch blocks Hetzner IPs, so this scraper must run from
# a non-blocked IP (e.g. your local Mac). This script:
#   1. Runs the ne_gerichte scraper locally
#   2. Syncs the JSONL + state files to the VPS
#
# Usage:
#   ./scripts/scrape_ne_local.sh           # daily incremental
#   ./scripts/scrape_ne_local.sh --max 10  # test with 10 decisions
#
# Prerequisites:
#   - SSH key at ~/.ssh/caselaw (VPS access)
#   - Python environment with dependencies installed locally
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VPS_HOST="root@46.225.212.40"
VPS_KEY="$HOME/.ssh/caselaw"
VPS_REPO="/opt/caselaw/repo"

JSONL="output/decisions/ne_gerichte.jsonl"
STATE="state/ne_gerichte.jsonl"

cd "$REPO_DIR"

echo "=== NE Gerichte local scraper ==="
echo "Running from: $REPO_DIR"
echo ""

# Step 1: Run the scraper locally
echo "[1/3] Running ne_gerichte scraper..."
python3 run_scraper.py ne_gerichte "$@" -v

# Step 2: Check if we have output to sync
if [ ! -f "$JSONL" ]; then
    echo "No JSONL output — nothing to sync."
    exit 0
fi

LOCAL_LINES=$(wc -l < "$JSONL" | tr -d ' ')
echo ""
echo "[2/3] Local JSONL: $LOCAL_LINES decisions"

# Step 3: Sync to VPS
echo "[3/3] Syncing to VPS..."

# Sync JSONL (append-only, so rsync is safe)
rsync -avz -e "ssh -i $VPS_KEY" \
    "$JSONL" \
    "$VPS_HOST:$VPS_REPO/$JSONL"

# Sync state file so VPS knows what's been scraped
if [ -f "$STATE" ]; then
    rsync -avz -e "ssh -i $VPS_KEY" \
        "$STATE" \
        "$VPS_HOST:$VPS_REPO/$STATE"
fi

# Verify remote
REMOTE_LINES=$(ssh -i "$VPS_KEY" "$VPS_HOST" "wc -l < $VPS_REPO/$JSONL" 2>/dev/null || echo "?")
echo ""
echo "=== Done ==="
echo "  Local:  $LOCAL_LINES decisions"
echo "  Remote: $REMOTE_LINES decisions"

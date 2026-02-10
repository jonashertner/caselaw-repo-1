#!/bin/bash
# Deploy ZH Gerichte scraper to VPS
# Run this on the VPS after uploading zh_gerichte.py and run_zh_gerichte.py
# to /opt/caselaw/repo/scrapers/cantonal/

set -e

REPO_DIR="/opt/caselaw/repo"
SCRAPER_DIR="$REPO_DIR/scrapers/cantonal"

echo "=== Deploying ZH Gerichte Scraper ==="

# 1. Ensure directory exists
mkdir -p "$SCRAPER_DIR"

# 2. Copy files (assumes they're in current directory)
cp zh_gerichte.py "$SCRAPER_DIR/"
cp run_zh_gerichte.py "$SCRAPER_DIR/"

# 3. Quick API probe
echo ""
echo "=== Probing gerichte-zh.ch API ==="
cd "$REPO_DIR/scrapers"
python3 -c "
import sys
sys.path.insert(0, '.')
sys.path.insert(0, '..')
from cantonal.run_zh_gerichte import probe_api
result = probe_api()
sys.exit(0 if result else 1)
" && echo "✅ API probe passed" || echo "❌ API probe failed"

# 4. Test run (first 5 decisions)
echo ""
echo "=== Test run (5 decisions) ==="
cd "$REPO_DIR/scrapers"
PYTHONPATH="$REPO_DIR/scrapers:$REPO_DIR" python3 cantonal/run_zh_gerichte.py \
    --max 5 \
    --state-dir "$REPO_DIR/state" \
    --output "$REPO_DIR/output/decisions/zh_gerichte.jsonl"

echo ""
echo "=== Test results ==="
if [ -f "$REPO_DIR/output/decisions/zh_gerichte.jsonl" ]; then
    LINES=$(wc -l < "$REPO_DIR/output/decisions/zh_gerichte.jsonl")
    echo "Decisions in JSONL: $LINES"
    echo "First entry preview:"
    head -1 "$REPO_DIR/output/decisions/zh_gerichte.jsonl" | python3 -m json.tool | head -20
else
    echo "No output file created yet"
fi

echo ""
echo "=== To start full scrape ==="
echo "Run in screen/tmux:"
echo "  cd $REPO_DIR/scrapers"
echo "  PYTHONPATH='$REPO_DIR/scrapers:$REPO_DIR' nohup python3 cantonal/run_zh_gerichte.py \\"
echo "      --state-dir $REPO_DIR/state \\"
echo "      --output $REPO_DIR/output/decisions/zh_gerichte.jsonl \\"
echo "      > $REPO_DIR/logs/zh_gerichte.log 2>&1 &"
echo ""
echo "Estimated runtime: ~8-12 hours for 20,000-30,000 decisions"

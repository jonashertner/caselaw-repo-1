#!/bin/bash
# deploy_incapsula_bypass.sh
# 
# Deploys Playwright-based Incapsula bypass to the VPS.
# Run from your Mac: bash deploy_incapsula_bypass.sh
#
# What this does:
# 1. Installs Playwright + Chromium on VPS (as root)
# 2. Copies modified scrapers + bypass module
# 3. Tests the bypass
# 4. Kills blocked BGer/BGE processes
# 5. Restarts with crash-safe run_scraper.py

set -e

VPS="root@46.225.79.22"
SSH="ssh -i ~/.ssh/caselaw"
SCP="scp -i ~/.ssh/caselaw"
REPO="/opt/caselaw/repo"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Step 1: Install Playwright system dependencies (as root) ==="
$SSH $VPS "
    apt-get update -qq
    apt-get install -y -qq \
        libnss3 libnspr4 libdbus-1-3 libatk1.0-0 libatk-bridge2.0-0 \
        libcups2 libdrm2 libxkbcommon0 libatspi2.0-0 libxcomposite1 \
        libxdamage1 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 \
        libcairo2 libasound2 2>/dev/null || \
    apt-get install -y -qq \
        libnss3 libnspr4 libdbus-1-3 libatk1.0-0t64 libatk-bridge2.0-0t64 \
        libcups2t64 libdrm2 libxkbcommon0 libatspi2.0-0t64 libxcomposite1 \
        libxdamage1 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 \
        libcairo2 libasound2t64 2>/dev/null || true
    echo 'System deps installed (some warnings are OK)'
"

echo ""
echo "=== Step 2: Install Python packages (as caselaw user) ==="
$SSH $VPS "su - caselaw -c '
    pip3 install playwright --break-system-packages 2>/dev/null || pip3 install playwright
    python3 -m playwright install chromium
    echo \"Playwright + Chromium installed\"
'"

echo ""
echo "=== Step 3: Copy modified files to VPS ==="
$SCP "$SCRIPT_DIR/incapsula_bypass.py" $VPS:$REPO/incapsula_bypass.py
$SCP "$SCRIPT_DIR/bger.py"             $VPS:$REPO/scrapers/bger.py
$SCP "$SCRIPT_DIR/bge.py"              $VPS:$REPO/scrapers/bge.py

# Fix ownership
$SSH $VPS "chown -R caselaw:caselaw $REPO/incapsula_bypass.py $REPO/scrapers/bger.py $REPO/scrapers/bge.py"
echo "Files copied"

echo ""
echo "=== Step 4: Test Incapsula bypass ==="
$SSH $VPS "su - caselaw -c '
    cd $REPO
    python3 -c \"
from incapsula_bypass import IncapsulaCookieManager
import requests

mgr = IncapsulaCookieManager()

# Test www.bger.ch
print(\\\"Testing www.bger.ch...\\\")
cookies = mgr.get_cookies(\\\"www.bger.ch\\\")
r = requests.get(\\\"https://www.bger.ch/ext/eurospider/live/de/php/aza/http/index.php?lang=de&type=simple_query&query_words=&top_subcollection_aza=all&from_date=01.01.2025&to_date=31.01.2025\\\", cookies=cookies, timeout=30)
blocked = mgr.is_incapsula_blocked(r.text)
print(f\\\"  www.bger.ch: status={r.status_code}, len={len(r.text)}, blocked={blocked}\\\")
if not blocked and len(r.text) > 500:
    print(\\\"  ✅ www.bger.ch WORKING\\\")
else:
    print(f\\\"  ❌ www.bger.ch STILL BLOCKED: {r.text[:200]}\\\")

# Test search.bger.ch
print(\\\"\\\\nTesting search.bger.ch...\\\")
cookies2 = mgr.get_cookies(\\\"search.bger.ch\\\")
r2 = requests.get(\\\"https://search.bger.ch/ext/eurospider/live/de/php/clir/http/index_atf.php?lang=de\\\", cookies=cookies2, timeout=30)
blocked2 = mgr.is_incapsula_blocked(r2.text)
print(f\\\"  search.bger.ch: status={r2.status_code}, len={len(r2.text)}, blocked={blocked2}\\\")
if not blocked2 and len(r2.text) > 500:
    print(\\\"  ✅ search.bger.ch WORKING\\\")
else:
    print(f\\\"  ❌ search.bger.ch STILL BLOCKED: {r2.text[:200]}\\\")
\"
'"

echo ""
echo "=== Step 5: Kill blocked processes ==="
$SSH $VPS "
    # Kill the BGer pipeline process (blocked by Incapsula)
    # Use exact match patterns to avoid killing BVGer
    pkill -f 'pipeline.py.*--courts bger' 2>/dev/null && echo 'Killed BGer pipeline' || echo 'No BGer pipeline running'
    # Kill the BGE/BStGer/BPatGer pipeline process (BGE blocked, BPatGer already done)
    pkill -f 'pipeline.py.*--courts bge,bstger,bpatger' 2>/dev/null && echo 'Killed BGE+BStGer pipeline' || echo 'No BGE pipeline running'
    sleep 2
    echo 'Remaining processes:'
    ps aux | grep -E 'scraper|pipeline' | grep -v grep || echo 'None'
"

echo ""
echo "=== Step 6: Restart with crash-safe persistence ==="
$SSH $VPS "su - caselaw -c '
    cd $REPO
    mkdir -p logs output/decisions
    
    # BGer backfill with crash-safe JSONL persistence
    nohup python3 run_scraper.py bger --since 2000-01-01 -v > logs/bger_run.log 2>&1 &
    echo \"Started BGer scraper (PID: \$!)\"
    
    # BGE backfill
    nohup python3 run_scraper.py bge -v > logs/bge_run.log 2>&1 &
    echo \"Started BGE scraper (PID: \$!)\"
    
    # BStGer (was in the killed process, not Incapsula-blocked)
    nohup python3 run_scraper.py bstger -v > logs/bstger_run.log 2>&1 &
    echo \"Started BStGer scraper (PID: \$!)\"
    
    sleep 5
    echo \"\"
    echo \"All running scrapers:\"
    ps aux | grep run_scraper | grep -v grep
    echo \"\"
    echo \"Early log check (BGer):\"
    tail -5 logs/bger_run.log 2>/dev/null || echo \"no output yet\"
'"

echo ""
echo "=== Done ==="
echo "Monitor with:"
echo "  ssh -i ~/.ssh/caselaw root@46.225.79.22 \"su - caselaw -c 'tail -20 /opt/caselaw/repo/logs/bger_run.log'\""
echo "  ssh -i ~/.ssh/caselaw root@46.225.79.22 \"su - caselaw -c 'wc -l /opt/caselaw/repo/output/decisions/bger.jsonl'\""
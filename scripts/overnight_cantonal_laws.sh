#!/bin/bash
# overnight_cantonal_laws.sh — Run all cantonal law scrapers and build DB
# Designed to run unattended overnight on VPS.
#
# Usage: nohup bash scripts/overnight_cantonal_laws.sh >> logs/cantonal_laws_overnight.log 2>&1 &

set -e
cd /opt/caselaw/repo

OUTPUT=/mnt/HC_Volume_104655575/output/cantonal_laws_direct
LEXFIND=/mnt/HC_Volume_104655575/output/lexfind_cantonal
DB_OUT=/mnt/HC_Volume_104655575/output/cantonal_laws.db
LOG=logs/cantonal_laws_overnight.log

echo "=== Overnight cantonal laws scrape started: $(date -u) ==="

# Wait for any running LexWork scrape to finish (max 2h)
echo "Checking for running scrape..."
for i in $(seq 1 240); do
    if pgrep -f "scrape_cantonal_laws" > /dev/null 2>&1; then
        echo "  Scrape still running (check $i/240)..."
        sleep 30
    else
        echo "  No running scrape found."
        break
    fi
done

# Phase 1: Run remaining cantons that weren't in the initial batch
echo ""
echo "=== Phase 1: BE (LexWork) ==="
python3 scrape_cantonal_laws.py --canton BE --output "$OUTPUT" 2>&1

echo ""
echo "=== Phase 2: NE (SIL) ==="
python3 scrape_cantonal_laws.py --canton NE --output "$OUTPUT" 2>&1

echo ""
echo "=== Phase 3: ZH (PDF) ==="
python3 scrape_cantonal_laws.py --canton ZH --output "$OUTPUT" 2>&1

# Phase 2: Verify all expected cantons have data
echo ""
echo "=== Verification ==="
for canton in AG AI AR BE BL BS FR GL GR LU NE NW OW SG SH SO TG VS ZG ZH; do
    f="$OUTPUT/$canton.jsonl"
    if [ -f "$f" ] && [ -s "$f" ]; then
        lines=$(wc -l < "$f")
        size=$(du -h "$f" | cut -f1)
        echo "  OK $canton: $lines laws, $size"
    else
        echo "  MISSING $canton"
    fi
done

# Phase 3: Build the database
echo ""
echo "=== Building cantonal_laws.db ==="
python3 -m search_stack.build_cantonal_laws_db \
    --input-direct "$OUTPUT" \
    --input-lexfind "$LEXFIND" \
    --output "$DB_OUT" 2>&1

# Phase 4: Symlink and restart MCP
echo ""
echo "=== Restarting MCP workers ==="
ln -sf "$DB_OUT" /opt/caselaw/repo/output/cantonal_laws.db
systemctl restart mcp-server@8770 mcp-server@8771 mcp-server@8772 mcp-server@8773

echo ""
echo "=== Done: $(date -u) ==="
echo "DB: $(ls -lh "$DB_OUT")"

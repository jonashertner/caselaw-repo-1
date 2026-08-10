#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo"

run_dir="${CODEX_MAINTENANCE_RUN_DIR:-logs/agent-loop}"
mkdir -p "$run_dir"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
assessment="$run_dir/assessment-$ts.json"
decision="$run_dir/decision-$ts.json"

python3 scripts/agent_assess.py --json > "$assessment"

prompt="$(cat <<PROMPT
\$opencaselaw-maintenance

Use the assessment JSON at $assessment and the policy at ops/autonomy-policy.json.
Pick exactly one highest-value safe action. If the action is gated or human-only,
write a proposal/record and stop. If it is safe, implement the minimal change,
run relevant tests, and record evidence. Return only JSON matching
schemas/agent_decision.schema.json.
PROMPT
)"

codex exec \
  --sandbox workspace-write \
  --output-schema schemas/agent_decision.schema.json \
  -o "$decision" \
  "$prompt"

echo "assessment=$assessment"
echo "decision=$decision"

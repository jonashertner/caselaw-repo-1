#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STAGE_DIR="$ROOT_DIR/.mcpb-build/swiss-caselaw-local"
OUT_DIR="$ROOT_DIR/dist"
OUT_FILE="$OUT_DIR/swiss-caselaw-local.mcpb"
DEFAULT_VENV_PY="$ROOT_DIR/.venv/bin/python3"
FALLBACK_PY="$(command -v python3 || true)"

mkdir -p "$STAGE_DIR" "$OUT_DIR"
rm -f "$OUT_FILE"

if [[ -x "$DEFAULT_VENV_PY" ]]; then
  PY_CMD="$DEFAULT_VENV_PY"
else
  PY_CMD="$FALLBACK_PY"
fi

if [[ -z "$PY_CMD" ]]; then
  echo "ERROR: python3 not found. Install Python and/or create .venv first." >&2
  exit 1
fi

PY_CMD="$PY_CMD" ROOT_DIR="$ROOT_DIR" STAGE_DIR="$STAGE_DIR" "$PY_CMD" - <<'PY'
import json
import os
from pathlib import Path

root = Path(os.environ["ROOT_DIR"])
stage = Path(os.environ["STAGE_DIR"])
manifest = json.loads((root / "desktop_extension" / "manifest.json").read_text(encoding="utf-8"))
manifest["server"]["mcp_config"]["command"] = os.environ["PY_CMD"]
(stage / "manifest.json").write_text(
    json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
PY

cp "$ROOT_DIR/mcp_server.py" "$STAGE_DIR/mcp_server.py"
cp "$ROOT_DIR/db_schema.py" "$STAGE_DIR/db_schema.py"

npx -y @anthropic-ai/mcpb validate "$STAGE_DIR/manifest.json"
npx -y @anthropic-ai/mcpb pack "$STAGE_DIR" "$OUT_FILE"

echo "Built extension: $OUT_FILE"
echo "Python command configured in bundle: $PY_CMD"
echo "Install in Claude Desktop: Settings -> Extensions -> Advanced settings -> Install Extension..."

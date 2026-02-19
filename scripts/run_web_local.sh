#!/usr/bin/env bash
# Start the Swiss Case Law web UI locally.
# Usage: ./scripts/run_web_local.sh

set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Load .env if present
if [ -f .env ]; then
  echo "Loading .env..."
  set -a; source .env; set +a
fi

BACKEND_PORT="${BACKEND_PORT:-8910}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"

# Check Python deps
echo "Checking Python dependencies..."
python3 -c "import fastapi, uvicorn, dotenv, mcp" 2>/dev/null || {
  echo "Installing Python dependencies..."
  pip3 install fastapi uvicorn python-dotenv mcp pyarrow pydantic
}

# Check at least one provider SDK
python3 -c "
has_any = False
try:
    import openai; has_any = True
except ImportError: pass
try:
    import anthropic; has_any = True
except ImportError: pass
try:
    import google.genai; has_any = True
except ImportError: pass
if not has_any:
    print('WARNING: No LLM provider SDK found.')
    print('Install at least one: pip install openai anthropic google-genai')
" 2>/dev/null

# Check Node.js / npm
if ! command -v npm &>/dev/null; then
  echo "ERROR: npm not found. Install Node.js (https://nodejs.org)"
  exit 1
fi

# Install frontend deps if needed
if [ ! -d web_ui/node_modules ]; then
  echo "Installing frontend dependencies..."
  (cd web_ui && npm install)
fi

echo ""
echo "Starting backend on http://127.0.0.1:${BACKEND_PORT}"
echo "Starting frontend on http://127.0.0.1:${FRONTEND_PORT}"
echo ""

# Start backend in background
python3 -m uvicorn web_api.main:app \
  --host 127.0.0.1 \
  --port "$BACKEND_PORT" \
  --log-level info &
BACKEND_PID=$!

# Start frontend
(cd web_ui && npm run dev -- --host 127.0.0.1 --port "$FRONTEND_PORT") &
FRONTEND_PID=$!

# Cleanup on exit
cleanup() {
  echo ""
  echo "Shutting down..."
  kill "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null
  wait "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null
}
trap cleanup EXIT INT TERM

echo "Press Ctrl+C to stop."
wait

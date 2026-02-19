# Swiss Case Law — Local Web UI

Local-only web interface for searching Swiss court decisions using AI chat.
Connects to the existing MCP server (local SQLite FTS5 database) via stdio subprocess.

## Architecture

```
Browser (localhost:5173)
    ↕ HTTP/SSE
FastAPI backend (127.0.0.1:8910)
    ↕ JSON-RPC over stdio
mcp_server.py subprocess
    ↕ SQL
~/.swiss-caselaw/decisions.db (SQLite FTS5)
```

Everything runs locally. No remote MCP server is exposed.

## Prerequisites

- Python 3.11+
- Node.js 18+ and npm
- The local database (run `update_database` via the MCP server first)
- At least one LLM API key (OpenAI, Anthropic, or Google)

## Setup

```bash
# 1. Clone and enter the repo
cd caselaw-repo-1

# 2. Install Python dependencies
pip install fastapi uvicorn python-dotenv mcp pyarrow pydantic

# 3. Install at least one LLM provider SDK
pip install openai          # for OpenAI
pip install anthropic       # for Claude
pip install google-genai    # for Gemini

# 4. Copy and configure environment
cp .env.example .env
# Edit .env — add your API key(s)

# 5. Install frontend dependencies
cd web_ui && npm install && cd ..

# 6. Ensure the database exists
# If you haven't built it yet, the MCP server will tell you.
# The database is at ~/.swiss-caselaw/decisions.db
```

## Running

```bash
# Option A: Use the run script (starts both backend + frontend)
./scripts/run_web_local.sh

# Option B: Start manually in two terminals
# Terminal 1 — backend:
python -m uvicorn web_api.main:app --host 127.0.0.1 --port 8910

# Terminal 2 — frontend:
cd web_ui && npm run dev
```

Open http://localhost:5173

## API Keys

You need at least one of:

| Provider | Env variable | How to get |
|----------|-------------|------------|
| OpenAI | `OPENAI_API_KEY` | https://platform.openai.com/api-keys |
| Anthropic | `ANTHROPIC_API_KEY` | https://console.anthropic.com/ |
| Google Gemini | `GEMINI_API_KEY` | https://aistudio.google.com/apikey |

**Important:** A Claude Desktop or Claude Pro *subscription* does NOT provide an
Anthropic API key. You need a separate developer account at console.anthropic.com.

## Features

- **Multi-provider**: Switch between OpenAI, Claude, and Gemini mid-conversation
- **Streaming**: Token-by-token responses via server-sent events
- **Chat with tools**: The LLM calls search, get_decision, list_courts, etc. via MCP
- **Structured results**: Decision cards with court, canton, date, language, snippets
- **Filters**: Court, canton, language, date range (with reset button)
- **Toggles**: Duplicate collapse (default ON), multilingual expansion
- **Tool traces**: See which tools were called, latency, and hit counts
- **Session management**: Conversations persist in-memory across messages (max 50, auto-eviction)
- **In-app settings**: Configure API keys from the UI without editing `.env`

## Configuration

All settings in `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKEND_PORT` | 8910 | FastAPI port |
| `FRONTEND_PORT` | 5173 | Vite dev server port |
| `MCP_SERVER_PATH` | auto-detected | Path to mcp_server.py |
| `MCP_PYTHON` | system python3 | Python for MCP subprocess |
| `MCP_TOOL_TIMEOUT` | 120 | Tool call timeout (seconds) |
| `SWISS_CASELAW_DIR` | ~/.swiss-caselaw | Database directory |

## Troubleshooting

**"Database not found"**
The MCP server needs a local database. If you're using this with Claude Code,
run the `update_database` MCP tool first. Otherwise, the database should be at
`~/.swiss-caselaw/decisions.db`.

**"Failed to initialize provider"**
Check that the API key is set in `.env` and the corresponding SDK is installed.

**MCP subprocess crashes**
Check stderr output in the backend logs. Common cause: missing Python dependencies
for `mcp_server.py` (pyarrow, mcp, pydantic).

**CORS errors in browser**
The backend only allows `http://localhost:5173` and `http://127.0.0.1:5173`.
Make sure you're accessing the frontend at the correct URL.

**Port already in use**
Change `BACKEND_PORT` or `FRONTEND_PORT` in `.env`.

## Tests

```bash
# Install test deps
pip install pytest pytest-asyncio httpx

# Run all web UI tests
pytest tests/web/ -v

# Run specific test file
pytest tests/web/test_providers.py -v
pytest tests/web/test_chat_integration.py -v
pytest tests/web/test_health_smoke.py -v
```

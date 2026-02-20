# Swiss Case Law MCP Server — Claude Desktop Setup

Search 1,000,000+ Swiss court decisions directly from Claude Desktop. Everything runs locally on your machine.

---

## Prerequisites

- **Claude Desktop** installed ([download](https://claude.ai/download))
- **Python 3.10+** installed
- **~65 GB free disk space** (7 GB download + 58 GB search index)

---

## Setup (5 minutes)

### 1. Clone the repository

```bash
git clone https://github.com/jonashertner/caselaw-repo-1.git
cd caselaw-repo-1
```

### 2. Create a virtual environment and install dependencies

**macOS / Linux:**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install mcp pydantic huggingface-hub pyarrow
```

**Windows:**

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install mcp pydantic huggingface-hub pyarrow
```

### 3. Connect to Claude Desktop

Open the Claude Desktop config file in a text editor:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

If the file doesn't exist, create it. Add the following (replace `/path/to/caselaw-repo-1` with the actual path where you cloned the repository):

**macOS / Linux:**

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "/path/to/caselaw-repo-1/.venv/bin/python3",
      "args": ["/path/to/caselaw-repo-1/mcp_server.py"]
    }
  }
}
```

**Windows:**

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "C:\\path\\to\\caselaw-repo-1\\.venv\\Scripts\\python.exe",
      "args": ["C:\\path\\to\\caselaw-repo-1\\mcp_server.py"]
    }
  }
}
```

> If the file already has other MCP servers, add `"swiss-caselaw": { ... }` inside the existing `"mcpServers"` block — don't overwrite them.

### 4. Restart Claude Desktop

Quit and reopen Claude Desktop. You should see a hammer icon in the input bar, indicating MCP tools are available.

### 5. Build the search index (first time only)

In Claude Desktop, type:

> *"Run the update_database tool to download the Swiss case law dataset."*

Claude will call the `update_database` tool. This downloads ~7 GB of Parquet files from HuggingFace and builds a local SQLite search index. **Takes 30–60 minutes** depending on your internet speed and disk. You only need to do this once.

---

## Usage

Once the index is built, just ask questions in natural language:

- *"Find BGer decisions on tenant eviction from 2024"*
- *"Search for BVGer asylum cases involving Eritrea"*
- *"Show me recent ECHR decisions involving Switzerland"*
- *"What does BGE 133 I 106 say?"*
- *"Find decisions citing Art. 8 BV"*

Claude automatically calls the search tools and shows matching decisions with highlighted snippets. You can ask follow-up questions about specific decisions.

### Available tools

| Tool | What it does |
|------|-------------|
| `search_decisions` | Full-text search with filters (court, canton, language, date range) |
| `get_decision` | Fetch a single decision by docket number (e.g., `6B_1234/2025`) |
| `list_courts` | List all 93 courts with decision counts |
| `get_statistics` | Aggregate statistics by court, canton, or year |
| `draft_mock_decision` | Generate a research outline grounded in case law and statutes |
| `update_database` | Download the latest data from HuggingFace |

---

## Updating the dataset

The dataset is updated daily. To get the latest decisions, ask Claude:

> *"Update the Swiss case law database."*

This re-downloads changed Parquet files and rebuilds the index incrementally.

---

## Troubleshooting

**"No MCP tools available" (no hammer icon)**
- Make sure the config file path is correct for your OS
- Verify the JSON is valid (no trailing commas, correct quoting)
- Check that the `command` path points to the Python executable *inside* the `.venv`, not the system Python
- Restart Claude Desktop after editing the config

**"Database not found" or empty search results**
- Run the `update_database` tool first — the index must be built before searching

**Slow first search after restart**
- The first query takes a few seconds to open the ~58 GB database. Subsequent queries are fast.

**Path issues on macOS**
- Use the full absolute path (e.g., `/Users/yourname/caselaw-repo-1/.venv/bin/python3`), not `~` or relative paths

**Path issues on Windows**
- Use double backslashes in the JSON: `C:\\Users\\...`
- Use `.venv\\Scripts\\python.exe`, not `.venv\\bin\\python3`

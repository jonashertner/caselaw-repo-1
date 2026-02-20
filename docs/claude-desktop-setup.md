# Swiss Case Law — Claude Desktop Setup Guide

Search 1,000,000+ Swiss court decisions directly inside Claude Desktop. The entire dataset runs locally on your machine — no API keys, no cloud services.

This guide walks you through every step. Pick your operating system and follow along.

---

## What you need before starting

1. **Claude Desktop** — download it at [claude.ai/download](https://claude.ai/download) if you don't have it yet
2. **Python 3.10 or newer** — check by opening a terminal and running `python3 --version` (macOS/Linux) or `python --version` (Windows)
3. **Git** — check by running `git --version`
4. **65 GB of free disk space** — the dataset is 7 GB to download, and the search index it builds is ~58 GB

---

## macOS Setup

### Step 1 — Download the code

Open **Terminal** (press `Cmd + Space`, type "Terminal", press Enter). Then run:

```bash
cd ~
git clone https://github.com/jonashertner/caselaw-repo-1.git
```

This creates a folder at `/Users/YOUR_USERNAME/caselaw-repo-1`.

### Step 2 — Install Python dependencies

Still in Terminal, run these three commands one by one:

```bash
cd ~/caselaw-repo-1
python3 -m venv .venv
.venv/bin/pip install mcp pydantic huggingface-hub pyarrow
```

Wait for the installation to finish. You should see "Successfully installed ..." at the end.

### Step 3 — Find your exact paths

Run this command. It prints the two paths you'll need in the next step:

```bash
echo "command: $(cd ~/caselaw-repo-1 && pwd)/.venv/bin/python3"
echo "    arg: $(cd ~/caselaw-repo-1 && pwd)/mcp_server.py"
```

It will print something like:

```
command: /Users/anna/caselaw-repo-1/.venv/bin/python3
    arg: /Users/anna/caselaw-repo-1/mcp_server.py
```

Keep these — you'll paste them into the config file next.

### Step 4 — Edit the Claude Desktop config file

The config file is at:

```
~/Library/Application Support/Claude/claude_desktop_config.json
```

Open it by running:

```bash
open -a TextEdit ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

If TextEdit says the file doesn't exist, create it:

```bash
mkdir -p ~/Library/Application\ Support/Claude
echo '{}' > ~/Library/Application\ Support/Claude/claude_desktop_config.json
open -a TextEdit ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

**Replace the entire file contents** with the following. Use the paths from Step 3:

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "/Users/YOUR_USERNAME/caselaw-repo-1/.venv/bin/python3",
      "args": ["/Users/YOUR_USERNAME/caselaw-repo-1/mcp_server.py"]
    }
  }
}
```

> **Already have other MCP servers?** Don't overwrite the file. Instead, add the `"swiss-caselaw": { ... }` block inside your existing `"mcpServers"` object, separated by a comma.

Save the file (`Cmd + S`) and close TextEdit.

### Step 5 — Restart Claude Desktop

Quit Claude Desktop completely (`Cmd + Q` — not just close the window). Then reopen it.

**How to verify it worked:** Look at the text input bar. You should see a small hammer icon on the right side. Click it — you should see tools like `search_decisions` and `get_decision` in the list.

If you don't see the hammer icon, see [Troubleshooting](#troubleshooting) below.

### Step 6 — Build the search index (one time, ~30–60 minutes)

In Claude Desktop, send this message:

> **Please run the update_database tool to download the Swiss case law dataset.**

Claude will start downloading ~7 GB of data from HuggingFace and building the local search index. This takes 30–60 minutes depending on your internet connection and disk speed. You can watch the progress in Claude's response.

**You only need to do this once.** After it finishes, searching is instant.

---

## Windows Setup

### Step 1 — Download the code

Open **PowerShell** (press `Win + X`, select "Terminal" or "PowerShell"). Then run:

```powershell
cd $HOME
git clone https://github.com/jonashertner/caselaw-repo-1.git
```

This creates a folder at `C:\Users\YOUR_USERNAME\caselaw-repo-1`.

### Step 2 — Install Python dependencies

Still in PowerShell, run these three commands one by one:

```powershell
cd $HOME\caselaw-repo-1
python -m venv .venv
.venv\Scripts\pip install mcp pydantic huggingface-hub pyarrow
```

Wait for the installation to finish. You should see "Successfully installed ..." at the end.

### Step 3 — Find your exact paths

Run this command to print the paths you need:

```powershell
Write-Host "command: $HOME\caselaw-repo-1\.venv\Scripts\python.exe"
Write-Host "    arg: $HOME\caselaw-repo-1\mcp_server.py"
```

It will print something like:

```
command: C:\Users\Anna\caselaw-repo-1\.venv\Scripts\python.exe
    arg: C:\Users\Anna\caselaw-repo-1\mcp_server.py
```

Keep these paths for the next step. **Important:** when you put them in the JSON config file, you must double every backslash (`\` becomes `\\`).

### Step 4 — Edit the Claude Desktop config file

The config file is at:

```
%APPDATA%\Claude\claude_desktop_config.json
```

Open it by running:

```powershell
notepad "$env:APPDATA\Claude\claude_desktop_config.json"
```

If Notepad asks whether to create the file, click **Yes**.

**Replace the entire file contents** with the following. Use the paths from Step 3, with doubled backslashes:

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "C:\\Users\\YOUR_USERNAME\\caselaw-repo-1\\.venv\\Scripts\\python.exe",
      "args": ["C:\\Users\\YOUR_USERNAME\\caselaw-repo-1\\mcp_server.py"]
    }
  }
}
```

> **Already have other MCP servers?** Don't overwrite the file. Instead, add the `"swiss-caselaw": { ... }` block inside your existing `"mcpServers"` object, separated by a comma.

Save the file (`Ctrl + S`) and close Notepad.

### Step 5 — Restart Claude Desktop

Quit Claude Desktop completely (right-click the system tray icon and choose "Quit"). Then reopen it.

**How to verify it worked:** Look at the text input bar. You should see a small hammer icon on the right side. Click it — you should see tools like `search_decisions` and `get_decision` in the list.

If you don't see the hammer icon, see [Troubleshooting](#troubleshooting) below.

### Step 6 — Build the search index (one time, ~30–60 minutes)

In Claude Desktop, send this message:

> **Please run the update_database tool to download the Swiss case law dataset.**

Claude will start downloading ~7 GB of data from HuggingFace and building the local search index. This takes 30–60 minutes depending on your internet connection and disk speed. You can watch the progress in Claude's response.

**You only need to do this once.** After it finishes, searching is instant.

---

## Using it

Once the index is built, just ask questions in natural language. Examples:

| What you type | What happens |
|---|---|
| *"Find BGer decisions about Mietrecht from 2024"* | Searches all Federal Supreme Court decisions on tenancy law |
| *"Search for BVGer asylum cases involving Eritrea"* | Searches Federal Administrative Court asylum decisions |
| *"Look up BGE 133 I 106"* | Fetches that specific leading case with full text |
| *"Find decisions citing Art. 8 BV"* | Searches for decisions that reference this constitutional article |
| *"How many decisions does each court have?"* | Shows statistics across all 93 courts |
| *"Draft a legal analysis of whether X constitutes Y"* | Builds a research outline grounded in actual case law |

Claude automatically picks the right search tool, runs the query, and shows you the results. You can then ask follow-up questions like *"Show me the full text of the second result"* or *"Find more recent decisions on the same topic."*

### Available tools

| Tool | What it does |
|------|-------------|
| `search_decisions` | Full-text search with filters (court, canton, language, date range) |
| `get_decision` | Fetch one decision by docket number (e.g., `6B_1234/2025`) or ID |
| `list_courts` | List all 93 courts with decision counts and date ranges |
| `get_statistics` | Aggregate statistics by court, canton, or year |
| `draft_mock_decision` | Legal research outline grounded in case law and statutes |
| `update_database` | Download the latest data from HuggingFace |

---

## Keeping the dataset up to date

The dataset is updated every night with new court decisions. To get the latest data, just ask Claude:

> **Update the Swiss case law database.**

This downloads only the changed files and updates the index. Much faster than the initial build.

---

## Troubleshooting

### No hammer icon after restarting Claude Desktop

This means Claude Desktop didn't load the MCP server. Check these in order:

1. **Is the config file in the right place?**
   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`

2. **Is the JSON valid?** Open the file and check for:
   - Missing or extra commas
   - Mismatched braces `{ }`
   - Unescaped backslashes on Windows (must be `\\`, not `\`)

3. **Do the paths actually exist?** Test by running the command directly in your terminal:
   ```bash
   /Users/YOUR_USERNAME/caselaw-repo-1/.venv/bin/python3 --version
   ```
   This should print `Python 3.x.x`. If it says "No such file", the path is wrong.

4. **Did you fully quit Claude Desktop?** On macOS, use `Cmd + Q`. Just closing the window is not enough.

### "Database not found" or empty results

You need to build the search index first. Ask Claude to run the `update_database` tool (Step 6 above).

### First search is slow

The first query after opening Claude Desktop takes 3–5 seconds to load the ~58 GB database into memory. Every subsequent search is fast.

### "Permission denied" errors on macOS

If your terminal says "permission denied" when running Python:

```bash
chmod +x ~/caselaw-repo-1/.venv/bin/python3
```

### Python not found

- **macOS**: Install Python from [python.org](https://www.python.org/downloads/) or via Homebrew: `brew install python`
- **Windows**: Install Python from [python.org](https://www.python.org/downloads/). During installation, check **"Add Python to PATH"**.

### Not enough disk space

The full search index requires ~65 GB. If you don't have enough space, the `update_database` tool will fail partway through. Free up space and run it again — it resumes where it left off.

# Swiss Case Law — MCP Setup Guide

Search 1,000,000+ Swiss court decisions directly inside Claude, ChatGPT, or Gemini.

There are two options: **remote** (no download, instant access) or **local** (offline access, 65 GB disk). The remote server works with all major AI platforms.

---

## Option A: Remote server (recommended)

Connect to the hosted server. No data download, no Python, no 65 GB disk usage.

### Claude Desktop / claude.ai

**Plan required:** Pro, Max, Team, or Enterprise.

1. Open **Settings** → **Connectors**
2. Click **"Add custom connector"**
3. Paste `https://mcp.opencaselaw.ch`
4. Click **Add**

Same steps in the browser (claude.ai) and the desktop app. No Node.js, no config files.

### Claude Code

Any plan. One command:

```bash
claude mcp add swiss-caselaw --transport sse https://mcp.opencaselaw.ch
```

### ChatGPT

**Plan required:** Plus, Pro, Team, Enterprise, or Edu.

1. Open **Settings** → **Connectors**
2. Enable **Developer Mode** (under Advanced)
3. Click **Create**
4. Enter name: `swiss-caselaw`
5. Paste MCP server URL: `https://mcp.opencaselaw.ch`
6. Check **"I trust this application"** → **Create**

To use in a chat: click **+** → **More** → **Developer Mode** → enable the swiss-caselaw connector.

### Gemini CLI

Free, no account plan required.

Add to `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "url": "https://mcp.opencaselaw.ch"
    }
  }
}
```

Restart Gemini CLI after saving.

### Other MCP clients

The server uses the SSE (Server-Sent Events) transport at `https://mcp.opencaselaw.ch`. Any MCP client that supports remote SSE servers can connect using this URL. No authentication required.

### Verify it works

After connecting, try asking:

> Search for BGer decisions on Mietrecht Kündigung from 2024

You should see search results with matched decisions, snippets, and metadata.

> The `update_database` tool is not available on the remote server — the dataset is updated automatically every night.

<details>
<summary>Alternative: manual JSON config for Claude Desktop (if custom connectors aren't available on your plan)</summary>

Requires [Node.js 18+](https://nodejs.org).

Add to your `claude_desktop_config.json`:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://mcp.opencaselaw.ch"]
    }
  }
}
```

Restart Claude Desktop after saving.

</details>

---

## Option B: Local server (offline access)

Run the MCP server locally with your own copy of the database. Requires 65 GB free disk and a one-time 30–60 minute setup.

### What you need

1. **Claude Desktop** — [claude.ai/download](https://claude.ai/download)
2. **Python 3.10 or newer** — `python3 --version` (macOS/Linux) or `python --version` (Windows)
3. **Git** — `git --version`
4. **65 GB of free disk space**

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

### Step 4 — Add the MCP server to Claude Desktop

You have two options. **Option A** uses the Claude Desktop settings UI. **Option B** edits the config file directly — use this if Option A doesn't work or if your version of Claude Desktop doesn't have the settings UI.

#### Option A: Through the Claude Desktop settings (easiest)

1. Open **Claude Desktop**
2. Open Settings:
   - Click the **Claude** menu in the menu bar (top-left) → **Settings...**
   - Or press `Cmd + ,`
3. Click **Developer** in the left sidebar
4. Click **Edit Config**. This opens `claude_desktop_config.json` in your default text editor.
5. **Replace the entire file contents** with the following. Use the paths from Step 3:

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

6. Save the file (`Cmd + S`) and close the text editor.
7. Go back to Claude Desktop Settings → Developer. You should now see **swiss-caselaw** listed under MCP Servers.

> **Already have other MCP servers?** Don't overwrite the file. Add the `"swiss-caselaw": { ... }` block inside your existing `"mcpServers"` object, separated by a comma.

#### Option B: Edit the config file manually (fallback)

If you can't find the Developer settings, edit the config file directly:

```bash
open -a TextEdit ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

If the file doesn't exist, create it first:

```bash
mkdir -p ~/Library/Application\ Support/Claude
echo '{}' > ~/Library/Application\ Support/Claude/claude_desktop_config.json
open -a TextEdit ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

Paste the same JSON from Option A, save, and close.

### Step 5 — Restart Claude Desktop

Quit Claude Desktop completely (`Cmd + Q` — not just close the window). Then reopen it.

**How to verify it worked:** Look at the text input bar at the bottom of the chat. You should see a small hammer icon on the right side. Click it — you should see tools like `search_decisions` and `get_decision` in the list.

You can also verify in Settings → Developer — the swiss-caselaw server should show a green "running" indicator.

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

### Step 4 — Add the MCP server to Claude Desktop

You have two options. **Option A** uses the Claude Desktop settings UI. **Option B** edits the config file directly — use this if Option A doesn't work or if your version of Claude Desktop doesn't have the settings UI.

#### Option A: Through the Claude Desktop settings (easiest)

1. Open **Claude Desktop**
2. Open Settings:
   - Click the **hamburger menu** (three lines, top-left) → **Settings...**
   - Or click **File** → **Settings...**
3. Click **Developer** in the left sidebar
4. Click **Edit Config**. This opens `claude_desktop_config.json` in Notepad.
5. **Replace the entire file contents** with the following. Use the paths from Step 3, with doubled backslashes:

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

6. Save the file (`Ctrl + S`) and close Notepad.
7. Go back to Claude Desktop Settings → Developer. You should now see **swiss-caselaw** listed under MCP Servers.

> **Already have other MCP servers?** Don't overwrite the file. Add the `"swiss-caselaw": { ... }` block inside your existing `"mcpServers"` object, separated by a comma.

#### Option B: Edit the config file manually (fallback)

If you can't find the Developer settings, edit the config file directly:

```powershell
notepad "$env:APPDATA\Claude\claude_desktop_config.json"
```

If Notepad asks whether to create the file, click **Yes**. Paste the same JSON from Option A, save, and close.

### Step 5 — Restart Claude Desktop

Quit Claude Desktop completely (right-click the system tray icon and choose "Quit"). Then reopen it.

**How to verify it worked:** Look at the text input bar at the bottom of the chat. You should see a small hammer icon on the right side. Click it — you should see tools like `search_decisions` and `get_decision` in the list.

You can also verify in Settings → Developer — the swiss-caselaw server should show a green "running" indicator.

If you don't see the hammer icon, see [Troubleshooting](#troubleshooting) below.

### Step 6 — Build the search index (one time, ~30–60 minutes)

In Claude Desktop, send this message:

> **Please run the update_database tool to download the Swiss case law dataset.**

Claude will start downloading ~7 GB of data from HuggingFace and building the local search index. This takes 30–60 minutes depending on your internet connection and disk speed. You can watch the progress in Claude's response.

**You only need to do this once.** After it finishes, searching is instant.

---

## Using it

Once connected, just ask questions in natural language. Examples:

| What you type | What happens |
|---|---|
| *"Find BGer decisions about Mietrecht from 2024"* | Searches all Federal Supreme Court decisions on tenancy law |
| *"Search for BVGer asylum cases involving Eritrea"* | Searches Federal Administrative Court asylum decisions |
| *"Look up BGE 133 I 106"* | Fetches that specific leading case with full text |
| *"Find decisions citing Art. 8 BV"* | Searches for decisions that reference this constitutional article |
| *"How many decisions does each court have?"* | Shows statistics across all 93 courts |
| *"Draft a legal analysis of whether X constitutes Y"* | Builds a research outline grounded in actual case law |

The AI automatically picks the right search tool, runs the query, and shows you the results. You can then ask follow-up questions like *"Show me the full text of the second result"* or *"Find more recent decisions on the same topic."*

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

**Remote server:** The dataset is updated automatically every night. No action needed.

**Local server:** Ask your AI to run the `update_database` tool:

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

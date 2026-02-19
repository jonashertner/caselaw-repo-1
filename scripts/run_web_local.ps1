# Start the Swiss Case Law web UI locally (Windows PowerShell).
# Usage: .\scripts\run_web_local.ps1

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $RepoRoot

# Load .env if present
if (Test-Path .env) {
    Write-Host "Loading .env..."
    Get-Content .env | ForEach-Object {
        if ($_ -match '^([^#][^=]+)=(.*)$') {
            [Environment]::SetEnvironmentVariable($Matches[1].Trim(), $Matches[2].Trim(), "Process")
        }
    }
}

$BackendPort = if ($env:BACKEND_PORT) { $env:BACKEND_PORT } else { "8910" }
$FrontendPort = if ($env:FRONTEND_PORT) { $env:FRONTEND_PORT } else { "5173" }

# Check Python deps
Write-Host "Checking Python dependencies..."
try {
    python -c "import fastapi, uvicorn, dotenv, mcp" 2>$null
} catch {
    Write-Host "Installing Python dependencies..."
    pip install fastapi uvicorn python-dotenv mcp pyarrow pydantic
}

# Check at least one provider SDK
python -c @"
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
"@

# Check Node.js / npm
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: npm not found. Install Node.js from https://nodejs.org"
    exit 1
}

# Install frontend deps if needed
if (-not (Test-Path web_ui\node_modules)) {
    Write-Host "Installing frontend dependencies..."
    Push-Location web_ui
    npm install
    Pop-Location
}

Write-Host ""
Write-Host "Starting backend on http://127.0.0.1:$BackendPort"
Write-Host "Starting frontend on http://127.0.0.1:$FrontendPort"
Write-Host ""

# Start backend in background
$backend = Start-Process -NoNewWindow -PassThru python -ArgumentList "-m", "uvicorn", "web_api.main:app", "--host", "127.0.0.1", "--port", $BackendPort, "--log-level", "info"

# Start frontend in background
$frontend = Start-Process -NoNewWindow -PassThru -WorkingDirectory web_ui npm -ArgumentList "run", "dev", "--", "--host", "127.0.0.1", "--port", $FrontendPort

Write-Host "Press Ctrl+C to stop."

try {
    $backend.WaitForExit()
} finally {
    Write-Host "Shutting down..."
    if (-not $backend.HasExited) { $backend.Kill() }
    if (-not $frontend.HasExited) { $frontend.Kill() }
}

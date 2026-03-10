#!/bin/bash
# Reverse SOCKS tunnel for NE scraper
#
# Starts a local SOCKS5 proxy on port 1080, then reverse-tunnels
# VPS:1080 → localhost:1080 so the VPS can route NE scraper traffic
# through this machine's IP (jurisprudence.ne.ch blocks Hetzner).
#
# Usage:
#   ./ne_tunnel.sh          # foreground (Ctrl-C to stop)
#   ./ne_tunnel.sh --bg     # background (writes PID to ne_tunnel.pid)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROXY_SCRIPT="$SCRIPT_DIR/socks5_proxy.py"
PYTHON="/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"
SSH_KEY="$HOME/.ssh/caselaw"
VPS="root@46.225.212.40"
LOCAL_PORT=1080
REMOTE_PORT=1080

cleanup() {
    echo "Stopping tunnel..."
    [ -n "${PROXY_PID:-}" ] && kill "$PROXY_PID" 2>/dev/null
    [ -n "${SSH_PID:-}" ] && kill "$SSH_PID" 2>/dev/null
    exit 0
}
trap cleanup INT TERM

# Start local SOCKS5 proxy
"$PYTHON" "$PROXY_SCRIPT" --port "$LOCAL_PORT" &
PROXY_PID=$!
sleep 1

if ! kill -0 "$PROXY_PID" 2>/dev/null; then
    echo "ERROR: SOCKS5 proxy failed to start (port $LOCAL_PORT in use?)"
    exit 1
fi
echo "SOCKS5 proxy running on :$LOCAL_PORT (PID $PROXY_PID)"

# Reverse tunnel: VPS:1080 → localhost:1080
/opt/homebrew/bin/autossh -M 0 -N \
    -o "ServerAliveInterval 30" \
    -o "ServerAliveCountMax 3" \
    -o "ExitOnForwardFailure yes" \
    -R "$REMOTE_PORT:127.0.0.1:$LOCAL_PORT" \
    -i "$SSH_KEY" "$VPS" &
SSH_PID=$!
sleep 2

if ! kill -0 "$SSH_PID" 2>/dev/null; then
    echo "ERROR: SSH reverse tunnel failed to start"
    kill "$PROXY_PID" 2>/dev/null
    exit 1
fi
echo "Reverse tunnel active: VPS:$REMOTE_PORT → localhost:$LOCAL_PORT (PID $SSH_PID)"

if [ "${1:-}" = "--bg" ]; then
    echo "$PROXY_PID $SSH_PID" > "$SCRIPT_DIR/ne_tunnel.pid"
    echo "Running in background. PIDs saved to ne_tunnel.pid"
    echo "Stop with: kill \$(cat $SCRIPT_DIR/ne_tunnel.pid)"
    disown "$PROXY_PID" "$SSH_PID"
else
    echo "Tunnel active. Press Ctrl-C to stop."
    wait
fi

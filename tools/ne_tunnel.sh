#!/bin/bash
# Reverse SOCKS tunnel for NE/Bger scrapers
#
# Starts a local SOCKS5 proxy on port 1080, then reverse-tunnels
# VPS:1080 -> localhost:1080 so the VPS routes scraper traffic through
# this machine's residential IP (courts block Hetzner).
#
# Under launchd (ch.opencaselaw.ne-tunnel, KeepAlive) it self-supervises:
# restarts the proxy if it dies OR stops answering on :1080, and exits if
# autossh dies so launchd restarts the whole job. No more silent
# "tunnel up, proxy dead" state.
#
# Usage:
#   ./ne_tunnel.sh          # foreground + supervise (launchd uses this)
#   ./ne_tunnel.sh --bg     # background, write PIDs to ne_tunnel.pid

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROXY_SCRIPT="$SCRIPT_DIR/socks5_proxy.py"
PYTHON="/usr/bin/python3"
SSH_KEY="$HOME/.ssh/caselaw"
VPS="root@46.225.212.40"
LOCAL_PORT=1080
REMOTE_PORT=1080

cleanup() {
    echo "Stopping tunnel..."
    [ -n "${PROXY_PID:-}" ] && kill "$PROXY_PID" 2>/dev/null || true
    [ -n "${SSH_PID:-}" ] && kill "$SSH_PID" 2>/dev/null || true
    exit 0
}
trap cleanup INT TERM

start_proxy() {
    "$PYTHON" "$PROXY_SCRIPT" --host 127.0.0.1 --port "$LOCAL_PORT" &
    PROXY_PID=$!
    sleep 1
    kill -0 "$PROXY_PID" 2>/dev/null
}

# Start local SOCKS5 proxy
if ! start_proxy; then
    echo "ERROR: SOCKS5 proxy failed to start (port $LOCAL_PORT in use?)"
    exit 1
fi
echo "SOCKS5 proxy running on :$LOCAL_PORT (PID $PROXY_PID)"

# Reverse tunnel: VPS:1080 -> localhost:1080
env AUTOSSH_GATETIME=0 /opt/homebrew/bin/autossh -M 0 -N \
    -o "ServerAliveInterval 30" \
    -o "ServerAliveCountMax 3" \
    -o "ExitOnForwardFailure yes" \
    -o "StrictHostKeyChecking accept-new" \
    -R "$REMOTE_PORT:127.0.0.1:$LOCAL_PORT" \
    -i "$SSH_KEY" "$VPS" &
SSH_PID=$!
sleep 2

if ! kill -0 "$SSH_PID" 2>/dev/null; then
    echo "ERROR: SSH reverse tunnel failed to start"
    kill "$PROXY_PID" 2>/dev/null || true
    exit 1
fi
echo "Reverse tunnel active: VPS:$REMOTE_PORT -> localhost:$LOCAL_PORT (PID $SSH_PID)"

if [ "${1:-}" = "--bg" ]; then
    echo "$PROXY_PID $SSH_PID" > "$SCRIPT_DIR/ne_tunnel.pid"
    echo "Running in background. PIDs saved to ne_tunnel.pid"
    disown "$PROXY_PID" "$SSH_PID" 2>/dev/null || true
    exit 0
fi

# Foreground supervision (launchd path): keep the proxy alive + answering;
# exit if autossh dies so launchd KeepAlive restarts the whole job.
echo "Supervising proxy + tunnel (15s interval)..."
while true; do
    if ! kill -0 "$PROXY_PID" 2>/dev/null || ! /usr/bin/nc -z 127.0.0.1 "$LOCAL_PORT" 2>/dev/null; then
        echo "$(date '+%F %T') proxy down -> restarting"
        kill "$PROXY_PID" 2>/dev/null || true
        if ! start_proxy; then
            echo "$(date '+%F %T') proxy restart failed -> exiting for launchd"
            kill "$SSH_PID" 2>/dev/null || true
            exit 1
        fi
    fi
    if ! kill -0 "$SSH_PID" 2>/dev/null; then
        echo "$(date '+%F %T') autossh down -> exiting for launchd to restart"
        kill "$PROXY_PID" 2>/dev/null || true
        exit 1
    fi
    sleep 15
done

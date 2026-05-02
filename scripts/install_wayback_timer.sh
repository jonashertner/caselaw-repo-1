#!/usr/bin/env bash
# Install systemd unit + timer for the Wayback Machine archiver.
# Drains wayback_queue (populated by build_fts5._ensure_wayback_queue) at
# a respectful 2 req/s for up to 10 min per run, every hour.
#
# Backfill scale: ~1.94M URLs at 2 req/s = ~270 hours (11 days continuous)
# of background processing. Hourly 10-min runs work through ~72k URLs/day,
# so the initial fill completes in ~27 days. Daily delta is trivial.
#
# Usage (on the VPS):
#   sudo bash scripts/install_wayback_timer.sh
set -euo pipefail

REPO_DIR="${REPO_DIR:-/opt/caselaw/repo}"
SERVICE=/etc/systemd/system/opencaselaw-wayback.service
TIMER=/etc/systemd/system/opencaselaw-wayback.timer

cat > "$SERVICE" <<EOF
[Unit]
Description=OpenCaseLaw Wayback Machine archiver (drain wayback_queue)
After=network-online.target
OnFailure=ntfy-alert@%n.service

[Service]
Type=oneshot
WorkingDirectory=${REPO_DIR}
ExecStart=/usr/bin/python3 ${REPO_DIR}/scripts/wayback_archiver.py \\
    --batch 1000 --rate 2.0 --max-runtime 540
TimeoutStartSec=600
EOF

cat > "$TIMER" <<EOF
[Unit]
Description=Run OpenCaseLaw Wayback archiver hourly

[Timer]
OnBootSec=10min
OnUnitActiveSec=1h
AccuracySec=5min
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now opencaselaw-wayback.timer
systemctl list-timers opencaselaw-wayback.timer --no-pager
echo
echo "Installed. To run once now:"
echo "  systemctl start opencaselaw-wayback.service"
echo "Then check progress:"
echo "  journalctl -u opencaselaw-wayback.service --no-pager -n 30"

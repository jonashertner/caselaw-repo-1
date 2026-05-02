#!/usr/bin/env bash
# Install systemd unit + timer for the OpenCaseLaw disk monitor.
# Idempotent — safe to re-run.
#
# Usage (on the VPS):
#   sudo bash scripts/install_disk_monitor.sh
#
# After install: `systemctl status opencaselaw-disk-monitor.timer`
set -euo pipefail

REPO_DIR="${REPO_DIR:-/opt/caselaw/repo}"
SERVICE=/etc/systemd/system/opencaselaw-disk-monitor.service
TIMER=/etc/systemd/system/opencaselaw-disk-monitor.timer

cat > "$SERVICE" <<EOF
[Unit]
Description=OpenCaseLaw disk-space monitor
OnFailure=ntfy-alert@%n.service

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 ${REPO_DIR}/scripts/disk_monitor.py
TimeoutStartSec=30
EOF

cat > "$TIMER" <<EOF
[Unit]
Description=Run OpenCaseLaw disk monitor every 30 minutes

[Timer]
OnBootSec=5min
OnUnitActiveSec=30min
AccuracySec=1min
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now opencaselaw-disk-monitor.timer
systemctl list-timers opencaselaw-disk-monitor.timer --no-pager
echo
echo "Installed. To run once now:"
echo "  systemctl start opencaselaw-disk-monitor.service"
echo "Last result:"
echo "  systemctl status opencaselaw-disk-monitor.service --no-pager"

#!/bin/bash
set -euo pipefail

echo "Installing tgmcpd..."

pip install -e /home/gg/projects/MCP-TG

sudo cp /home/gg/projects/MCP-TG/scripts/tgmcpd.service /etc/systemd/system/tgmcpd.service
sudo systemctl daemon-reload
sudo systemctl enable tgmcpd
sudo systemctl restart tgmcpd

echo "tgmcpd installed and started."
echo "Check status: sudo systemctl status tgmcpd"
echo "View logs: sudo journalctl -u tgmcpd -f"

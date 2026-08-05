#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SYSTEMD="/etc/systemd/system"
VENV="/srv/shared/.venv"

"${VENV}/bin/pip" install -e "${ROOT}"

# Drop legacy per-agent units and install the template.
rm -f "${SYSTEMD}"/ltx_*.service
rm -f "${SYSTEMD}"/ltx-*.service
cp "${ROOT}"/services/ltx-agent@.service "${SYSTEMD}/"
systemctl daemon-reload

echo
echo "Installed template: ${SYSTEMD}/ltx-agent@.service"
echo
echo "Start an agent (\"/\" in the path becomes \"-\" in the instance name):"
echo "  sudo systemctl start ltx-agent@connectors-binanceusdm"
echo "  sudo systemctl start ltx-agent@connectors-binancespot"
echo "  sudo systemctl start ltx-agent@connectors-binancecoin"
echo "  sudo systemctl start ltx-agent@connectors-polymarketgamma"
echo "  sudo systemctl start ltx-agent@monitoring-datacollector"
echo
echo "Enable on boot:"
echo "  sudo systemctl enable ltx-agent@connectors-binanceusdm"
echo
echo "Status / logs:"
echo "  systemctl status ltx-agent@connectors-binanceusdm"
echo "  journalctl -u ltx-agent@connectors-binanceusdm -f"
echo

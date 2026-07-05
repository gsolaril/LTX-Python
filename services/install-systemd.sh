#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SYSTEMD="/etc/systemd/system"
VENV="/srv/shared/.venv"

"${VENV}/bin/pip" install -e "${ROOT}"

rm -f "${SYSTEMD}"/ltx_*.service
cp "${ROOT}"/services/*.service "${SYSTEMD}/"
systemctl daemon-reload

echo "LTX units in ${SYSTEMD}:"
echo
printf "  %s        %s\n" "Last modified" "Service name"
printf "  %s        %s\n" "‾‾‾‾‾‾‾‾‾‾‾‾‾" "‾‾‾‾‾‾‾‾‾‾‾‾"
shopt -s nullglob
for f in "${SYSTEMD}"/ltx_*; do
    modified="$(stat -c '%y' "$f" | cut -d'.' -f1)"
    printf "  %s  %s\n" "$modified" "$(basename "$f")"
done | sort -k2
shopt -u nullglob
echo

#!/usr/bin/env bash
set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
exec bash "${SCRIPT_DIR}/run-network-campaign.sh" edge-cloud "${1:-3}" "${2:-}"

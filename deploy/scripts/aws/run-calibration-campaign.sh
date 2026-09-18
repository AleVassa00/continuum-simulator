#!/usr/bin/env bash
set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
readonly REPETITIONS="${1:-1}"
readonly -a EXPERIMENTS=(
  "experiments/calibration/cloud-w1-a10000.yaml"
  "experiments/calibration/cloud-w1-a15000.yaml"
  "experiments/calibration/cloud-w1-a20000.yaml"
  "experiments/calibration/cloud-w1-a40000.yaml"
  "experiments/calibration/cloud-w6-a20000.yaml"
)

(( $# <= 1 )) || { echo "Uso: bash $0 [RIPETIZIONI]" >&2; exit 1; }
[[ "${REPETITIONS}" =~ ^[1-9][0-9]*$ ]] || {
  echo "RIPETIZIONI deve essere un intero positivo" >&2
  exit 1
}

cd "${REPO_ROOT}"
for experiment in "${EXPERIMENTS[@]}"; do
  echo "[calibration-campaign] deploy ${experiment}"
  bash deploy/scripts/aws/deploy-pilot.sh "${experiment}"

  for ((repetition = 1; repetition <= REPETITIONS; repetition++)); do
    echo "[calibration-campaign] run ${repetition}/${REPETITIONS}: ${experiment}"
    bash deploy/scripts/aws/run-full.sh "${experiment}"
  done
done

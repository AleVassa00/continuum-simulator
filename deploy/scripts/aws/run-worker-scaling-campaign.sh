#!/usr/bin/env bash
set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
readonly REPETITIONS="${1:-3}"
readonly -a EXPERIMENTS=(
  "experiments/worker-scaling/cloud-scale-w1.yaml"
  "experiments/worker-scaling/cloud-scale-w2.yaml"
  "experiments/worker-scaling/cloud-scale-w3.yaml"
  "experiments/worker-scaling/cloud-scale-w4.yaml"
  "experiments/worker-scaling/cloud-scale-w6.yaml"
)

(( $# <= 1 )) || { echo "Uso: bash $0 [RIPETIZIONI]" >&2; exit 1; }
[[ "${REPETITIONS}" =~ ^[1-9][0-9]*$ ]] || {
  echo "RIPETIZIONI deve essere un intero positivo" >&2
  exit 1
}

cd "${REPO_ROOT}"
for experiment in "${EXPERIMENTS[@]}"; do
  echo "[worker-scaling-campaign] deploy ${experiment}"
  bash deploy/scripts/aws/deploy-pilot.sh "${experiment}"

  for ((repetition = 1; repetition <= REPETITIONS; repetition++)); do
    echo "[worker-scaling-campaign] run ${repetition}/${REPETITIONS}: ${experiment}"
    bash deploy/scripts/aws/run-full.sh "${experiment}"
  done
done

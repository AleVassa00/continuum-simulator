#!/usr/bin/env bash
set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
readonly REPETITIONS="${1:-3}"
readonly -a EXPERIMENTS=(
  "experiments/network/network-baseline.yaml"
  "experiments/network/simulator-edge-delay-50ms.yaml"
  "experiments/network/simulator-edge-delay-100ms.yaml"
  "experiments/network/edge-kafka-delay-50ms.yaml"
  "experiments/network/edge-kafka-delay-100ms.yaml"
  "experiments/network/simulator-edge-rate-4mbit.yaml"
  "experiments/network/simulator-edge-rate-2mbit.yaml"
  "experiments/network/edge-kafka-rate-300kbit.yaml"
  "experiments/network/edge-kafka-rate-150kbit.yaml"
)

(( $# <= 1 )) || { echo "Uso: bash $0 [RIPETIZIONI]" >&2; exit 1; }
[[ "${REPETITIONS}" =~ ^[1-9][0-9]*$ ]] || {
  echo "RIPETIZIONI deve essere un intero positivo" >&2
  exit 1
}

cd "${REPO_ROOT}"
for experiment in "${EXPERIMENTS[@]}"; do
  echo "[network-campaign] deploy ${experiment}"
  bash deploy/scripts/aws/deploy-pilot.sh "${experiment}"

  for ((repetition = 1; repetition <= REPETITIONS; repetition++)); do
    echo "[network-campaign] run ${repetition}/${REPETITIONS}: ${experiment}"
    bash deploy/scripts/aws/run-full.sh "${experiment}"
  done
done

#!/usr/bin/env bash
set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
readonly SCOPE="${1:-all}"
readonly REPETITIONS="${2:-3}"
readonly PROVISION_FLAG="${3:-}"

(( $# <= 3 )) || { echo "Uso: bash $0 [all|edge-cloud|cloud-global] [RIPETIZIONI] [--provision]" >&2; exit 1; }
[[ "${SCOPE}" =~ ^(all|edge-cloud|cloud-global)$ ]] || {
  echo "scope non valido: ${SCOPE}" >&2
  exit 1
}
[[ "${REPETITIONS}" =~ ^[1-9][0-9]*$ ]] || {
  echo "RIPETIZIONI deve essere un intero positivo" >&2
  exit 1
}
[[ -z "${PROVISION_FLAG}" || "${PROVISION_FLAG}" == "--provision" ]] || {
  echo "terzo argomento non valido: ${PROVISION_FLAG}" >&2
  exit 1
}

experiments=("experiments/network/network-baseline.yaml")
if [[ "${SCOPE}" == "all" || "${SCOPE}" == "edge-cloud" ]]; then
  experiments+=(
    "experiments/network/edge-cloud-delay-conservative.yaml"
    "experiments/network/edge-cloud-delay-bounded.yaml"
    "experiments/network/edge-cloud-bandwidth-conservative.yaml"
    "experiments/network/edge-cloud-bandwidth-bounded.yaml"
  )
fi
if [[ "${SCOPE}" == "all" || "${SCOPE}" == "cloud-global" ]]; then
  experiments+=(
    "experiments/network/cloud-global-delay-conservative.yaml"
    "experiments/network/cloud-global-delay-bounded.yaml"
    "experiments/network/cloud-global-bandwidth-conservative.yaml"
    "experiments/network/cloud-global-bandwidth-bounded.yaml"
  )
fi

cd "${REPO_ROOT}"
provision_pending="${PROVISION_FLAG}"
for experiment in "${experiments[@]}"; do
  echo "[network-campaign] deploy ${experiment}"
  bash deploy/scripts/aws/deploy-pilot.sh "${experiment}"

  for ((repetition = 1; repetition <= REPETITIONS; repetition++)); do
    echo "[network-campaign] run ${repetition}/${REPETITIONS}: ${experiment}"
    if [[ -n "${provision_pending}" ]]; then
      bash deploy/scripts/aws/run-full.sh "${experiment}" --provision
      provision_pending=""
    else
      bash deploy/scripts/aws/run-full.sh "${experiment}"
    fi
  done
done

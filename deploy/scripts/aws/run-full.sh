#!/usr/bin/env bash
set -Eeuo pipefail
set +x

export AWS_SCRIPT_LOG_PREFIX="run-full"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/aws-common.sh"

readonly DEFAULT_EXPERIMENT="experiments/cloud-scale-w1.yaml"

PROVISION=0
REFRESH=0
EXPERIMENT_INPUT=""
PLAN_FILE=""

cleanup() {
  [[ -z "${PLAN_FILE}" || ! -f "${PLAN_FILE}" ]] || rm -f -- "${PLAN_FILE}"
}
trap cleanup EXIT

usage() {
  cat <<'EOF'
Uso:
  bash deploy/scripts/aws/run-full.sh [EXPERIMENT_YAML]
  bash deploy/scripts/aws/run-full.sh [EXPERIMENT_YAML] --refresh
  bash deploy/scripts/aws/run-full.sh [EXPERIMENT_YAML] --provision

Senza flag:
  1. carica deploy/pilot.env;
  2. imposta EXPERIMENT_CONFIG;
  3. esegue run-experiment.sh.

--refresh:
  prima della run esegue terraform init + apply -refresh-only.

--provision:
  prima della run esegue terraform init + plan + apply e distribuisce
  l'applicazione sulle quattro EC2 risultanti.

Senza --provision, run-full.sh NON esegue deploygen e NON distribuisce codice
sulle EC2. Per aggiornare codice o configurazione di deployment usare:
  bash deploy/scripts/aws/deploy-pilot.sh EXPERIMENT_YAML
EOF
}

resolve_experiment() {
  local input="$1"
  local candidate

  if [[ "${input}" = /* ]]; then
    candidate="${input}"
  else
    candidate="${REPO_ROOT}/${input}"
  fi

  resolve_file "${candidate}" ||
    die "file esperimento non trovato: ${input}"
}

terraform_refresh() {
  require_command "${TERRAFORM_BIN}"

  log "Terraform refresh-only"
  "${TERRAFORM_BIN}" -chdir="${TERRAFORM_DIR}" init -input=false
  "${TERRAFORM_BIN}" -chdir="${TERRAFORM_DIR}" apply \
    -refresh-only \
    -auto-approve \
    -input=false
}

terraform_provision() {
  local answer

  require_command "${TERRAFORM_BIN}"

  mkdir -p "${REPO_ROOT}/.build/terraform"
  PLAN_FILE="$(mktemp "${REPO_ROOT}/.build/terraform/plan.XXXXXX")"
  chmod 0600 "${PLAN_FILE}"

  log "Terraform init/plan"
  "${TERRAFORM_BIN}" -chdir="${TERRAFORM_DIR}" init -input=false
  "${TERRAFORM_BIN}" -chdir="${TERRAFORM_DIR}" plan \
    -input=false \
    -out="${PLAN_FILE}"

  printf '\n'
  read -r -p "Applicare questo piano Terraform? [y/N] " answer
  case "${answer}" in
    y|Y|yes|YES) ;;
    *) die "terraform apply annullato" ;;
  esac

  log "Terraform apply"
  "${TERRAFORM_BIN}" -chdir="${TERRAFORM_DIR}" apply \
    -input=false \
    "${PLAN_FILE}"

  rm -f -- "${PLAN_FILE}"
  PLAN_FILE=""
}

deploy_after_provision() {
  log "deployment applicativo dopo il provisioning"
  bash "${SCRIPT_DIR}/deploy-pilot.sh" "${EXPERIMENT_CONFIG}"
}

main() {
  local arg

  for arg in "$@"; do
    case "${arg}" in
      --provision)
        PROVISION=1
        ;;
      --refresh)
        REFRESH=1
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      --*)
        die "opzione sconosciuta: ${arg}"
        ;;
      *)
        [[ -z "${EXPERIMENT_INPUT}" ]] ||
          die "specificare un solo file esperimento"
        EXPERIMENT_INPUT="${arg}"
        ;;
    esac
  done

  (( PROVISION == 0 || REFRESH == 0 )) ||
    die "--provision e --refresh non possono essere usati insieme"

  load_pilot_environment

  TERRAFORM_BIN="${TERRAFORM_BIN:-terraform}"
  TERRAFORM_DIR="${TERRAFORM_DIR:-${REPO_ROOT}/deploy/terraform}"
  if [[ "${TERRAFORM_DIR}" != /* ]]; then
    TERRAFORM_DIR="${REPO_ROOT}/${TERRAFORM_DIR}"
  fi

  EXPERIMENT_INPUT="${EXPERIMENT_INPUT:-${DEFAULT_EXPERIMENT}}"
  EXPERIMENT_CONFIG="$(resolve_experiment "${EXPERIMENT_INPUT}")"
  export EXPERIMENT_CONFIG

  if (( PROVISION == 1 )); then
    terraform_provision
    deploy_after_provision
  elif (( REFRESH == 1 )); then
    terraform_refresh
  fi

  log "avvio run-experiment: ${EXPERIMENT_CONFIG}"
  exec bash "${SCRIPT_DIR}/run-experiment.sh"
}

main "$@"

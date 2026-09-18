#!/usr/bin/env bash
set -Eeuo pipefail
set +x
umask 077

export AWS_SCRIPT_LOG_PREFIX="deploy-pilot"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/aws-common.sh"

readonly DEFAULT_EXPERIMENT="experiments/worker-scaling/cloud-scale-w1.yaml"
readonly COMPOSE_BUILD_DIR="${REPO_ROOT}/.build/aws-compose"

EXPERIMENT_INPUT=""
STAGING_ROOT=""
COMMON_ARCHIVE=""
REPLAY_ARCHIVE=""
NETWORK_ENABLED="false"

cleanup() {
  [[ -z "${STAGING_ROOT}" || ! -d "${STAGING_ROOT}" ]] || rm -rf -- "${STAGING_ROOT}"
}
trap cleanup EXIT

usage() {
  cat <<'EOF'
Uso:
  bash deploy/scripts/aws/deploy-pilot.sh [EXPERIMENT_YAML]

Esempio:
  bash deploy/scripts/aws/deploy-pilot.sh experiments/worker-scaling/cloud-scale-w4.yaml

Lo script:
  1. carica deploy/pilot.env;
  2. legge gli indirizzi prodotti da Terraform;
  3. genera i Compose distribuiti per l'esperimento;
  4. copia il software in /opt/continuum/current sulle quattro EC2;
  5. costruisce le immagini Docker e valida i Compose.

Non crea o modifica infrastruttura AWS: quello resta compito di Terraform.
Non avvia l'esperimento.
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

generate_deployment() {
  log "generazione Compose per ${EXPERIMENT_CONFIG}"
  rm -rf -- "${COMPOSE_BUILD_DIR}"
  mkdir -p "${COMPOSE_BUILD_DIR}"

  (
    cd "${REPO_ROOT}"
    go run ./cmd/deploygen \
      -mode distributed \
      -experiment "${EXPERIMENT_CONFIG}" \
      -distributed-output-dir "${COMPOSE_BUILD_DIR}"
  )

  for file in \
    cloud-core.generated.yml \
    workers.generated.yml \
    edge.generated.yml \
    simulator.generated.yml \
    generation-manifest.json; do
    [[ -s "${COMPOSE_BUILD_DIR}/${file}" ]] ||
      die "deploygen non ha prodotto ${file}"
  done
}

write_deployment_info() {
  local destination="$1"
  local git_commit dirty experiment_name

  git_commit="$(git -C "${REPO_ROOT}" rev-parse --verify HEAD 2>/dev/null || true)"
  [[ -n "${git_commit}" ]] || git_commit="unknown"
  if [[ -n "$(git -C "${REPO_ROOT}" status --porcelain --untracked-files=all)" ]]; then
    dirty=true
  else
    dirty=false
  fi

  experiment_name="$(
    cd "${REPO_ROOT}"
    go run ./cmd/runconfig --experiment "${EXPERIMENT_CONFIG}" --describe |
      jq -er '.experiment_name'
  )"

  jq -n \
    --arg deployed_at "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" \
    --arg git_commit_sha "${git_commit}" \
    --arg experiment "${experiment_name}" \
    --argjson git_dirty "${dirty}" \
    '{
      deployed_at: $deployed_at,
      git_commit_sha: $git_commit_sha,
      git_dirty: $git_dirty,
      experiment: $experiment
    }' >"${destination}"
}

prepare_common_archive() {
  local root

  STAGING_ROOT="$(mktemp -d)"
  root="${STAGING_ROOT}/common"

  mkdir -p \
    "${root}/cmd" \
    "${root}/internal" \
    "${root}/deploy/docker" \
    "${root}/deploy/mosquitto" \
    "${root}/deploy/postgres" \
    "${root}/deploy/scripts/aws" \
    "${root}/deploy/compose/distributed"

  cp "${REPO_ROOT}/go.mod" "${root}/"
  cp "${REPO_ROOT}/go.sum" "${root}/"
  cp "${REPO_ROOT}/.dockerignore" "${root}/"

  cp -R "${REPO_ROOT}/cmd/." "${root}/cmd/"
  cp -R "${REPO_ROOT}/internal/." "${root}/internal/"
  cp -R "${REPO_ROOT}/deploy/docker/." "${root}/deploy/docker/"
  cp -R "${REPO_ROOT}/deploy/mosquitto/." "${root}/deploy/mosquitto/"
  cp -R "${REPO_ROOT}/deploy/postgres/." "${root}/deploy/postgres/"
  cp "${REPO_ROOT}/deploy/scripts/aws/init-rds-schema.sh" \
    "${root}/deploy/scripts/aws/init-rds-schema.sh"
  cp "${COMPOSE_BUILD_DIR}/"*.generated.yml \
    "${COMPOSE_BUILD_DIR}/generation-manifest.json" \
    "${root}/deploy/compose/distributed/"

  write_deployment_info "${root}/deployment-info.json"

  COMMON_ARCHIVE="${STAGING_ROOT}/continuum-current.tar.gz"
  tar -C "${root}" -czf "${COMMON_ARCHIVE}" .
  chmod 0600 "${COMMON_ARCHIVE}"
}

prepare_replay_archive() {
  local edge_number shard

  mkdir -p "${STAGING_ROOT}/replay/dataset/derived/replay_by_edge"

  for ((edge_number = 0; edge_number < 13; edge_number++)); do
    shard="${REPO_ROOT}/dataset/derived/replay_by_edge/edge-${edge_number}.csv"
    [[ -f "${shard}" ]] || die "shard replay non trovato: ${shard}"
    cp "${shard}" "${STAGING_ROOT}/replay/dataset/derived/replay_by_edge/"
  done

  find "${STAGING_ROOT}/replay/dataset" -type d -exec chmod 0755 {} +
  find "${STAGING_ROOT}/replay/dataset" -type f -exec chmod 0644 {} +

  REPLAY_ARCHIVE="${STAGING_ROOT}/continuum-replay.tar.gz"
  tar -C "${STAGING_ROOT}/replay" -czf "${REPLAY_ARCHIVE}" dataset
  chmod 0600 "${REPLAY_ARCHIVE}"
}

write_role_env() {
  local role="$1"
  local destination="$2"

  printf 'DEPLOYMENT_ID=current\n' >"${destination}"

  case "${role}" in
    cloud-core)
      {
        printf 'KAFKA_ADVERTISED_HOST=%s\n' "${PRIVATE_IPS[cloud-core]}"
        printf 'GLOBAL_SINK_TYPE=postgres\n'
        printf 'GLOBAL_POSTGRES_HOST=%s\n' "$(jq -jer '.host' <<<"${RDS_CONNECTION}")"
        printf 'GLOBAL_POSTGRES_PORT=%s\n' "$(jq -jer '.port' <<<"${RDS_CONNECTION}")"
        printf 'GLOBAL_POSTGRES_DATABASE=%s\n' "$(jq -jer '.database' <<<"${RDS_CONNECTION}")"
        printf 'GLOBAL_POSTGRES_USER=%s\n' "$(jq -jer '.username' <<<"${RDS_CONNECTION}")"
        printf 'GLOBAL_POSTGRES_PASSWORD=%s\n' "${TF_VAR_rds_password}"
        printf 'GLOBAL_POSTGRES_SSLMODE=verify-full\n'
      } >>"${destination}"
      ;;
    workers|edge)
      printf 'CLOUD_KAFKA_HOST=%s\n' "${PRIVATE_IPS[cloud-core]}" >>"${destination}"
      ;;
    simulator)
      {
        printf 'EDGE_HOST=%s\n' "${PRIVATE_IPS[edge]}"
        printf '# REPLAY_START_AT viene aggiunto da run-experiment.sh al momento della run.\n'
      } >>"${destination}"
      ;;
    *)
      die "ruolo non supportato: ${role}"
      ;;
  esac

  printf '%s' "${RESOURCE_PROFILE_VALUES}" >>"${destination}"
  chmod 0600 "${destination}"
}

stop_existing_stack() {
  local role="$1"
  local host="${PUBLIC_IPS[${role}]}"

  ssh_run "${host}" bash -s -- "${role}" <<'REMOTE'
set -euo pipefail
role="$1"
root="/opt/continuum/current"

[[ -e "${root}" || -L "${root}" ]] || exit 0
[[ -f "${root}/.env" ]] || exit 0

cd "${root}"
case "${role}" in
  simulator)
    if [[ -f deploy/compose/distributed/simulator.generated.yml ]]; then
      REPLAY_START_AT=1970-01-01T00:00:00Z docker compose \
        --env-file .env --profile replay \
        -f deploy/compose/distributed/simulator.generated.yml \
        down --remove-orphans --timeout 30 || true
    fi
    ;;
  edge)
    [[ ! -f deploy/compose/distributed/edge.generated.yml ]] ||
      docker compose --env-file .env \
        -f deploy/compose/distributed/edge.generated.yml \
        down --remove-orphans --timeout 30 || true
    ;;
  workers)
    [[ ! -f deploy/compose/distributed/workers.generated.yml ]] ||
      docker compose --env-file .env \
        -f deploy/compose/distributed/workers.generated.yml \
        down --remove-orphans --timeout 30 || true
    ;;
  cloud-core)
    [[ ! -f deploy/compose/distributed/cloud-core.generated.yml ]] ||
      docker compose --env-file .env \
        -f deploy/compose/distributed/cloud-core.generated.yml \
        down --remove-orphans --timeout 30 || true
    ;;
esac
REMOTE
}

install_common_tree() {
  local role="$1"
  local host="${PUBLIC_IPS[${role}]}"
  local remote_archive="/tmp/continuum-current-${role}.tar.gz"

  log "copia sorgenti su ${role}"
  scp_to_host "${COMMON_ARCHIVE}" "${host}" "${remote_archive}"

  ssh_run "${host}" bash -s -- "${remote_archive}" <<'REMOTE'
set -euo pipefail
archive="$1"
sudo install -d -m 0755 /opt/continuum
sudo chown "$(id -u):$(id -g)" /opt/continuum
rm -rf /opt/continuum/current
mkdir -m 0755 /opt/continuum/current
tar -xzf "${archive}" -C /opt/continuum/current
rm -f "${archive}"
REMOTE
}

install_role_env() {
  local role="$1"
  local host="${PUBLIC_IPS[${role}]}"
  local local_env="${STAGING_ROOT}/${role}.env"
  local remote_env="/tmp/continuum-${role}.env"

  write_role_env "${role}" "${local_env}"
  scp_to_host "${local_env}" "${host}" "${remote_env}"

  ssh_run "${host}" bash -s -- "${remote_env}" <<'REMOTE'
set -euo pipefail
install -m 0600 "$1" /opt/continuum/current/.env
rm -f "$1"
REMOTE
}

install_replay_data() {
  local host="${PUBLIC_IPS[simulator]}"
  local remote_archive="/tmp/continuum-replay.tar.gz"

  log "copia replay shard su simulator"
  scp_to_host "${REPLAY_ARCHIVE}" "${host}" "${remote_archive}"

  ssh_run "${host}" bash -s -- "${remote_archive}" <<'REMOTE'
set -euo pipefail
cd /opt/continuum/current
tar -xzf "$1"
rm -f "$1"
REMOTE
}

build_role() {
  local role="$1"
  local host="${PUBLIC_IPS[${role}]}"

  log "build e validazione Docker su ${role}"

  case "${role}" in
    cloud-core)
      ssh_run "${host}" 'set -euo pipefail
cd /opt/continuum/current
docker pull apache/kafka:4.3.0
docker tag apache/kafka:4.3.0 continuum-kafka:current
docker build --pull -f deploy/docker/global-aggregator.Dockerfile -t continuum-global-aggregator:current .
docker compose --env-file .env -f deploy/compose/distributed/cloud-core.generated.yml config --quiet'
      ;;
    workers)
      ssh_run "${host}" 'set -euo pipefail
cd /opt/continuum/current
docker build --pull -f deploy/docker/cloud-worker.Dockerfile -t continuum-cloud-worker:current .
docker compose --env-file .env -f deploy/compose/distributed/workers.generated.yml config --quiet'
      ;;
    edge)
      ssh_run "${host}" 'set -euo pipefail
cd /opt/continuum/current
docker pull eclipse-mosquitto:2
docker tag eclipse-mosquitto:2 continuum-mosquitto:current
docker build --pull -f deploy/docker/edge.Dockerfile -t continuum-edge:current .
docker compose --env-file .env -f deploy/compose/distributed/edge.generated.yml config --quiet'
      ;;
    simulator)
      ssh_run "${host}" 'set -euo pipefail
cd /opt/continuum/current
docker build --pull -f deploy/docker/simulator.Dockerfile -t continuum-simulator:current .
REPLAY_START_AT=1970-01-01T00:00:00Z docker compose \
  --env-file .env --profile replay \
  -f deploy/compose/distributed/simulator.generated.yml \
  config --quiet'
      ;;
  esac
}

ensure_netem_dependencies() {
  local role host

  [[ "${NETWORK_ENABLED}" == "true" ]] || return 0
  for role in simulator edge; do
    host="${PUBLIC_IPS[${role}]}"
    log "verifica dipendenze tc-netem su ${role}"
    ssh_run "${host}" 'set -euo pipefail
if ! command -v tc >/dev/null 2>&1 || ! command -v ip >/dev/null 2>&1 || ! command -v nsenter >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    sudo -n apt-get update -y
    sudo -n apt-get install -y iproute2 util-linux
  elif command -v dnf >/dev/null 2>&1; then
    sudo -n dnf install -y iproute util-linux
  elif command -v yum >/dev/null 2>&1; then
    sudo -n yum install -y iproute util-linux
  else
    echo "package manager non supportato per installare tc/ip/nsenter" >&2
    exit 1
  fi
fi
command -v tc >/dev/null
command -v ip >/dev/null
command -v nsenter >/dev/null
sudo -n true'
  done
}

main() {
  local role

  (( $# <= 1 )) || die "specificare un solo file esperimento"

  case "${1:-}" in
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      die "opzione sconosciuta: $1"
      ;;
  esac

  EXPERIMENT_INPUT="${1:-${DEFAULT_EXPERIMENT}}"

  require_command go
  require_command git
  require_command jq
  require_command ssh
  require_command scp
  require_command tar

  init_aws_context
  require_command "${TERRAFORM_BIN}"

  EXPERIMENT_CONFIG="$(resolve_experiment "${EXPERIMENT_INPUT}")"
  export EXPERIMENT_CONFIG
  NETWORK_ENABLED="$(
    cd "${REPO_ROOT}"
    go run ./cmd/runconfig --experiment "${EXPERIMENT_CONFIG}" --describe |
      jq -r '
        if (.network.enabled | type) == "boolean" then
          .network.enabled
        else
          error("network.enabled must be boolean")
        end
      '
  )"

  load_terraform_addresses
  load_rds_configuration
  wait_for_all_ssh

  for role in "${ROLES[@]}"; do
    verify_host_runtime "${role}"
  done
  ensure_netem_dependencies

  generate_deployment
  prepare_common_archive
  prepare_replay_archive

  # Prima fermiamo tutti i servizi della vecchia versione; poi sostituiamo
  # direttamente /opt/continuum/current. Non esistono release versionate.
  for role in "${ROLES[@]}"; do
    stop_existing_stack "${role}"
  done

  for role in "${ROLES[@]}"; do
    install_common_tree "${role}"
    install_role_env "${role}"
  done

  install_replay_data

  for role in "${ROLES[@]}"; do
    build_role "${role}"
  done

  printf '\nDeployment applicativo completato.\n'
  printf 'Experiment: %s\n' "${EXPERIMENT_CONFIG}"
  printf 'Remote path: %s/current\n' "${REMOTE_ROOT}"
  printf 'Ora puoi eseguire:\n'
  printf '  bash deploy/scripts/aws/run-full.sh %q\n' "${EXPERIMENT_INPUT}"
}

main "$@"

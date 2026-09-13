#!/usr/bin/env bash
# Funzioni comuni ai runner AWS.
# La configurazione locale vive esclusivamente in deploy/pilot.env.

readonly AWS_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly REPO_ROOT="$(cd "${AWS_COMMON_DIR}/../../.." && pwd -P)"
readonly PILOT_ENV="${CONTINUUM_PILOT_ENV:-${REPO_ROOT}/deploy/pilot.env}"
readonly REMOTE_ROOT="/opt/continuum"
readonly -a ROLES=(simulator edge cloud-core workers)

declare -A PUBLIC_IPS
declare -A PRIVATE_IPS
declare -a SSH_ARGS

# Preserve exported overrides when one runner starts another one. Each direct
# invocation still receives defaults in init_aws_context.
TERRAFORM_BIN="${TERRAFORM_BIN:-}"
TERRAFORM_DIR="${TERRAFORM_DIR:-}"
RESOURCE_PROFILE_PATH=""
RESOURCE_PROFILE_VALUES=""
SSH_KEY=""
SSH_WAIT_ATTEMPTS_VALUE=""
SSH_WAIT_INTERVAL_SECONDS_VALUE=""
TIME_SYNC_ATTEMPTS_VALUE=""
RDS_CONNECTION='{}'

log() {
  printf '[%s] %s\n' "${AWS_SCRIPT_LOG_PREFIX:-aws}" "$*"
}

die() {
  printf '[%s] ERROR: %s\n' "${AWS_SCRIPT_LOG_PREFIX:-aws}" "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 ||
    die "comando richiesto non trovato: $1"
}

resolve_file() {
  local path="$1"
  local directory
  local filename

  directory="$(cd "$(dirname "${path}")" 2>/dev/null && pwd -P)" || return 1
  filename="$(basename "${path}")"
  [[ -f "${directory}/${filename}" ]] || return 1
  printf '%s/%s\n' "${directory}" "${filename}"
}


resolve_repo_file() {
  local path="$1"
  if [[ "${path}" = /* ]]; then
    resolve_file "${path}"
  else
    resolve_file "${REPO_ROOT}/${path}"
  fi
}

load_pilot_environment() {
  [[ "${CONTINUUM_PILOT_ENV_LOADED:-0}" != "1" ]] || return 0
  [[ -f "${PILOT_ENV}" ]] || die "file di ambiente non trovato: ${PILOT_ENV}"

  set +x
  set -a
  # shellcheck disable=SC1090
  source "${PILOT_ENV}"
  set +a
  set +x

  export CONTINUUM_PILOT_ENV_LOADED=1
}

load_resource_profile() {
  local input="${RESOURCE_PROFILE:-${REPO_ROOT}/deploy/resources/aws-pilot.env}"
  local line key value prefix suffix
  local -a prefixes=(SIMULATOR EDGE MQTT CLOUD_WORKER KAFKA KAFKA_INIT GLOBAL COORDINATOR)
  declare -A values=()

  RESOURCE_PROFILE_PATH="$(resolve_repo_file "${input}")" ||
    die "resource profile non trovato: ${input}"

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%$'\r'}"
    [[ -z "${line}" || "${line}" == \#* ]] && continue

    [[ "${line}" =~ ^(SIMULATOR|EDGE|MQTT|CLOUD_WORKER|KAFKA|KAFKA_INIT|GLOBAL|COORDINATOR)_(CPUS|MEMORY)=([0-9.mMgG]+)$ ]] ||
      die "resource profile: assegnazione non valida: ${line}"

    key="${line%%=*}"
    value="${line#*=}"
    [[ -z "${values[${key}]:-}" ]] || die "resource profile: chiave duplicata ${key}"

    if [[ "${key}" == *_CPUS ]]; then
      [[ "${value}" =~ ^[0-9]+([.][0-9]+)?$ ]] &&
        awk -v value="${value}" 'BEGIN { exit !(value > 0) }' ||
        die "CPU non valide per ${key}"
    else
      [[ "${value}" =~ ^[1-9][0-9]*[mMgG]$ ]] ||
        die "memoria non valida per ${key} (usare m o g)"
    fi
    values["${key}"]="${value}"
  done <"${RESOURCE_PROFILE_PATH}"

  values[COORDINATOR_CPUS]="${values[COORDINATOR_CPUS]:-0.1}"
  values[COORDINATOR_MEMORY]="${values[COORDINATOR_MEMORY]:-64m}"

  RESOURCE_PROFILE_VALUES=""
  for prefix in "${prefixes[@]}"; do
    for suffix in CPUS MEMORY; do
      key="${prefix}_${suffix}"
      [[ -n "${values[${key}]:-}" ]] || die "resource profile: manca ${key}"
      RESOURCE_PROFILE_VALUES+="${key}=${values[${key}]}"$'\n'
    done
  done
}

init_aws_context() {
  load_pilot_environment

  TERRAFORM_BIN="${TERRAFORM_BIN:-terraform}"
  TERRAFORM_DIR="${TERRAFORM_DIR:-${REPO_ROOT}/deploy/terraform}"
  if [[ "${TERRAFORM_DIR}" != /* ]]; then
    TERRAFORM_DIR="${REPO_ROOT}/${TERRAFORM_DIR}"
  fi
  SSH_WAIT_ATTEMPTS_VALUE="${SSH_WAIT_ATTEMPTS:-60}"
  SSH_WAIT_INTERVAL_SECONDS_VALUE="${SSH_WAIT_INTERVAL_SECONDS:-5}"
  TIME_SYNC_ATTEMPTS_VALUE="${TIME_SYNC_ATTEMPTS:-24}"

  [[ -d "${TERRAFORM_DIR}" ]] || die "directory Terraform non trovata: ${TERRAFORM_DIR}"
  [[ -n "${SSH_USER:-}" ]] || die "SSH_USER non impostato in deploy/pilot.env"
  [[ -n "${SSH_KEY_PATH:-}" ]] || die "SSH_KEY_PATH non impostato in deploy/pilot.env"
  [[ "${SSH_WAIT_ATTEMPTS_VALUE}" =~ ^[1-9][0-9]*$ ]] ||
    die "SSH_WAIT_ATTEMPTS deve essere un intero positivo"
  [[ "${SSH_WAIT_INTERVAL_SECONDS_VALUE}" =~ ^[1-9][0-9]*$ ]] ||
    die "SSH_WAIT_INTERVAL_SECONDS deve essere un intero positivo"
  [[ "${TIME_SYNC_ATTEMPTS_VALUE}" =~ ^[1-9][0-9]*$ ]] ||
    die "TIME_SYNC_ATTEMPTS deve essere un intero positivo"

  SSH_KEY="$(resolve_repo_file "${SSH_KEY_PATH}")" ||
    die "chiave SSH non trovata: ${SSH_KEY_PATH}"

  case "${SSH_KEY}" in
    "${REPO_ROOT}"|"${REPO_ROOT}"/*)
      die "la chiave SSH deve rimanere fuori dal repository"
      ;;
  esac

  SSH_ARGS=(
    -i "${SSH_KEY}"
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o StrictHostKeyChecking=accept-new
  )

  load_resource_profile
}

terraform_output() {
  "${TERRAFORM_BIN}" "-chdir=${TERRAFORM_DIR}" output -json "$1"
}

read_address() {
  local json="$1"
  local role="$2"
  local output_name="$3"
  local value

  value="$(jq -er --arg role "${role}" '.[$role] | select(type == "string" and length > 0)' <<<"${json}")" ||
    die "output Terraform ${output_name} mancante o vuoto per ${role}"
  printf '%s\n' "${value}"
}

load_terraform_addresses() {
  local public_json private_json role

  public_json="$(terraform_output public_ips)" ||
    die "impossibile leggere public_ips dagli output Terraform"
  private_json="$(terraform_output private_ips)" ||
    die "impossibile leggere private_ips dagli output Terraform"

  for role in "${ROLES[@]}"; do
    PUBLIC_IPS["${role}"]="$(read_address "${public_json}" "${role}" public_ips)"
    PRIVATE_IPS["${role}"]="$(read_address "${private_json}" "${role}" private_ips)"
    log "indirizzo ${role}: public=${PUBLIC_IPS["${role}"]} private=${PRIVATE_IPS["${role}"]}"
  done
}

load_rds_configuration() {
  RDS_CONNECTION="$(terraform_output rds_connection)" ||
    die "output Terraform rds_connection assente: creare prima RDS"

  jq -e '
    (.host | type == "string" and length > 0) and
    (.port == 5432) and
    (.database | type == "string" and length > 0) and
    (.username | type == "string" and length > 0)
  ' <<<"${RDS_CONNECTION}" >/dev/null ||
    die "output rds_connection non valido"

  [[ "${TF_VAR_rds_password:-}" =~ ^[A-Za-z0-9_+=.!-]{16,128}$ ]] ||
    die "TF_VAR_rds_password mancante/non valida in deploy/pilot.env"
}

ssh_run() {
  local host="$1"
  shift
  ssh "${SSH_ARGS[@]}" "${SSH_USER}@${host}" "$@"
}

scp_to_host() {
  local source="$1"
  local host="$2"
  local destination="$3"
  scp -p -q "${SSH_ARGS[@]}" "${source}" "${SSH_USER}@${host}:${destination}"
}

wait_for_all_ssh() {
  local attempt role reachable_count
  declare -A reachable=()

  log "attesa SSH sulle quattro istanze"

  for ((attempt = 1; attempt <= SSH_WAIT_ATTEMPTS_VALUE; attempt++)); do
    reachable_count=0
    for role in "${ROLES[@]}"; do
      if [[ "${reachable[${role}]:-}" == "yes" ]]; then
        ((reachable_count += 1))
        continue
      fi
      if ssh_run "${PUBLIC_IPS[${role}]}" true >/dev/null 2>&1; then
        reachable["${role}"]="yes"
        ((reachable_count += 1))
        log "SSH raggiungibile: ${role} (${PUBLIC_IPS[${role}]})"
      fi
    done

    (( reachable_count == ${#ROLES[@]} )) && return 0
    (( attempt < SSH_WAIT_ATTEMPTS_VALUE )) &&
      sleep "${SSH_WAIT_INTERVAL_SECONDS_VALUE}"
  done

  die "timeout in attesa delle quattro istanze via SSH"
}

wait_for_time_sync() {
  local role="$1"
  local host="${PUBLIC_IPS[${role}]}"
  local attempt

  for ((attempt = 1; attempt <= TIME_SYNC_ATTEMPTS_VALUE; attempt++)); do
    if ssh_run "${host}" 'set -euo pipefail
if command -v timedatectl >/dev/null 2>&1; then
  state="$(timedatectl show --property=NTPSynchronized --value)"
  [[ "${state}" == "yes" || "${state}" == "true" ]]
elif command -v chronyc >/dev/null 2>&1; then
  chronyc tracking | grep -Eq "^Leap status[[:space:]]*:[[:space:]]*Normal$"
else
  exit 2
fi' >/dev/null 2>&1; then
      return 0
    fi

    (( attempt < TIME_SYNC_ATTEMPTS_VALUE )) &&
      sleep "${SSH_WAIT_INTERVAL_SECONDS_VALUE}"
  done

  die "sincronizzazione orologio non attiva su ${role}"
}

verify_host_runtime() {
  local role="$1"
  local host="${PUBLIC_IPS[${role}]}"

  log "verifica host ${role}"
  ssh_run "${host}" 'set -euo pipefail
if command -v cloud-init >/dev/null 2>&1; then
  sudo cloud-init status --wait >/dev/null
fi
command -v docker >/dev/null
systemctl is-active --quiet docker
docker compose version >/dev/null
docker info >/dev/null
command -v curl >/dev/null
command -v timeout >/dev/null'

  wait_for_time_sync "${role}"
}

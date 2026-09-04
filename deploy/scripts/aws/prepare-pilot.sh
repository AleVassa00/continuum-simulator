#!/usr/bin/env bash
set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
readonly REMOTE_ROOT="/opt/continuum"

readonly TERRAFORM_BIN="${TERRAFORM_BIN:-terraform}"
readonly TERRAFORM_DIR="${TERRAFORM_DIR:-${REPO_ROOT}/deploy/terraform}"
readonly GENERATION_MANIFEST_PATH="${REPO_ROOT}/deploy/compose/distributed/generation-manifest.json"
readonly SSH_USER="${SSH_USER:-}"
readonly SSH_KEY_INPUT="${SSH_KEY_PATH:-}"
readonly SSH_WAIT_ATTEMPTS="${SSH_WAIT_ATTEMPTS:-60}"
readonly SSH_WAIT_INTERVAL_SECONDS="${SSH_WAIT_INTERVAL_SECONDS:-5}"
readonly TIME_SYNC_ATTEMPTS="${TIME_SYNC_ATTEMPTS:-24}"
readonly DEPLOYMENT_ID="${DEPLOYMENT_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
readonly LOG_PREFIX="${AWS_SCRIPT_LOG_PREFIX:-prepare-pilot}"

readonly -a ROLES=(simulator edge cloud-core workers)

declare -A PUBLIC_IPS
declare -A PRIVATE_IPS
declare -A PREVIOUS_CURRENT_TARGETS
declare -a SSH_ARGS

STAGING_ROOT=""
SSH_KEY=""
GIT_COMMIT_SHA=""
GENERATION_MANIFEST_SHA256=""
REPLAY_SHARD_SHA256='{}'
IMAGES_BY_ROLE='{}'
RELEASE_MANIFEST_PATH=""

log() {
  printf '[%s] %s\n' "${LOG_PREFIX}" "$*"
}

die() {
  printf '[%s] ERROR: %s\n' "${LOG_PREFIX}" "$*" >&2
  exit 1
}

cleanup() {
  if [[ -n "${STAGING_ROOT}" && -d "${STAGING_ROOT}" ]]; then
    rm -rf -- "${STAGING_ROOT}"
  fi
}

trap cleanup EXIT

require_command() {
  local command_name="$1"

  command -v "${command_name}" >/dev/null 2>&1 ||
    die "comando richiesto non trovato: ${command_name}"
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

validate_inputs() {
  [[ -n "${SSH_USER}" ]] ||
    die "SSH_USER e obbligatorio (per esempio ubuntu oppure ec2-user)"
  [[ "${SSH_USER}" =~ ^[a-z_][a-z0-9_-]*$ ]] ||
    die "SSH_USER contiene caratteri non validi"
  [[ -n "${SSH_KEY_INPUT}" ]] ||
    die "SSH_KEY_PATH e obbligatorio e deve indicare una chiave privata esterna al repository"
  [[ "${SSH_WAIT_ATTEMPTS}" =~ ^[1-9][0-9]*$ ]] ||
    die "SSH_WAIT_ATTEMPTS deve essere un intero positivo"
  [[ "${SSH_WAIT_INTERVAL_SECONDS}" =~ ^[1-9][0-9]*$ ]] ||
    die "SSH_WAIT_INTERVAL_SECONDS deve essere un intero positivo"
  [[ "${TIME_SYNC_ATTEMPTS}" =~ ^[1-9][0-9]*$ ]] ||
    die "TIME_SYNC_ATTEMPTS deve essere un intero positivo"
  [[ "${DEPLOYMENT_ID}" =~ ^[A-Za-z0-9._-]+$ ]] ||
    die "DEPLOYMENT_ID puo contenere solo lettere, numeri, punto, underscore e trattino"
  [[ -d "${TERRAFORM_DIR}" ]] ||
    die "directory Terraform non trovata: ${TERRAFORM_DIR}"
  [[ -f "${GENERATION_MANIFEST_PATH}" ]] ||
    die "generation manifest non trovato: eseguire deploygen --mode distributed"

  SSH_KEY="$(resolve_file "${SSH_KEY_INPUT}")" ||
    die "chiave SSH non trovata: ${SSH_KEY_INPUT}"
  case "${SSH_KEY}" in
    "${REPO_ROOT}" | "${REPO_ROOT}"/*)
      die "la chiave SSH deve rimanere fuori dal repository: ${SSH_KEY}"
      ;;
  esac

  SSH_ARGS=(
    -i "${SSH_KEY}"
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o StrictHostKeyChecking=accept-new
  )
}

validate_release_source() {
  local filename
  local expected
  local actual
  local source_status

  [[ "$(jq -er '.schema_version' "${GENERATION_MANIFEST_PATH}")" == "1" ]] ||
    die "schema version del generation manifest non supportata"
  [[ "$(jq -er '.config_sha256 | select(type == "string" and length == 64)' "${GENERATION_MANIFEST_PATH}")" =~ ^[0-9a-f]{64}$ ]] ||
    die "config_sha256 mancante o non valido nel generation manifest"
  [[ "$(jq -er '.topology_sha256 | select(type == "string" and length == 64)' "${GENERATION_MANIFEST_PATH}")" =~ ^[0-9a-f]{64}$ ]] ||
    die "topology_sha256 mancante o non valido nel generation manifest"
  [[ "$(jq -er '.compose_sha256 | length' "${GENERATION_MANIFEST_PATH}")" == "4" ]] ||
    die "il generation manifest deve contenere esattamente quattro checksum Compose"
  jq -e '.resolved_config_yaml | type == "string" and length > 0' \
    "${GENERATION_MANIFEST_PATH}" >/dev/null ||
    die "configurazione risolta mancante nel generation manifest"

  for filename in \
    cloud-core.generated.yml \
    workers.generated.yml \
    edge.generated.yml \
    simulator.generated.yml; do
    expected="$(jq -er --arg filename "${filename}" '.compose_sha256[$filename]' "${GENERATION_MANIFEST_PATH}")" ||
      die "checksum mancante per ${filename} nel generation manifest"
    actual="$(sha256sum "${REPO_ROOT}/deploy/compose/distributed/${filename}" | awk '{print $1}')"
    [[ "${actual}" == "${expected}" ]] ||
      die "${filename} non corrisponde al generation manifest; rieseguire deploygen"
  done

  GIT_COMMIT_SHA="$(git -C "${REPO_ROOT}" rev-parse --verify HEAD)" ||
    die "impossibile determinare il commit Git"
  source_status="$(git -C "${REPO_ROOT}" status --porcelain --untracked-files=all)"
  [[ -z "${source_status}" ]] ||
    die "il deployment richiede un worktree Git pulito affinche il commit identifichi integralmente il codice"

  GENERATION_MANIFEST_SHA256="$(sha256sum "${GENERATION_MANIFEST_PATH}" | awk '{print $1}')"
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
    die "output Terraform ${output_name} mancante o vuoto per il ruolo ${role}"
  printf '%s\n' "${value}"
}

load_terraform_addresses() {
  local public_json
  local private_json
  local role

  public_json="$(terraform_output public_ips)" ||
    die "impossibile leggere public_ips dagli output Terraform; verificare che terraform apply sia gia stato eseguito"
  private_json="$(terraform_output private_ips)" ||
    die "impossibile leggere private_ips dagli output Terraform; verificare che terraform apply sia gia stato eseguito"

  for role in "${ROLES[@]}"; do
    PUBLIC_IPS["${role}"]="$(read_address "${public_json}" "${role}" public_ips)"
    PRIVATE_IPS["${role}"]="$(read_address "${private_json}" "${role}" private_ips)"
  done
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

  scp -q "${SSH_ARGS[@]}" "${source}" "${SSH_USER}@${host}:${destination}"
}

wait_for_all_ssh() {
  local attempt
  local role
  local reachable_count
  declare -A reachable=()

  for ((attempt = 1; attempt <= SSH_WAIT_ATTEMPTS; attempt++)); do
    reachable_count=0

    for role in "${ROLES[@]}"; do
      if [[ "${reachable["${role}"]:-}" == "yes" ]]; then
        ((reachable_count += 1))
        continue
      fi

      if ssh_run "${PUBLIC_IPS["${role}"]}" true >/dev/null 2>&1; then
        reachable["${role}"]="yes"
        ((reachable_count += 1))
        log "SSH raggiungibile: ${role} (${PUBLIC_IPS["${role}"]})"
      fi
    done

    if ((reachable_count == ${#ROLES[@]})); then
      return 0
    fi

    if ((attempt < SSH_WAIT_ATTEMPTS)); then
      sleep "${SSH_WAIT_INTERVAL_SECONDS}"
    fi
  done

  for role in "${ROLES[@]}"; do
    if [[ "${reachable["${role}"]:-}" != "yes" ]]; then
      log "SSH non raggiungibile: ${role} (${PUBLIC_IPS["${role}"]})"
    fi
  done
  die "timeout in attesa delle quattro istanze via SSH"
}

verify_host_runtime() {
  local role="$1"
  local host="${PUBLIC_IPS["${role}"]}"

  log "attesa completamento cloud-init su ${role}"
  ssh_run "${host}" 'set -euo pipefail
if command -v cloud-init >/dev/null 2>&1; then
  sudo cloud-init status --wait >/dev/null
fi'

  # A fresh SSH login is required to observe docker-group membership added by cloud-init.
  log "verifica Docker su ${role}"
  ssh_run "${host}" 'set -euo pipefail
command -v docker >/dev/null
systemctl is-active --quiet docker
docker --version
docker compose version
docker info >/dev/null
command -v curl >/dev/null
command -v timeout >/dev/null
command -v sha256sum >/dev/null'

  wait_for_time_sync "${role}"
}

wait_for_time_sync() {
  local role="$1"
  local host="${PUBLIC_IPS["${role}"]}"
  local attempt

  log "verifica sincronizzazione orologio su ${role}"
  for ((attempt = 1; attempt <= TIME_SYNC_ATTEMPTS; attempt++)); do
    if ssh_run "${host}" 'set -euo pipefail
if command -v timedatectl >/dev/null 2>&1; then
  state="$(timedatectl show --property=NTPSynchronized --value)"
  [[ "${state}" == "yes" || "${state}" == "true" ]]
elif command -v chronyc >/dev/null 2>&1; then
  chronyc tracking | grep -Eq '^Leap status[[:space:]]*:[[:space:]]*Normal$'
else
  echo "nessun controllo NTP supportato trovato (timedatectl o chronyc)" >&2
  exit 2
fi' >/dev/null 2>&1; then
      return 0
    fi

    if ((attempt < TIME_SYNC_ATTEMPTS)); then
      sleep "${SSH_WAIT_INTERVAL_SECONDS}"
    fi
  done

  die "sincronizzazione dell'orologio non attiva su ${role}"
}

copy_common_build_context() {
  local destination="$1"

  cp "${REPO_ROOT}/go.mod" "${destination}/go.mod"
  cp "${REPO_ROOT}/go.sum" "${destination}/go.sum"
  mkdir -p \
    "${destination}/cmd" \
    "${destination}/deploy/docker" \
    "${destination}/deploy/compose/distributed" \
    "${destination}/internal"
}

copy_internal_packages() {
  local destination="$1"
  shift
  local package_name

  for package_name in "$@"; do
    cp -R \
      "${REPO_ROOT}/internal/${package_name}" \
      "${destination}/internal/${package_name}"
  done
}

write_runtime_environment() {
  local role="$1"
  local destination="$2"

  printf 'DEPLOYMENT_ID=%s\n' "${DEPLOYMENT_ID}" >"${destination}/.env"

  case "${role}" in
    cloud-core)
      printf 'KAFKA_ADVERTISED_HOST=%s\n' "${PRIVATE_IPS[cloud-core]}" >>"${destination}/.env"
      ;;
    workers | edge)
      printf 'CLOUD_KAFKA_HOST=%s\n' "${PRIVATE_IPS[cloud-core]}" >>"${destination}/.env"
      ;;
    simulator)
      {
        printf 'EDGE_HOST=%s\n' "${PRIVATE_IPS[edge]}"
        printf '# REPLAY_START_AT must be supplied only when the replay is started.\n'
      } >>"${destination}/.env"
      ;;
    *)
      die "ruolo non supportato durante la generazione dell'environment: ${role}"
      ;;
  esac

  chmod 0644 "${destination}/.env"
}

stage_role() {
  local role="$1"
  local destination="${STAGING_ROOT}/${role}"
  local edge_number
  local shard

  mkdir -p "${destination}"
  copy_common_build_context "${destination}"
  write_runtime_environment "${role}" "${destination}"
  cp "${GENERATION_MANIFEST_PATH}" "${destination}/deploy/compose/distributed/"

  case "${role}" in
    cloud-core)
      copy_internal_packages \
        "${destination}" \
        model \
        cloudworker \
        envutil \
        globalaggregator \
        kafkautil
      cp -R "${REPO_ROOT}/cmd/global-aggregator" "${destination}/cmd/global-aggregator"
      cp "${REPO_ROOT}/deploy/docker/global-aggregator.Dockerfile" "${destination}/deploy/docker/"
      cp "${REPO_ROOT}/deploy/compose/distributed/cloud-core.generated.yml" "${destination}/deploy/compose/distributed/"
      ;;
    workers)
      copy_internal_packages \
        "${destination}" \
        model \
        cloudworker \
        envutil \
        kafkautil
      cp -R "${REPO_ROOT}/cmd/cloud-worker" "${destination}/cmd/cloud-worker"
      cp "${REPO_ROOT}/deploy/docker/cloud-worker.Dockerfile" "${destination}/deploy/docker/"
      cp "${REPO_ROOT}/deploy/compose/distributed/workers.generated.yml" "${destination}/deploy/compose/distributed/"
      ;;
    edge)
      copy_internal_packages "${destination}" model mqtttopic
      cp -R "${REPO_ROOT}/cmd/edge" "${destination}/cmd/edge"
      cp "${REPO_ROOT}/deploy/docker/edge.Dockerfile" "${destination}/deploy/docker/"
      cp "${REPO_ROOT}/deploy/compose/distributed/edge.generated.yml" "${destination}/deploy/compose/distributed/"
      mkdir -p "${destination}/deploy/mosquitto"
      cp "${REPO_ROOT}/deploy/mosquitto/mosquitto.conf" "${destination}/deploy/mosquitto/"
      ;;
    simulator)
      copy_internal_packages "${destination}" model mqtttopic
      cp -R "${REPO_ROOT}/cmd/simulator" "${destination}/cmd/simulator"
      cp "${REPO_ROOT}/deploy/docker/simulator.Dockerfile" "${destination}/deploy/docker/"
      cp "${REPO_ROOT}/deploy/compose/distributed/simulator.generated.yml" "${destination}/deploy/compose/distributed/"
      mkdir -p "${destination}/dataset/derived/replay_by_edge"
      for ((edge_number = 0; edge_number < 13; edge_number++)); do
        shard="${REPO_ROOT}/dataset/derived/replay_by_edge/edge-${edge_number}.csv"
        [[ -f "${shard}" ]] || die "shard replay richiesto non trovato: ${shard}"
        cp "${shard}" "${destination}/dataset/derived/replay_by_edge/"
        REPLAY_SHARD_SHA256="$(jq -cn \
          --argjson current "${REPLAY_SHARD_SHA256}" \
          --arg filename "edge-${edge_number}.csv" \
          --arg digest "$(sha256sum "${shard}" | awk '{print $1}')" \
          '$current + {($filename): $digest}')"
      done
      ;;
    *)
      die "ruolo non supportato durante lo staging: ${role}"
      ;;
  esac
}

build_command_for_role() {
  case "$1" in
    cloud-core)
      printf '%s\n' \
        'docker pull apache/kafka:4.3.0' \
        "docker tag apache/kafka:4.3.0 continuum-kafka:${DEPLOYMENT_ID}" \
        "docker build --pull -f deploy/docker/global-aggregator.Dockerfile -t continuum-global-aggregator:${DEPLOYMENT_ID} ." \
        'docker compose --env-file .env -f deploy/compose/distributed/cloud-core.generated.yml config --quiet'
      ;;
    workers)
      printf '%s\n' \
        "docker build --pull -f deploy/docker/cloud-worker.Dockerfile -t continuum-cloud-worker:${DEPLOYMENT_ID} ." \
        'docker compose --env-file .env -f deploy/compose/distributed/workers.generated.yml config --quiet'
      ;;
    edge)
      printf '%s\n' \
        'docker pull eclipse-mosquitto:2' \
        "docker tag eclipse-mosquitto:2 continuum-mosquitto:${DEPLOYMENT_ID}" \
        "docker build --pull -f deploy/docker/edge.Dockerfile -t continuum-edge:${DEPLOYMENT_ID} ." \
        'docker compose --env-file .env -f deploy/compose/distributed/edge.generated.yml config --quiet'
      ;;
    simulator)
      printf '%s\n' \
        "docker build --pull -f deploy/docker/simulator.Dockerfile -t continuum-simulator:${DEPLOYMENT_ID} ." \
        'REPLAY_START_AT=1970-01-01T00:00:00Z docker compose --env-file .env -f deploy/compose/distributed/simulator.generated.yml config --quiet'
      ;;
    *)
      die "ruolo non supportato durante la build: $1"
      ;;
  esac
}

upload_and_build_role() {
  local role="$1"
  local host="${PUBLIC_IPS["${role}"]}"
  local source="${STAGING_ROOT}/${role}"
  local archive="${STAGING_ROOT}/${role}-${DEPLOYMENT_ID}.tar.gz"
  local remote_archive="/tmp/continuum-${role}-${DEPLOYMENT_ID}.tar.gz"
  local release="${REMOTE_ROOT}/releases/${DEPLOYMENT_ID}"
  local build_command

  tar -C "${source}" -czf "${archive}" .

  log "trasferimento file necessari a ${role}"
  scp_to_host "${archive}" "${host}" "${remote_archive}"

  ssh_run "${host}" "set -euo pipefail
sudo install -d -m 0755 '${REMOTE_ROOT}/releases'
sudo chown \"\$(id -u):\$(id -g)\" '${REMOTE_ROOT}' '${REMOTE_ROOT}/releases'
if [[ -e '${release}' ]]; then
  echo 'release remota gia esistente: ${release}' >&2
  exit 1
fi
mkdir -p '${release}'
tar -xzf '${remote_archive}' -C '${release}'
rm -f '${remote_archive}'"

  build_command="$(build_command_for_role "${role}")"
  log "preparazione immagini e validazione Compose su ${role}"
  ssh_run "${host}" "set -euo pipefail
cd '${release}'
${build_command}"
}

image_metadata() {
  local role="$1"
  local image_ref="$2"
  local inspection
  local image_id
  local repo_digests
  local role_images

  inspection="$(ssh_run "${PUBLIC_IPS["${role}"]}" bash -s -- "${image_ref}" <<'REMOTE'
set -euo pipefail
docker image inspect --format '{{.Id}}|{{json .RepoDigests}}' "$1"
REMOTE
  )" ||
    die "impossibile ispezionare ${image_ref} su ${role}"
  image_id="${inspection%%|*}"
  repo_digests="${inspection#*|}"
  [[ "${image_id}" == sha256:* ]] || die "image ID non valido per ${image_ref} su ${role}"
  [[ "${repo_digests}" != "null" ]] || repo_digests='[]'
  jq -e 'type == "array"' <<<"${repo_digests}" >/dev/null ||
    die "RepoDigests non validi per ${image_ref} su ${role}"

  role_images="$(jq -c --arg role "${role}" '.[$role] // {}' <<<"${IMAGES_BY_ROLE}")"
  role_images="$(jq -cn \
    --argjson current "${role_images}" \
    --arg ref "${image_ref}" \
    --arg id "${image_id}" \
    --argjson repo_digests "${repo_digests}" \
    '$current + {($ref): {id: $id, repo_digests: $repo_digests}}')"
  IMAGES_BY_ROLE="$(jq -cn \
    --argjson current "${IMAGES_BY_ROLE}" \
    --arg role "${role}" \
    --argjson images "${role_images}" \
    '$current + {($role): $images}')"
}

collect_image_metadata() {
  image_metadata simulator "continuum-simulator:${DEPLOYMENT_ID}"
  image_metadata edge "continuum-edge:${DEPLOYMENT_ID}"
  image_metadata edge "continuum-mosquitto:${DEPLOYMENT_ID}"
  image_metadata cloud-core "continuum-global-aggregator:${DEPLOYMENT_ID}"
  image_metadata cloud-core "continuum-kafka:${DEPLOYMENT_ID}"
  image_metadata workers "continuum-cloud-worker:${DEPLOYMENT_ID}"
}

create_release_manifest() {
  RELEASE_MANIFEST_PATH="${STAGING_ROOT}/release-manifest.json"
  jq -n \
    --arg deployment_id "${DEPLOYMENT_ID}" \
    --arg git_commit_sha "${GIT_COMMIT_SHA}" \
    --arg generation_manifest_sha256 "${GENERATION_MANIFEST_SHA256}" \
    --argjson generation_manifest "$(<"${GENERATION_MANIFEST_PATH}")" \
    --argjson replay_shard_sha256 "${REPLAY_SHARD_SHA256}" \
    --argjson images_by_role "${IMAGES_BY_ROLE}" \
    '{
      schema_version: 1,
      deployment_id: $deployment_id,
      git_commit_sha: $git_commit_sha,
      generation_manifest_sha256: $generation_manifest_sha256,
      config_sha256: $generation_manifest.config_sha256,
      topology_sha256: $generation_manifest.topology_sha256,
      resolved_config_yaml: $generation_manifest.resolved_config_yaml,
      compose_sha256: $generation_manifest.compose_sha256,
      replay_shard_sha256: $replay_shard_sha256,
      images_by_role: $images_by_role
    }' >"${RELEASE_MANIFEST_PATH}"
}

install_release_manifest() {
  local role="$1"
  local host="${PUBLIC_IPS["${role}"]}"
  local remote_manifest="/tmp/continuum-release-manifest-${DEPLOYMENT_ID}.json"

  scp_to_host "${RELEASE_MANIFEST_PATH}" "${host}" "${remote_manifest}"
  ssh_run "${host}" "set -euo pipefail
install -m 0644 '${remote_manifest}' '${REMOTE_ROOT}/releases/${DEPLOYMENT_ID}/release-manifest.json'
rm -f '${remote_manifest}'"
}

verify_releases_before_promotion() {
  local role
  local expected_manifest_sha256
  local previous_target

  expected_manifest_sha256="$(sha256sum "${RELEASE_MANIFEST_PATH}" | awk '{print $1}')"
  for role in "${ROLES[@]}"; do
    ssh_run "${PUBLIC_IPS["${role}"]}" "set -euo pipefail
release='${REMOTE_ROOT}/releases/${DEPLOYMENT_ID}'
test -d \"\${release}\"
test \"\$(sha256sum \"\${release}/release-manifest.json\" | awk '{print \$1}')\" = '${expected_manifest_sha256}'
grep -Fx 'DEPLOYMENT_ID=${DEPLOYMENT_ID}' \"\${release}/.env\" >/dev/null
if [[ -e '${REMOTE_ROOT}/current' && ! -L '${REMOTE_ROOT}/current' ]]; then
  echo '${REMOTE_ROOT}/current esiste ma non e un link simbolico' >&2
  exit 1
fi" || die "release ${DEPLOYMENT_ID} non verificabile su ${role}"

    previous_target="$(ssh_run "${PUBLIC_IPS["${role}"]}" 'set -euo pipefail
if [[ -L /opt/continuum/current ]]; then
  readlink -f /opt/continuum/current
fi')"
    if [[ -n "${previous_target}" && "${previous_target}" != "${REMOTE_ROOT}/releases/"* ]]; then
      die "target current inatteso su ${role}: ${previous_target}"
    fi
    PREVIOUS_CURRENT_TARGETS["${role}"]="${previous_target}"
  done
}

promote_releases() {
  local role
  local promoted_role
  local previous_target
  local -a promoted_roles=()

  log "promozione coordinata della release ${DEPLOYMENT_ID}"
  for role in "${ROLES[@]}"; do
    if ! ssh_run "${PUBLIC_IPS["${role}"]}" "set -euo pipefail
next='${REMOTE_ROOT}/current.${DEPLOYMENT_ID}.next'
rm -f \"\${next}\"
ln -s '${REMOTE_ROOT}/releases/${DEPLOYMENT_ID}' \"\${next}\"
mv -Tf \"\${next}\" '${REMOTE_ROOT}/current'"; then
      log "promozione fallita su ${role}; tentativo di rollback degli host gia promossi"
      for promoted_role in "${promoted_roles[@]}"; do
        previous_target="${PREVIOUS_CURRENT_TARGETS["${promoted_role}"]}"
        if [[ -n "${previous_target}" ]]; then
          ssh_run "${PUBLIC_IPS["${promoted_role}"]}" "set -euo pipefail
rollback='${REMOTE_ROOT}/current.${DEPLOYMENT_ID}.rollback'
rm -f \"\${rollback}\"
ln -s '${previous_target}' \"\${rollback}\"
mv -Tf \"\${rollback}\" '${REMOTE_ROOT}/current'" || true
        else
          ssh_run "${PUBLIC_IPS["${promoted_role}"]}" \
            "rm -f '${REMOTE_ROOT}/current'" || true
        fi
      done
      die "promozione coordinata della release fallita su ${role}"
    fi
    promoted_roles+=("${role}")
  done
}

print_summary() {
  local role

  printf '\nPilot predisposto senza avviare container.\n'
  printf 'Release: %s\n' "${DEPLOYMENT_ID}"
  for role in "${ROLES[@]}"; do
    printf '  %-10s public=%s private=%s current=%s/current\n' \
      "${role}" \
      "${PUBLIC_IPS["${role}"]}" \
      "${PRIVATE_IPS["${role}"]}" \
      "${REMOTE_ROOT}"
  done
}

main() {
  local role

  require_command "${TERRAFORM_BIN}"
  require_command jq
  require_command ssh
  require_command scp
  require_command tar
  require_command git
  require_command sha256sum
  validate_inputs
  validate_release_source
  load_terraform_addresses

  wait_for_all_ssh
  for role in "${ROLES[@]}"; do
    verify_host_runtime "${role}"
  done

  STAGING_ROOT="$(mktemp -d)"
  for role in "${ROLES[@]}"; do
    stage_role "${role}"
  done
  for role in "${ROLES[@]}"; do
    upload_and_build_role "${role}"
  done
  collect_image_metadata
  create_release_manifest
  for role in "${ROLES[@]}"; do
    install_release_manifest "${role}"
  done
  verify_releases_before_promotion
  promote_releases

  print_summary
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi

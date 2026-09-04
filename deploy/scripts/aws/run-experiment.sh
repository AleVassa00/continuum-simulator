#!/usr/bin/env bash
set -Eeuo pipefail

export AWS_SCRIPT_LOG_PREFIX="run-experiment"
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/prepare-pilot.sh"
trap - EXIT

readonly EXPERIMENT_CONFIG_INPUT="${EXPERIMENT_CONFIG:-${REPO_ROOT}/experiments/baseline.yaml}"
readonly ARTIFACTS_ROOT="${ARTIFACTS_ROOT:-${REPO_ROOT}/artifacts/aws-runs}"
readonly KAFKA_READY_TIMEOUT_SECONDS="${KAFKA_READY_TIMEOUT_SECONDS:-300}"
readonly EDGE_READY_TIMEOUT_SECONDS="${EDGE_READY_TIMEOUT_SECONDS:-300}"
readonly RUN_COMPLETION_TIMEOUT_SECONDS="${RUN_COMPLETION_TIMEOUT_SECONDS:-3600}"
readonly POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"
readonly METRICS_INTERVAL_SECONDS="${METRICS_INTERVAL_SECONDS:-5}"

declare -A METRICS_PIDS

EXPERIMENT_CONFIG_PATH=""
EXPERIMENT_NAME=""
CONFIG_SHA256=""
CONFIGURED_WORKERS="0"
WORKER_COUNT="0"
DEPLOYMENT_ID_VALUE=""
DEPLOYED_GIT_COMMIT_SHA=""
RUN_ID_VALUE=""
ARTIFACT_DIR=""
INSTANCE_IDENTITIES='{}'
ORCHESTRATION_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
CLOCK_VERIFIED_AT=""
REPLAY_LAUNCHED_AT=""
REPLAY_START_AT=""
RUN_FINISHED_AT=""
RUN_STATUS="failed"
ADDRESSES_LOADED="false"
METRICS_STARTED="false"

validate_positive_integer() {
  local name="$1"
  local value="$2"

  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || die "${name} deve essere un intero positivo"
}

runconfig() {
  (
    cd "${REPO_ROOT}"
    go run ./cmd/runconfig "$@"
  )
}

load_experiment_description() {
  local description

  EXPERIMENT_CONFIG_PATH="$(resolve_file "${EXPERIMENT_CONFIG_INPUT}")" ||
    die "configurazione esperimento non trovata: ${EXPERIMENT_CONFIG_INPUT}"
  description="$(runconfig --experiment "${EXPERIMENT_CONFIG_PATH}" --describe)" ||
    die "impossibile leggere la configurazione esperimento"
  EXPERIMENT_NAME="$(jq -er '.experiment_name' <<<"${description}")"
  CONFIGURED_WORKERS="$(jq -er '.workers' <<<"${description}")"
  CONFIG_SHA256="$(jq -er '.config_sha256 | select(type == "string" and length == 64)' <<<"${description}")"
  [[ "${CONFIG_SHA256}" =~ ^[0-9a-f]{64}$ ]] || die "config_sha256 non valido"
}

initialize_artifacts() {
  local requested_run_id="${RUN_ID:-}"
  local artifacts_root_path
  local public_json
  local private_json
  local private_dns_json

  if [[ -z "${requested_run_id}" ]]; then
    requested_run_id="aws-$(date -u +%Y%m%dT%H%M%SZ)-$$-${RANDOM}"
  fi
  [[ "${requested_run_id}" =~ ^[A-Za-z0-9._-]+$ ]] ||
    die "RUN_ID puo contenere solo lettere, numeri, punto, underscore e trattino"
  RUN_ID_VALUE="${requested_run_id}"

  mkdir -p "${ARTIFACTS_ROOT}"
  artifacts_root_path="$(cd "${ARTIFACTS_ROOT}" && pwd -P)"
  ARTIFACT_DIR="${artifacts_root_path}/${RUN_ID_VALUE}"
  if ! mkdir "${ARTIFACT_DIR}"; then
    die "directory artefatti gia esistente o non creabile: ${ARTIFACT_DIR}"
  fi
  mkdir "${ARTIFACT_DIR}/logs" "${ARTIFACT_DIR}/metrics" "${ARTIFACT_DIR}/compose"
  exec > >(tee -a "${ARTIFACT_DIR}/orchestrator.log") 2>&1
  trap finalize_run EXIT

  cp "${EXPERIMENT_CONFIG_PATH}" "${ARTIFACT_DIR}/experiment.yaml"

  public_json="$(terraform_output public_ips)"
  private_json="$(terraform_output private_ips)"
  private_dns_json="$(terraform_output private_dns)"
  jq -n \
    --argjson public_ips "${public_json}" \
    --argjson private_ips "${private_json}" \
    --argjson private_dns "${private_dns_json}" \
    '{public_ips: $public_ips, private_ips: $private_ips, private_dns: $private_dns}' \
    >"${ARTIFACT_DIR}/infrastructure.json"
}

collect_instance_identities() {
  local role
  local identity
  local instance_id
  local instance_type
  local availability_zone
  local region

  for role in "${ROLES[@]}"; do
    identity="$(ssh_run "${PUBLIC_IPS["${role}"]}" 'set -euo pipefail
token="$(curl -fsS --max-time 5 -X PUT \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 60" \
  http://169.254.169.254/latest/api/token)"
metadata() {
  curl -fsS --max-time 5 \
    -H "X-aws-ec2-metadata-token: ${token}" \
    "http://169.254.169.254/latest/meta-data/$1"
}
printf "%s|%s|%s|%s\n" \
  "$(metadata instance-id)" \
  "$(metadata instance-type)" \
  "$(metadata placement/availability-zone)" \
  "$(metadata placement/region)"')" ||
      die "impossibile leggere i metadata EC2 IMDSv2 per ${role}"
    IFS='|' read -r instance_id instance_type availability_zone region <<<"${identity}"
    [[ "${instance_id}" == i-* && -n "${instance_type}" && -n "${availability_zone}" && -n "${region}" ]] ||
      die "metadata EC2 non validi per ${role}: ${identity}"
    INSTANCE_IDENTITIES="$(jq -cn \
      --argjson current "${INSTANCE_IDENTITIES}" \
      --arg role "${role}" \
      --arg instance_id "${instance_id}" \
      --arg instance_type "${instance_type}" \
      --arg availability_zone "${availability_zone}" \
      --arg region "${region}" \
      '$current + {($role): {
        instance_id: $instance_id,
        instance_type: $instance_type,
        availability_zone: $availability_zone,
        region: $region
      }}')"
  done
  jq . <<<"${INSTANCE_IDENTITIES}" >"${ARTIFACT_DIR}/instance-identities.json"
}

collect_host_logs() {
  local role
  local host

  [[ "${ADDRESSES_LOADED}" == "true" && -n "${ARTIFACT_DIR}" ]] || return 0
  log "raccolta log dei quattro host"
  for role in "${ROLES[@]}"; do
    host="${PUBLIC_IPS["${role}"]}"
    if ! ssh_run "${host}" 'set -u
docker ps -a --no-trunc
while IFS= read -r container; do
  [[ -n "${container}" ]] || continue
  printf "\n===== %s =====\n" "${container}"
  docker logs --timestamps "${container}" 2>&1 || true
done < <(docker ps -a --format "{{.Names}}" | sort)' \
      >"${ARTIFACT_DIR}/logs/${role}.log" 2>&1; then
      printf 'raccolta log fallita per %s\n' "${role}" >"${ARTIFACT_DIR}/logs/${role}.log"
    fi
  done
}

capture_container_states() {
  local label="$1"
  local role

  [[ "${ADDRESSES_LOADED}" == "true" && -n "${ARTIFACT_DIR}" ]] || return 0
  for role in "${ROLES[@]}"; do
    ssh_run "${PUBLIC_IPS["${role}"]}" 'set -u
printf "name\timage_id\tstate\trestart_count\toom_killed\texit_code\thealth\tstarted_at\tfinished_at\n"
while IFS= read -r container; do
  [[ -n "${container}" ]] || continue
  docker inspect --format "{{.Name}}\t{{.Image}}\t{{.State.Status}}\t{{.RestartCount}}\t{{.State.OOMKilled}}\t{{.State.ExitCode}}\t{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}\t{{.State.StartedAt}}\t{{.State.FinishedAt}}" "${container}" |
    sed "s#^/##"
done < <(docker ps -a --format "{{.Names}}" | sort)' \
      >"${ARTIFACT_DIR}/container-state-${label}-${role}.tsv" 2>&1 || true
  done
}

write_run_metadata() {
  local exit_code="$1"
  local fallback_finished_at

  fallback_finished_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
  if [[ -z "${RUN_FINISHED_AT}" && "${ADDRESSES_LOADED}" == "true" ]]; then
    RUN_FINISHED_AT="$(ssh_run "${PUBLIC_IPS[simulator]}" 'date -u +%Y-%m-%dT%H:%M:%S.%NZ' 2>/dev/null || true)"
  fi
  [[ -n "${RUN_FINISHED_AT}" ]] || RUN_FINISHED_AT="${fallback_finished_at}"

  jq -n \
    --arg run_id "${RUN_ID_VALUE}" \
    --arg experiment "${EXPERIMENT_NAME}" \
    --arg status "${RUN_STATUS}" \
    --arg deployment_id "${DEPLOYMENT_ID_VALUE}" \
    --arg git_commit_sha "${DEPLOYED_GIT_COMMIT_SHA}" \
    --arg config_sha256 "${CONFIG_SHA256}" \
    --arg orchestration_started_at "${ORCHESTRATION_STARTED_AT}" \
    --arg clock_verified_at "${CLOCK_VERIFIED_AT}" \
    --arg replay_start_at "${REPLAY_START_AT}" \
    --arg replay_launched_at "${REPLAY_LAUNCHED_AT}" \
    --arg finished_at "${RUN_FINISHED_AT}" \
    --argjson workers "${WORKER_COUNT:-0}" \
    --argjson metrics_interval_seconds "${METRICS_INTERVAL_SECONDS}" \
    --argjson instance_identities "${INSTANCE_IDENTITIES}" \
    --argjson exit_code "${exit_code}" \
    '{
      run_id: $run_id,
      experiment: $experiment,
      status: $status,
      deployment_id: $deployment_id,
      git_commit_sha: $git_commit_sha,
      config_sha256: $config_sha256,
      workers: $workers,
      orchestration_started_at: $orchestration_started_at,
      clock_verified_at: $clock_verified_at,
      replay_start_at: $replay_start_at,
      replay_launched_at: $replay_launched_at,
      finished_at: $finished_at,
      orchestrator_exit_code: $exit_code,
      metrics: {
        interval_seconds: $metrics_interval_seconds,
        files: "metrics/{simulator,edge,cloud-core,workers}.log"
      },
      ec2: $instance_identities,
      cpu_credits: {
        source: "AWS/EC2 CloudWatch",
        dimension: "InstanceId",
        metric_names: [
          "CPUCreditBalance",
          "CPUCreditUsage",
          "CPUSurplusCreditBalance",
          "CPUSurplusCreditsCharged"
        ],
        recommended_period_seconds: 60,
        retrieval_method: "Query every recorded InstanceId over orchestration_started_at..finished_at; this script deliberately does not add AWS CLI/CloudWatch dependencies.",
        cloudwatch_command_template: "aws cloudwatch get-metric-statistics --namespace AWS/EC2 --metric-name <metric-name> --dimensions Name=InstanceId,Value=<instance-id> --statistics Average Minimum Maximum --period 60 --start-time <start> --end-time <end> --region <region>",
        credit_mode_command_template: "aws ec2 describe-instance-credit-specifications --instance-ids <instance-id> --region <region>"
      }
    }' >"${ARTIFACT_DIR}/run-metadata.json"
}

stop_metric_collectors() {
  local role
  local pid

  [[ "${METRICS_STARTED}" == "true" ]] || return 0
  for role in "${ROLES[@]}"; do
    ssh_run "${PUBLIC_IPS["${role}"]}" bash -s -- "${RUN_ID_VALUE}" <<'REMOTE' >/dev/null 2>&1 || true
set -u
pidfile="/tmp/continuum-metrics-$1.pid"
if [[ -f "${pidfile}" ]]; then
  kill "$(cat "${pidfile}")" 2>/dev/null || true
  rm -f "${pidfile}"
fi
REMOTE
  done
  for role in "${!METRICS_PIDS[@]}"; do
    pid="${METRICS_PIDS["${role}"]}"
    kill "${pid}" 2>/dev/null || true
  done
  for role in "${!METRICS_PIDS[@]}"; do
    pid="${METRICS_PIDS["${role}"]}"
    wait "${pid}" 2>/dev/null || true
  done
  METRICS_STARTED="false"
}

finalize_run() {
  local exit_code="$?"

  trap - EXIT
  set +e
  stop_metric_collectors
  if [[ -n "${ARTIFACT_DIR}" ]]; then
    capture_container_states final
    collect_host_logs
    write_run_metadata "${exit_code}"
    log "artefatti run: ${ARTIFACT_DIR}"
  fi
  exit "${exit_code}"
}

verify_prepared_releases() {
  local role
  local deployment_id
  local manifest_sha256
  local expected_manifest_sha256=""
  local filename
  local expected_compose_sha256
  local actual_compose_sha256
  local image_ref
  local expected_image_id
  local actual_image_id
  local local_git_commit_sha
  local edge_number
  local expected_shard_sha256
  local actual_shard_sha256
  local source_status

  for role in "${ROLES[@]}"; do
    deployment_id="$(ssh_run "${PUBLIC_IPS["${role}"]}" \
      'set -euo pipefail
test -L /opt/continuum/current
basename "$(readlink -f /opt/continuum/current)"')" ||
      die "host ${role} non predisposto: eseguire prima prepare-pilot.sh"
    [[ "${deployment_id}" =~ ^[A-Za-z0-9._-]+$ ]] ||
      die "DEPLOYMENT_ID non valido su ${role}: ${deployment_id}"
    if [[ -z "${DEPLOYMENT_ID_VALUE}" ]]; then
      DEPLOYMENT_ID_VALUE="${deployment_id}"
    elif [[ "${deployment_id}" != "${DEPLOYMENT_ID_VALUE}" ]]; then
      die "release disallineate: ${role} usa ${deployment_id}, attesa ${DEPLOYMENT_ID_VALUE}"
    fi

    manifest_sha256="$(ssh_run "${PUBLIC_IPS["${role}"]}" \
      'set -euo pipefail
test -f /opt/continuum/current/release-manifest.json
sha256sum /opt/continuum/current/release-manifest.json | awk "{print \$1}"')"
    if [[ -z "${expected_manifest_sha256}" ]]; then
      expected_manifest_sha256="${manifest_sha256}"
      ssh_run "${PUBLIC_IPS["${role}"]}" \
        'cat /opt/continuum/current/release-manifest.json' >"${ARTIFACT_DIR}/release-manifest.json"
    elif [[ "${manifest_sha256}" != "${expected_manifest_sha256}" ]]; then
      die "release-manifest diverso su ${role}"
    fi

    ssh_run "${PUBLIC_IPS["${role}"]}" \
      "grep -Fx 'DEPLOYMENT_ID=${DEPLOYMENT_ID_VALUE}' /opt/continuum/current/.env >/dev/null" ||
      die "DEPLOYMENT_ID nell'environment non coerente su ${role}"
  done

  [[ "$(sha256sum "${ARTIFACT_DIR}/release-manifest.json" | awk '{print $1}')" == "${expected_manifest_sha256}" ]] ||
    die "release-manifest trasferito non integro"
  [[ "$(jq -er '.deployment_id' "${ARTIFACT_DIR}/release-manifest.json")" == "${DEPLOYMENT_ID_VALUE}" ]] ||
    die "DEPLOYMENT_ID interno al manifest non coerente"
  [[ "$(jq -er '.schema_version' "${ARTIFACT_DIR}/release-manifest.json")" == "1" ]] ||
    die "schema version del release manifest non supportata"
  [[ "$(jq -er '.config_sha256' "${ARTIFACT_DIR}/release-manifest.json")" == "${CONFIG_SHA256}" ]] ||
    die "EXPERIMENT_CONFIG non corrisponde alla configurazione usata da deploygen"

  DEPLOYED_GIT_COMMIT_SHA="$(jq -er '.git_commit_sha' "${ARTIFACT_DIR}/release-manifest.json")"
  [[ "${DEPLOYED_GIT_COMMIT_SHA}" =~ ^[0-9a-f]{40,64}$ ]] ||
    die "Git commit SHA non valida nel release manifest"
  local_git_commit_sha="$(git -C "${REPO_ROOT}" rev-parse --verify HEAD)"
  [[ "${local_git_commit_sha}" == "${DEPLOYED_GIT_COMMIT_SHA}" ]] ||
    die "il checkout dell'orchestratore (${local_git_commit_sha}) non corrisponde alla release (${DEPLOYED_GIT_COMMIT_SHA})"
  source_status="$(git -C "${REPO_ROOT}" status --porcelain --untracked-files=all)"
  [[ -z "${source_status}" ]] ||
    die "l'orchestratore richiede un worktree Git pulito per rendere significativa la commit SHA"

  for role in "${ROLES[@]}"; do
    case "${role}" in
      cloud-core) filename="cloud-core.generated.yml" ;;
      workers) filename="workers.generated.yml" ;;
      edge) filename="edge.generated.yml" ;;
      simulator) filename="simulator.generated.yml" ;;
    esac
    expected_compose_sha256="$(jq -er --arg filename "${filename}" '.compose_sha256[$filename]' \
      "${ARTIFACT_DIR}/release-manifest.json")"
    actual_compose_sha256="$(ssh_run "${PUBLIC_IPS["${role}"]}" \
      "sha256sum '/opt/continuum/current/deploy/compose/distributed/${filename}' | awk '{print \$1}'")"
    [[ "${actual_compose_sha256}" == "${expected_compose_sha256}" ]] ||
      die "Compose ${filename} su ${role} non corrisponde alla release"

    jq -e --arg role "${role}" \
      '.images_by_role[$role] | type == "object" and length > 0' \
      "${ARTIFACT_DIR}/release-manifest.json" >/dev/null ||
      die "image metadata mancanti per ${role} nel release manifest"

    while IFS=$'\t' read -r image_ref expected_image_id; do
      [[ -n "${image_ref}" ]] || continue
      actual_image_id="$(ssh_run "${PUBLIC_IPS["${role}"]}" \
        docker image inspect --format '{{.Id}}' "${image_ref}")" ||
        die "immagine ${image_ref} assente su ${role}"
      [[ "${actual_image_id}" == "${expected_image_id}" ]] ||
        die "image ID di ${image_ref} su ${role} diverso dal release manifest"
    done < <(jq -r --arg role "${role}" \
      '.images_by_role[$role] | to_entries[] | [.key, .value.id] | @tsv' \
      "${ARTIFACT_DIR}/release-manifest.json")
  done

  for ((edge_number = 0; edge_number < 13; edge_number++)); do
    filename="edge-${edge_number}.csv"
    expected_shard_sha256="$(jq -er --arg filename "${filename}" '.replay_shard_sha256[$filename]' \
      "${ARTIFACT_DIR}/release-manifest.json")"
    actual_shard_sha256="$(ssh_run "${PUBLIC_IPS[simulator]}" \
      "sha256sum '/opt/continuum/current/dataset/derived/replay_by_edge/${filename}' | awk '{print \$1}'")"
    [[ "${actual_shard_sha256}" == "${expected_shard_sha256}" ]] ||
      die "replay shard ${filename} non corrisponde al release manifest"
  done

  [[ "$(jq -er '.replay_shard_sha256 | length' "${ARTIFACT_DIR}/release-manifest.json")" == "13" ]] ||
    die "il release manifest non contiene esattamente 13 replay shard"
  jq -r '.replay_shard_sha256 | to_entries | sort_by(.key)[] | "\(.value)  \(.key)"' \
    "${ARTIFACT_DIR}/release-manifest.json" >"${ARTIFACT_DIR}/replay-shards.sha256"
  jq '.images_by_role' "${ARTIFACT_DIR}/release-manifest.json" >"${ARTIFACT_DIR}/image-metadata.json"

  printf '%s\n' "${DEPLOYMENT_ID_VALUE}" >"${ARTIFACT_DIR}/deployment-id.txt"
  printf '%s\n' "${DEPLOYED_GIT_COMMIT_SHA}" >"${ARTIFACT_DIR}/git-commit-sha.txt"
}

reset_previous_run() {
  log "1/9 reset stato della run precedente"

  ssh_run "${PUBLIC_IPS[simulator]}" 'set -euo pipefail
cd /opt/continuum/current
REPLAY_START_AT=1970-01-01T00:00:00Z docker compose \
  --env-file .env \
  --profile replay \
  -f deploy/compose/distributed/simulator.generated.yml \
  down --remove-orphans --timeout 30
rm -f /opt/continuum/current/.run.env /opt/continuum/current/.run.env.tmp'

  ssh_run "${PUBLIC_IPS[edge]}" 'set -euo pipefail
cd /opt/continuum/current
docker compose --env-file .env -f deploy/compose/distributed/edge.generated.yml down --remove-orphans --timeout 30'

  ssh_run "${PUBLIC_IPS[workers]}" 'set -euo pipefail
cd /opt/continuum/current
docker compose --env-file .env -f deploy/compose/distributed/workers.generated.yml down --remove-orphans --timeout 30'

  ssh_run "${PUBLIC_IPS[cloud-core]}" 'set -euo pipefail
cd /opt/continuum/current
docker compose --env-file .env -f deploy/compose/distributed/cloud-core.generated.yml down --remove-orphans --volumes --timeout 30'
}

start_metric_collectors() {
  local role
  local output

  log "avvio raccolta metriche di sizing ogni ${METRICS_INTERVAL_SECONDS}s"
  for role in "${ROLES[@]}"; do
    output="${ARTIFACT_DIR}/metrics/${role}.log"
    ssh_run "${PUBLIC_IPS["${role}"]}" bash -s -- \
      "${METRICS_INTERVAL_SECONDS}" "${RUN_ID_VALUE}" >"${output}" 2>&1 <<'REMOTE' &
set -euo pipefail
interval_seconds="$1"
run_id="$2"
pidfile="/tmp/continuum-metrics-${run_id}.pid"
printf '%s\n' "$$" >"${pidfile}"
trap 'rm -f "${pidfile}"' EXIT
trap 'exit 0' HUP INT TERM

while true; do
  printf '===== sample %s =====\n' "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
  printf '%s\n' '-- docker-stats'
  timeout 20s docker stats --no-stream --format '{{json .}}' 2>&1 || true
  printf '%s\n' '-- host-load'
  cat /proc/loadavg
  printf '%s\n' '-- host-cpu-counters'
  grep '^cpu ' /proc/stat
  printf '%s\n' '-- host-memory-kib'
  grep -E '^(MemTotal|MemFree|MemAvailable|Buffers|Cached|SwapTotal|SwapFree):' /proc/meminfo
  printf '%s\n' '-- filesystem-bytes'
  df -B1 -P / /var/lib/docker 2>&1 | awk '!seen[$0]++' || true
  printf '%s\n' '-- block-devices'
  cat /proc/diskstats
  printf '%s\n' '-- network-devices'
  cat /proc/net/dev
  sleep "${interval_seconds}" &
  wait $!
done
REMOTE
    METRICS_PIDS["${role}"]=$!
  done
  METRICS_STARTED="true"

  for role in "${ROLES[@]}"; do
    for _ in $(seq 1 10); do
      if [[ -s "${ARTIFACT_DIR}/metrics/${role}.log" ]]; then
        break
      fi
      kill -0 "${METRICS_PIDS["${role}"]}" 2>/dev/null ||
        die "collector metriche non avviabile per ${role}"
      sleep 1
    done
    [[ -s "${ARTIFACT_DIR}/metrics/${role}.log" ]] ||
      die "collector metriche senza campioni iniziali per ${role}"
  done
}

verify_metric_collectors() {
  local role
  local pid

  for role in "${ROLES[@]}"; do
    pid="${METRICS_PIDS["${role}"]:-}"
    [[ -n "${pid}" ]] || die "collector metriche non registrato per ${role}"
    kill -0 "${pid}" 2>/dev/null || die "collector metriche terminato prematuramente per ${role}"
    [[ -s "${ARTIFACT_DIR}/metrics/${role}.log" ]] ||
      die "nessun campione metrico raccolto per ${role}"
  done
}

collect_normalized_compose() {
  local role="$1"
  local env_file="${2:-.env}"
  local compose_file
  local profile=""

  case "${role}" in
    cloud-core) compose_file="cloud-core.generated.yml" ;;
    workers) compose_file="workers.generated.yml" ;;
    edge) compose_file="edge.generated.yml" ;;
    simulator)
      compose_file="simulator.generated.yml"
      profile="replay"
      ;;
    *) die "ruolo Compose non supportato: ${role}" ;;
  esac

  ssh_run "${PUBLIC_IPS["${role}"]}" bash -s -- \
    "${env_file}" "${compose_file}" "${profile}" <<'REMOTE' \
    >"${ARTIFACT_DIR}/compose/${role}.normalized.yml"
set -euo pipefail
env_file="$1"
compose_file="$2"
profile="$3"
cd /opt/continuum/current
args=(docker compose --env-file "${env_file}")
if [[ -n "${profile}" ]]; then
  args+=(--profile "${profile}")
fi
args+=(-f "deploy/compose/distributed/${compose_file}" config)
"${args[@]}"
REMOTE
}

write_compose_checksums() {
  (
    cd "${ARTIFACT_DIR}/compose"
    sha256sum \
      cloud-core.normalized.yml \
      workers.normalized.yml \
      edge.normalized.yml \
      simulator.normalized.yml
  ) >"${ARTIFACT_DIR}/compose-checksums.sha256"
}

start_cloud_core() {
  log "2/9 avvio Cloud Core"
  ssh_run "${PUBLIC_IPS[cloud-core]}" 'set -euo pipefail
cd /opt/continuum/current
docker compose --env-file .env -f deploy/compose/distributed/cloud-core.generated.yml up -d kafka kafka-init'
}

wait_for_kafka() {
  log "3/9 attesa Kafka healthy e topic"
  ssh_run "${PUBLIC_IPS[cloud-core]}" bash -s -- \
    "${KAFKA_READY_TIMEOUT_SECONDS}" "${POLL_INTERVAL_SECONDS}" <<'REMOTE'
set -euo pipefail
timeout_seconds="$1"
poll_seconds="$2"
deadline=$(( $(date +%s) + timeout_seconds ))

while (( $(date +%s) < deadline )); do
  kafka_health="$(docker inspect --format '{{.State.Health.Status}}' kafka 2>/dev/null || true)"
  init_state="$(docker inspect --format '{{.State.Status}}' kafka-init 2>/dev/null || true)"
  if [[ "${init_state}" == "exited" ]]; then
    init_exit="$(docker inspect --format '{{.State.ExitCode}}' kafka-init)"
    if [[ "${init_exit}" != "0" ]]; then
      docker logs kafka-init >&2 || true
      echo "kafka-init terminato con exit code ${init_exit}" >&2
      exit 1
    fi
  fi
  if [[ "${kafka_health}" == "healthy" && "${init_state}" == "exited" ]]; then
    edge_topic="$(docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:29092 --describe --topic edge-aggregates)"
    cloud_topic="$(docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:29092 --describe --topic cloud-edge-aggregates)"
    grep -F 'PartitionCount: 6' <<<"${edge_topic}" >/dev/null || exit 1
    grep -F 'PartitionCount: 1' <<<"${cloud_topic}" >/dev/null || exit 1
    exit 0
  fi
  sleep "${poll_seconds}"
done

docker ps -a >&2
docker logs kafka >&2 || true
docker logs kafka-init >&2 || true
echo "Kafka o i topic non sono diventati ready entro ${timeout_seconds}s" >&2
exit 1
REMOTE

  ssh_run "${PUBLIC_IPS[cloud-core]}" 'set -euo pipefail
cd /opt/continuum/current
docker compose --env-file .env -f deploy/compose/distributed/cloud-core.generated.yml up -d global-aggregator
[[ "$(docker inspect --format "{{.State.Running}}" global-aggregator)" == "true" ]]'
  collect_normalized_compose cloud-core
}

verify_kafka_tcp_from_role() {
  local role="$1"

  ssh_run "${PUBLIC_IPS["${role}"]}" bash -s -- "${PRIVATE_IPS[cloud-core]}" <<'REMOTE'
set -euo pipefail
host="$1"
timeout 5 bash -c 'exec 3<>/dev/tcp/$1/9092' _ "${host}"
REMOTE
}

start_workers() {
  local detected_workers

  log "4/9 avvio Worker Host"
  detected_workers="$(ssh_run "${PUBLIC_IPS[workers]}" 'set -euo pipefail
cd /opt/continuum/current
docker compose --env-file .env -f deploy/compose/distributed/workers.generated.yml config --services |
  awk "/^cloud-worker-[0-9]+$/ { count++ } END { print count+0 }"')"
  [[ "${detected_workers}" =~ ^[1-9][0-9]*$ ]] || die "numero Worker nel Compose non valido: ${detected_workers}"
  [[ "${detected_workers}" == "${CONFIGURED_WORKERS}" ]] ||
    die "Worker nel Compose=${detected_workers}, ma experiment cloud.workers=${CONFIGURED_WORKERS}"
  WORKER_COUNT="${detected_workers}"
  printf '%s\n' "${WORKER_COUNT}" >"${ARTIFACT_DIR}/worker-count.txt"

  ssh_run "${PUBLIC_IPS[workers]}" bash -s -- \
    "${WORKER_COUNT}" "${KAFKA_READY_TIMEOUT_SECONDS}" "${POLL_INTERVAL_SECONDS}" <<'REMOTE'
set -euo pipefail
expected="$1"
timeout_seconds="$2"
poll_seconds="$3"
cd /opt/continuum/current
docker compose --env-file .env -f deploy/compose/distributed/workers.generated.yml up -d
deadline=$(( $(date +%s) + timeout_seconds ))

while (( $(date +%s) < deadline )); do
  running=0
  for ((worker_number = 0; worker_number < expected; worker_number++)); do
    name="cloud-worker-${worker_number}"
    state="$(docker inspect --format '{{.State.Status}}' "${name}" 2>/dev/null || true)"
    if [[ "${state}" == "exited" || "${state}" == "dead" || "${state}" == "restarting" ]]; then
      docker logs "${name}" >&2 || true
      echo "${name} non e avviabile: state=${state}" >&2
      exit 1
    fi
    [[ "${state}" == "running" ]] && ((running += 1))
  done
  ((running == expected)) && exit 0
  sleep "${poll_seconds}"
done

docker compose --env-file .env -f deploy/compose/distributed/workers.generated.yml ps -a >&2
echo "Worker running=${running:-0}, attesi=${expected} dopo ${timeout_seconds}s" >&2
exit 1
REMOTE
  collect_normalized_compose workers
}

wait_for_worker_group() {
  local timeout_seconds="$1"

  ssh_run "${PUBLIC_IPS[cloud-core]}" bash -s -- \
    "${WORKER_COUNT}" "${timeout_seconds}" "${POLL_INTERVAL_SECONDS}" <<'REMOTE'
set -euo pipefail
expected="$1"
timeout_seconds="$2"
poll_seconds="$3"
deadline=$(( $(date +%s) + timeout_seconds ))

while (( $(date +%s) < deadline )); do
  group_state="$(docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:29092 \
    --describe --group cloud-workers --state 2>/dev/null || true)"
  state_and_members="$(awk '$1 == "cloud-workers" {print $(NF-1), $NF}' <<<"${group_state}" | tail -n 1)"
  [[ "${state_and_members}" == "Stable ${expected}" ]] && exit 0
  sleep "${poll_seconds}"
done

echo "consumer group cloud-workers non Stable con ${expected} membri" >&2
echo "${group_state:-group non disponibile}" >&2
exit 1
REMOTE
}

start_edges() {
  log "5/9 avvio Edge Host"
  ssh_run "${PUBLIC_IPS[edge]}" 'set -euo pipefail
cd /opt/continuum/current
docker compose --env-file .env -f deploy/compose/distributed/edge.generated.yml up -d'
  collect_normalized_compose edge
}

wait_for_edges() {
  log "6/9 attesa dei 13 Edge healthy"
  ssh_run "${PUBLIC_IPS[edge]}" bash -s -- \
    "${EDGE_READY_TIMEOUT_SECONDS}" "${POLL_INTERVAL_SECONDS}" <<'REMOTE'
set -euo pipefail
timeout_seconds="$1"
poll_seconds="$2"
deadline=$(( $(date +%s) + timeout_seconds ))

while (( $(date +%s) < deadline )); do
  healthy=0
  for edge_number in $(seq 0 12); do
    name="edge-${edge_number}"
    state="$(docker inspect --format '{{.State.Status}}' "${name}" 2>/dev/null || true)"
    health="$(docker inspect --format '{{.State.Health.Status}}' "${name}" 2>/dev/null || true)"
    if [[ "${state}" == "exited" || "${state}" == "dead" || "${health}" == "unhealthy" ]]; then
      docker logs "${name}" >&2 || true
      echo "${name} non puo diventare healthy: state=${state} health=${health}" >&2
      exit 1
    fi
    [[ "${health}" == "healthy" ]] && ((healthy += 1))
  done
  ((healthy == 13)) && exit 0
  sleep "${poll_seconds}"
done

docker ps -a >&2
echo "non tutti i 13 Edge sono diventati healthy entro ${timeout_seconds}s" >&2
exit 1
REMOTE
}

verify_all_clocks() {
  local role

  log "7/9 verifica sincronizzazione clock"
  for role in "${ROLES[@]}"; do
    wait_for_time_sync "${role}"
  done
  CLOCK_VERIFIED_AT="$(ssh_run "${PUBLIC_IPS[simulator]}" 'date -u +%Y-%m-%dT%H:%M:%S.%NZ')"
}

quick_preflight() {
  log "preflight end-to-end immediatamente precedente alla barriera temporale"
  ssh_run "${PUBLIC_IPS[cloud-core]}" 'set -euo pipefail
[[ "$(docker inspect --format "{{.State.Health.Status}}" kafka)" == "healthy" ]]
[[ "$(docker inspect --format "{{.State.Status}}" global-aggregator)" == "running" ]]
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic edge-aggregates |
  grep -F "PartitionCount: 6" >/dev/null
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic cloud-edge-aggregates |
  grep -F "PartitionCount: 1" >/dev/null'
  verify_kafka_tcp_from_role edge
  verify_kafka_tcp_from_role workers
  wait_for_worker_group "${KAFKA_READY_TIMEOUT_SECONDS}"
  wait_for_edges
  ssh_run "${PUBLIC_IPS[workers]}" bash -s -- "${WORKER_COUNT}" <<'REMOTE'
set -euo pipefail
expected="$1"
for ((worker_number = 0; worker_number < expected; worker_number++)); do
  [[ "$(docker inspect --format '{{.State.Status}}' "cloud-worker-${worker_number}")" == "running" ]]
done
REMOTE
  wait_for_time_sync simulator
}

materialize_replay_start() {
  local simulator_now
  local materialized

  log "8/9 calcolo della barriera temporale comune sulla EC2 Simulator"
  simulator_now="$(ssh_run "${PUBLIC_IPS[simulator]}" 'date -u +%Y-%m-%dT%H:%M:%S.%NZ')"
  materialized="$(runconfig \
    --experiment "${EXPERIMENT_CONFIG_PATH}" \
    --base-time "${simulator_now}" \
    --output "${ARTIFACT_DIR}/effective-config.yaml")" ||
    die "materializzazione effective config fallita"

  REPLAY_START_AT="$(jq -er '.replay_start_at' <<<"${materialized}")"
  [[ "$(jq -er '.workers' <<<"${materialized}")" == "${WORKER_COUNT}" ]] ||
    die "numero Worker cambiato durante la materializzazione della configurazione"
  [[ "$(jq -er '.config_sha256' <<<"${materialized}")" == "${CONFIG_SHA256}" ]] ||
    die "configurazione cambiata durante la materializzazione"
  printf '%s\n' "${REPLAY_START_AT}" >"${ARTIFACT_DIR}/replay-start-at.txt"
}

start_simulators() {
  log "9/9 avvio dei 13 Simulator con REPLAY_START_AT=${REPLAY_START_AT}"
  ssh_run "${PUBLIC_IPS[simulator]}" bash -s -- "${REPLAY_START_AT}" <<'REMOTE'
set -euo pipefail
replay_start_at="$1"
cd /opt/continuum/current

cp .env .run.env.tmp
printf 'REPLAY_START_AT=%s\n' "${replay_start_at}" >>.run.env.tmp
mv .run.env.tmp .run.env
docker compose \
  --env-file .run.env \
  --profile replay \
  -f deploy/compose/distributed/simulator.generated.yml \
  up -d

for edge_number in $(seq 0 12); do
  name="simulator-edge-${edge_number}"
  state="$(docker inspect --format '{{.State.Status}}' "${name}")"
  if [[ "${state}" == "exited" ]]; then
    exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${name}")"
    [[ "${exit_code}" == "0" ]] || {
      docker logs "${name}" >&2 || true
      echo "${name} terminato durante lo startup con exit code ${exit_code}" >&2
      exit 1
    }
  elif [[ "${state}" != "running" ]]; then
    echo "stato inatteso per ${name} durante lo startup: ${state}" >&2
    exit 1
  fi
  docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "${name}" |
    grep -Fx "REPLAY_START_AT=${replay_start_at}" >/dev/null
done
REMOTE
  REPLAY_LAUNCHED_AT="$(ssh_run "${PUBLIC_IPS[simulator]}" 'date -u +%Y-%m-%dT%H:%M:%S.%NZ')"
  collect_normalized_compose simulator .run.env
  write_compose_checksums
}

validate_container_lifecycle() {
  local phase="$1"
  local role

  for role in "${ROLES[@]}"; do
    ssh_run "${PUBLIC_IPS["${role}"]}" bash -s -- \
      "${role}" "${phase}" "${WORKER_COUNT}" <<'REMOTE'
set -euo pipefail
role="$1"
phase="$2"
workers="$3"

check_container() {
  local name="$1"
  local expected_state="$2"
  local expected_health="$3"
  local state restart_count oom_killed exit_code health

  state="$(docker inspect --format '{{.State.Status}}' "${name}" 2>/dev/null || true)"
  [[ -n "${state}" ]] || { echo "container critico mancante: ${name}" >&2; return 1; }
  restart_count="$(docker inspect --format '{{.RestartCount}}' "${name}")"
  oom_killed="$(docker inspect --format '{{.State.OOMKilled}}' "${name}")"
  exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${name}")"
  health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${name}")"

  [[ "${restart_count}" == "0" ]] || { echo "${name} RestartCount=${restart_count}" >&2; return 1; }
  [[ "${oom_killed}" == "false" ]] || { echo "${name} OOMKilled=true" >&2; return 1; }
  if [[ "${expected_state}" == "running" ]]; then
    [[ "${state}" == "running" ]] || { echo "${name} state=${state}, atteso running" >&2; return 1; }
  else
    [[ "${state}" == "exited" && "${exit_code}" == "0" ]] || {
      echo "${name} state=${state} exit=${exit_code}, atteso exited/0" >&2
      return 1
    }
  fi
  [[ "${expected_health}" == "none" || "${health}" == "${expected_health}" ]] || {
    echo "${name} health=${health}, atteso ${expected_health}" >&2
    return 1
  }
}

case "${role}" in
  cloud-core)
    check_container kafka running healthy
    check_container kafka-init exited none
    if [[ "${phase}" == "before" ]]; then
      check_container global-aggregator running none
    else
      check_container global-aggregator exited none
    fi
    ;;
  workers)
    for ((worker_number = 0; worker_number < workers; worker_number++)); do
      check_container "cloud-worker-${worker_number}" running none
    done
    ;;
  edge)
    for edge_number in $(seq 0 12); do
      check_container "mqtt-edge-${edge_number}" running healthy
      check_container "edge-${edge_number}" running healthy
    done
    ;;
  simulator)
    for edge_number in $(seq 0 12); do
      if [[ "${phase}" == "before" ]]; then
        check_container "simulator-edge-${edge_number}" running none
      else
        check_container "simulator-edge-${edge_number}" exited none
      fi
    done
    ;;
esac
REMOTE
  done
  capture_container_states "${phase}"
}

wait_for_run_completion() {
  log "attesa completamento dei 13 Simulator"
  ssh_run "${PUBLIC_IPS[simulator]}" bash -s -- \
    "${RUN_COMPLETION_TIMEOUT_SECONDS}" "${POLL_INTERVAL_SECONDS}" <<'REMOTE'
set -euo pipefail
timeout_seconds="$1"
poll_seconds="$2"
deadline=$(( $(date +%s) + timeout_seconds ))

while (( $(date +%s) < deadline )); do
  completed=0
  for edge_number in $(seq 0 12); do
    name="simulator-edge-${edge_number}"
    state="$(docker inspect --format '{{.State.Status}}' "${name}" 2>/dev/null || true)"
    restart_count="$(docker inspect --format '{{.RestartCount}}' "${name}" 2>/dev/null || true)"
    oom_killed="$(docker inspect --format '{{.State.OOMKilled}}' "${name}" 2>/dev/null || true)"
    [[ "${restart_count}" == "0" && "${oom_killed}" == "false" ]] || {
      echo "${name} lifecycle non valido: restart=${restart_count:-missing} oom=${oom_killed:-missing}" >&2
      exit 1
    }
    if [[ "${state}" == "exited" ]]; then
      exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${name}")"
      [[ "${exit_code}" == "0" ]] || {
        docker logs "${name}" >&2 || true
        echo "${name} terminato con exit code ${exit_code}" >&2
        exit 1
      }
      ((completed += 1))
    elif [[ "${state}" != "running" ]]; then
      echo "stato non valido per ${name}: ${state:-missing}" >&2
      exit 1
    fi
  done
  ((completed == 13)) && exit 0
  sleep "${poll_seconds}"
done

echo "i Simulator non hanno completato entro ${timeout_seconds}s" >&2
exit 1
REMOTE

  log "attesa completamento del Global Aggregator"
  ssh_run "${PUBLIC_IPS[cloud-core]}" bash -s -- \
    "${RUN_COMPLETION_TIMEOUT_SECONDS}" "${POLL_INTERVAL_SECONDS}" <<'REMOTE'
set -euo pipefail
timeout_seconds="$1"
poll_seconds="$2"
deadline=$(( $(date +%s) + timeout_seconds ))

while (( $(date +%s) < deadline )); do
  state="$(docker inspect --format '{{.State.Status}}' global-aggregator 2>/dev/null || true)"
  restart_count="$(docker inspect --format '{{.RestartCount}}' global-aggregator 2>/dev/null || true)"
  oom_killed="$(docker inspect --format '{{.State.OOMKilled}}' global-aggregator 2>/dev/null || true)"
  [[ "${restart_count}" == "0" && "${oom_killed}" == "false" ]] || {
    echo "Global Aggregator lifecycle non valido: restart=${restart_count:-missing} oom=${oom_killed:-missing}" >&2
    exit 1
  }
  if [[ "${state}" == "exited" ]]; then
    exit_code="$(docker inspect --format '{{.State.ExitCode}}' global-aggregator)"
    [[ "${exit_code}" == "0" ]] || {
      docker logs global-aggregator >&2 || true
      echo "Global Aggregator terminato con exit code ${exit_code}" >&2
      exit 1
    }
    docker logs global-aggregator 2>&1 | grep -F 'GLOBAL_REPLAY_COMPLETED' >/dev/null
    exit 0
  fi
  [[ "${state}" == "running" ]] || {
    echo "stato Global Aggregator non valido: ${state:-missing}" >&2
    exit 1
  }
  sleep "${poll_seconds}"
done

docker logs global-aggregator >&2 || true
echo "Global Aggregator non ha completato entro ${timeout_seconds}s" >&2
exit 1
REMOTE
}

main_run() {
  require_command "${TERRAFORM_BIN}"
  require_command go
  require_command git
  require_command jq
  require_command ssh
  require_command sha256sum
  require_command tee
  validate_inputs
  validate_positive_integer KAFKA_READY_TIMEOUT_SECONDS "${KAFKA_READY_TIMEOUT_SECONDS}"
  validate_positive_integer EDGE_READY_TIMEOUT_SECONDS "${EDGE_READY_TIMEOUT_SECONDS}"
  validate_positive_integer RUN_COMPLETION_TIMEOUT_SECONDS "${RUN_COMPLETION_TIMEOUT_SECONDS}"
  validate_positive_integer POLL_INTERVAL_SECONDS "${POLL_INTERVAL_SECONDS}"
  validate_positive_integer METRICS_INTERVAL_SECONDS "${METRICS_INTERVAL_SECONDS}"

  load_experiment_description
  load_terraform_addresses
  ADDRESSES_LOADED="true"
  wait_for_all_ssh
  initialize_artifacts
  verify_prepared_releases
  collect_instance_identities

  log "run=${RUN_ID_VALUE} experiment=${EXPERIMENT_NAME} deployment=${DEPLOYMENT_ID_VALUE}"
  reset_previous_run
  start_metric_collectors
  start_cloud_core
  wait_for_kafka
  verify_kafka_tcp_from_role edge
  verify_kafka_tcp_from_role workers
  start_workers
  wait_for_worker_group "${KAFKA_READY_TIMEOUT_SECONDS}"
  start_edges
  wait_for_edges
  verify_all_clocks
  quick_preflight
  materialize_replay_start
  start_simulators
  validate_container_lifecycle before
  wait_for_run_completion
  validate_container_lifecycle after
  verify_metric_collectors

  RUN_FINISHED_AT="$(ssh_run "${PUBLIC_IPS[simulator]}" 'date -u +%Y-%m-%dT%H:%M:%S.%NZ')"
  RUN_STATUS="completed"
  log "run completata correttamente"
}

main_run "$@"

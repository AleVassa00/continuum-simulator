#!/usr/bin/env bash
set -Eeuo pipefail
set +x

export AWS_SCRIPT_LOG_PREFIX="run-experiment"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/aws-common.sh"

load_pilot_environment

readonly EXPERIMENT_CONFIG_INPUT="${EXPERIMENT_CONFIG:-${REPO_ROOT}/experiments/cloud-scale-w1.yaml}"
readonly ARTIFACTS_ROOT="${ARTIFACTS_ROOT:-${REPO_ROOT}/artifacts/aws-runs}"
readonly KAFKA_READY_TIMEOUT_SECONDS="${KAFKA_READY_TIMEOUT_SECONDS:-300}"
readonly EDGE_READY_TIMEOUT_SECONDS="${EDGE_READY_TIMEOUT_SECONDS:-300}"
readonly RUN_COMPLETION_TIMEOUT_SECONDS="${RUN_COMPLETION_TIMEOUT_SECONDS:-3600}"
readonly POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"
readonly METRICS_INTERVAL_SECONDS="${METRICS_INTERVAL_SECONDS:-5}"
readonly PYTHON_BIN="${PYTHON_BIN:-python3}"

declare -A METRICS_PIDS

EXPERIMENT_CONFIG_PATH=""
EXPERIMENT_NAME=""
CONFIGURED_WORKERS="0"
CONFIGURED_PARTITIONS="0"
WORKER_COUNT="0"
DEPLOYED_GIT_COMMIT_SHA=""
DEPLOYED_AT=""
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
KAFKA_METRICS_STARTED="false"

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
  [[ "${EXPERIMENT_NAME}" =~ ^[A-Za-z0-9._-]+$ ]] ||
    die "experiment.name puo contenere solo lettere, numeri, punto, underscore e trattino"
  CONFIGURED_WORKERS="$(jq -er '.workers' <<<"${description}")"
  CONFIGURED_PARTITIONS="$(jq -er '.kafka_partitions | select(type == "number" and . > 0 and . == floor)' <<<"${description}")"
}

initialize_artifacts() {
  local requested_run_id="${RUN_ID:-}"
  local artifacts_root_path
  local experiment_artifacts_path
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
  experiment_artifacts_path="${artifacts_root_path}/${EXPERIMENT_NAME}"
  mkdir -p "${experiment_artifacts_path}"
  ARTIFACT_DIR="${experiment_artifacts_path}/${RUN_ID_VALUE}"
  if ! mkdir "${ARTIFACT_DIR}"; then
    die "directory artefatti gia esistente o non creabile: ${ARTIFACT_DIR}"
  fi
  mkdir "${ARTIFACT_DIR}/logs" "${ARTIFACT_DIR}/metrics" "${ARTIFACT_DIR}/compose"
  exec > >(tee -a "${ARTIFACT_DIR}/orchestrator.log") 2>&1
  trap finalize_run EXIT

  cp "${EXPERIMENT_CONFIG_PATH}" "${ARTIFACT_DIR}/experiment.yaml"
  cp "${RESOURCE_PROFILE_PATH}" "${ARTIFACT_DIR}/resource-profile.env"

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
    --arg git_commit_sha "${DEPLOYED_GIT_COMMIT_SHA}" \
    --arg deployed_at "${DEPLOYED_AT}" \
    --arg orchestration_started_at "${ORCHESTRATION_STARTED_AT}" \
    --arg clock_verified_at "${CLOCK_VERIFIED_AT}" \
    --arg replay_start_at "${REPLAY_START_AT}" \
    --arg replay_launched_at "${REPLAY_LAUNCHED_AT}" \
    --arg finished_at "${RUN_FINISHED_AT}" \
    --argjson workers "${WORKER_COUNT:-0}" \
    --argjson kafka_partitions "${CONFIGURED_PARTITIONS}" \
    --argjson metrics_interval_seconds "${METRICS_INTERVAL_SECONDS}" \
    --argjson instance_identities "${INSTANCE_IDENTITIES}" \
    --argjson exit_code "${exit_code}" \
    '{
      run_id: $run_id,
      experiment: $experiment,
      status: $status,
      git_commit_sha: $git_commit_sha,
      deployed_at: $deployed_at,
      workers: $workers,
      kafka_partitions: $kafka_partitions,
      orchestration_started_at: $orchestration_started_at,
      clock_verified_at: $clock_verified_at,
      replay_start_at: $replay_start_at,
      replay_launched_at: $replay_launched_at,
      finished_at: $finished_at,
      orchestrator_exit_code: $exit_code,
      metrics: {
        interval_seconds: $metrics_interval_seconds,
        files: "metrics/{simulator,edge,cloud-core,workers}.log",
        kafka_lag: "metrics/kafka-lag.log",
        kafka_lag_final: "kafka-consumer-groups-final.txt"
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
        retrieval_method: "Query every recorded InstanceId over orchestration_started_at..finished_at.",
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
lag_pidfile="/tmp/continuum-kafka-lag-$1.pid"
if [[ -f "${lag_pidfile}" ]]; then
  kill "$(cat "${lag_pidfile}")" 2>/dev/null || true
  rm -f "${lag_pidfile}"
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

export_postgres_results() {
  local sink
  sink="$(jq -er '.services["global-aggregator"].environment.GLOBAL_SINK_TYPE' \
    "${ARTIFACT_DIR}/compose/cloud-core.normalized.json")" || return 1
  [[ "$sink" == postgres ]] || return 0
  log "esportazione aggregati PostgreSQL negli artefatti della run"
  if ! ssh_run "${PUBLIC_IPS[cloud-core]}" bash -s \
    <"${SCRIPT_DIR}/export-postgres.sh" >"${ARTIFACT_DIR}/global-aggregates.ndjson.tmp"; then
    log "esportazione PostgreSQL fallita; risultati non archiviati"
    return 1
  fi
  [[ -s "${ARTIFACT_DIR}/global-aggregates.ndjson.tmp" ]] || return 1
  mv "${ARTIFACT_DIR}/global-aggregates.ndjson.tmp" "${ARTIFACT_DIR}/global-aggregates.ndjson"
}

finalize_run() {
  local exit_code="$?"
  local postprocess_exit

  trap - EXIT
  set +e

  stop_metric_collectors

  if [[ -n "${ARTIFACT_DIR}" ]]; then
    if [[ "${KAFKA_METRICS_STARTED}" == "true" ]]; then
      ssh_run "${PUBLIC_IPS[cloud-core]}" bash -s -- \
        "${METRICS_INTERVAL_SECONDS}" "${RUN_ID_VALUE}" once "${CONFIGURED_PARTITIONS}" \
        <"${SCRIPT_DIR}/collect-kafka-lag.sh" \
        >"${ARTIFACT_DIR}/kafka-consumer-groups-final.txt" 2>&1
    fi

    collect_host_logs

    if [[ "${exit_code}" == "0" && "${RUN_STATUS}" == "completed" ]]; then
      if ! export_postgres_results; then
        log "export PostgreSQL fallito"
        exit_code=1
      fi
    fi

    write_run_metadata "${exit_code}"
    "${PYTHON_BIN}" "${SCRIPT_DIR}/summarize-run.py" "${ARTIFACT_DIR}"
    postprocess_exit="$?"

    if ! jq --argjson result "${postprocess_exit}" \
      '. + {postprocess_exit_code: $result, postprocess_status: (if $result == 0 then "success" elif $result == 2 then "quality_failed" else "failed" end)}' \
      "${ARTIFACT_DIR}/run-metadata.json" >"${ARTIFACT_DIR}/run-metadata.json.tmp"; then
      log "impossibile aggiornare i metadata del post-processing"
      [[ "${postprocess_exit}" != "0" ]] || postprocess_exit=1
    elif ! mv "${ARTIFACT_DIR}/run-metadata.json.tmp" "${ARTIFACT_DIR}/run-metadata.json"; then
      [[ "${postprocess_exit}" != "0" ]] || postprocess_exit=1
    fi

    log "artefatti run: ${ARTIFACT_DIR}"
    [[ "${exit_code}" != "0" ]] || exit_code="${postprocess_exit}"
  fi

  exit "${exit_code}"
}

verify_deployed_config() {
  local role
  local filename
  local deployment_info

  for role in "${ROLES[@]}"; do
    case "${role}" in
      simulator) filename="simulator.generated.yml" ;;
      edge) filename="edge.generated.yml" ;;
      cloud-core) filename="cloud-core.generated.yml" ;;
      workers) filename="workers.generated.yml" ;;
    esac

    ssh_run "${PUBLIC_IPS[${role}]}" \
      "test -f /opt/continuum/current/.env && test -f /opt/continuum/current/deploy/compose/distributed/${filename}" ||
      die "deployment applicativo assente o incompleto su ${role}; eseguire deploy-pilot.sh"
  done

  deployment_info="$(ssh_run "${PUBLIC_IPS[cloud-core]}" \
    'cat /opt/continuum/current/deployment-info.json')" ||
    die "deployment-info.json non leggibile su cloud-core"
  jq -e --arg experiment "${EXPERIMENT_NAME}" \
    '.experiment == $experiment' <<<"${deployment_info}" >/dev/null ||
    die "deployment remoto preparato per un altro esperimento; eseguire deploy-pilot.sh"

  DEPLOYED_GIT_COMMIT_SHA="$(jq -r '.git_commit_sha // ""' <<<"${deployment_info}")"
  DEPLOYED_AT="$(jq -r '.deployed_at // ""' <<<"${deployment_info}")"
  printf '%s\n' "${deployment_info}" | jq . >"${ARTIFACT_DIR}/deployment-info.json"
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


initialize_rds_schema() {
  local sink
  sink="$(jq -er '.services["global-aggregator"].environment.GLOBAL_SINK_TYPE' \
    "${ARTIFACT_DIR}/compose/cloud-core.normalized.json")" || return 1
  [[ "$sink" == postgres ]] || return 0
  log "init/verifica schema RDS dopo l'arresto dei container precedenti"
  ssh_run "${PUBLIC_IPS[cloud-core]}" \
    'bash /opt/continuum/current/deploy/scripts/aws/init-rds-schema.sh'
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

start_kafka_metrics() {
  ssh_run "${PUBLIC_IPS[cloud-core]}" bash -s -- \
    "${METRICS_INTERVAL_SECONDS}" "${RUN_ID_VALUE}" loop "${CONFIGURED_PARTITIONS}" \
    <"${SCRIPT_DIR}/collect-kafka-lag.sh" >"${ARTIFACT_DIR}/metrics/kafka-lag.log" 2>&1 &
  METRICS_PIDS[kafka-lag]="$!"
  KAFKA_METRICS_STARTED="true"
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
  [[ "${KAFKA_METRICS_STARTED}" == "true" ]] && kill -0 "${METRICS_PIDS[kafka-lag]}" 2>/dev/null ||
    die "collector Kafka lag terminato prematuramente"
}

collect_normalized_compose() {
  local role="$1"
  local env_file="${2:-.env}"
  local compose_file
  local profile="__none__"

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

  ssh_run "${PUBLIC_IPS[${role}]}" bash -s -- \
    "${env_file}" "${compose_file}" "${profile}" "${REPLAY_START_AT:-1970-01-01T00:00:00Z}" <<'REMOTE' \
    >"${ARTIFACT_DIR}/compose/${role}.normalized.yml"
set -euo pipefail
env_file="$1"
compose_file="$2"
profile="$3"
[[ "${profile}" != "__none__" ]] || profile=""
cd /opt/continuum/current
args=(docker compose --env-file "${env_file}")
[[ -z "${profile}" ]] || args+=(--profile "${profile}")
args+=(-f "deploy/compose/distributed/${compose_file}" config)
GLOBAL_POSTGRES_PASSWORD=__REDACTED__ REPLAY_START_AT="$4" "${args[@]}"
REMOTE

  ssh_run "${PUBLIC_IPS[${role}]}" bash -s -- \
    "${env_file}" "${compose_file}" "${profile}" "${REPLAY_START_AT:-1970-01-01T00:00:00Z}" <<'REMOTE' \
    >"${ARTIFACT_DIR}/compose/${role}.normalized.json"
set -euo pipefail
profile="$3"
[[ "${profile}" != "__none__" ]] || profile=""
cd /opt/continuum/current
args=(docker compose --env-file "$1")
[[ -z "${profile}" ]] || args+=(--profile "${profile}")
GLOBAL_POSTGRES_PASSWORD=__REDACTED__ REPLAY_START_AT="$4" \
  "${args[@]}" -f "deploy/compose/distributed/$2" config --format json
REMOTE
}

check_host_budgets() {
  local role
  for role in "${ROLES[@]}"; do
    collect_normalized_compose "${role}"
    ssh_run "${PUBLIC_IPS["${role}"]}" bash -s <<'REMOTE' >"${ARTIFACT_DIR}/capacity-${role}.json"
set -euo pipefail
printf '{"cpus":%s,"memory_bytes":%s}\n' \
  "$(nproc)" "$(awk '/MemTotal:/ {printf "%.0f", $2 * 1024}' /proc/meminfo)"
REMOTE
    "${PYTHON_BIN}" "${SCRIPT_DIR}/check-resource-budget.py" \
      "${ARTIFACT_DIR}/compose/${role}.normalized.json" "${ARTIFACT_DIR}/capacity-${role}.json" \
      >"${ARTIFACT_DIR}/resource-budget-${role}.json" || die "budget container incompatibile con host ${role}"
  done
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
    "${KAFKA_READY_TIMEOUT_SECONDS}" "${POLL_INTERVAL_SECONDS}" "${CONFIGURED_PARTITIONS}" <<'REMOTE'
set -euo pipefail
timeout_seconds="$1"
poll_seconds="$2"
partition_count="$3"
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
      --bootstrap-server kafka:29092 --describe --topic cloud-partition-aggregates)"

    grep -E "PartitionCount: ${partition_count}([[:space:]]|$)" <<<"${edge_topic}" >/dev/null || exit 1
    grep -E "PartitionCount: 1([[:space:]]|$)" <<<"${cloud_topic}" >/dev/null || exit 1
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
if ! docker compose --env-file .env -f deploy/compose/distributed/cloud-core.generated.yml run --rm --no-deps -T global-aggregator --reset-postgres-if-enabled; then
  echo "Reset PostgreSQL non riuscito: GlobalAggregator non avviato" >&2
  exit 1
fi
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

  ssh_run "${PUBLIC_IPS[cloud-core]}" bash -s -- "${CONFIGURED_PARTITIONS}" <<'REMOTE'
set -euo pipefail
partition_count="$1"
[[ "$(docker inspect --format "{{.State.Health.Status}}" kafka)" == "healthy" ]]
[[ "$(docker inspect --format "{{.State.Status}}" global-aggregator)" == "running" ]]
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic edge-aggregates |
  grep -E "PartitionCount: ${partition_count}([[:space:]]|$)" >/dev/null
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic cloud-partition-aggregates |
  grep -E "PartitionCount: 1([[:space:]]|$)" >/dev/null
REMOTE

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
  case "${expected_state}" in
    running)
      [[ "${state}" == "running" ]] || {
        echo "${name} state=${state}, atteso running" >&2
        return 1
      }
      ;;
    exited)
      [[ "${state}" == "exited" && "${exit_code}" == "0" ]] || {
        echo "${name} state=${state} exit=${exit_code}, atteso exited/0" >&2
        return 1
      }
      ;;
    running-or-exited-0)
      [[ "${state}" == "running" || ( "${state}" == "exited" && "${exit_code}" == "0" ) ]] || {
        echo "${name} state=${state} exit=${exit_code}, atteso running oppure exited/0" >&2
        return 1
      }
      ;;
    *)
      echo "stato atteso non supportato per ${name}: ${expected_state}" >&2
      return 1
      ;;
  esac
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
      if [[ "${phase}" == "before" ]]; then
        check_container "edge-${edge_number}" running healthy
      else
        # Edge exits successfully after handling Simulator EOS and publishing its final aggregate.
        # Healthchecks are meaningful only while the process is running.
        check_container "edge-${edge_number}" exited none
      fi
    done
    ;;
  simulator)
    for edge_number in $(seq 0 12); do
      if [[ "${phase}" == "before" ]]; then
        # A fast shard may already have completed between compose up and this snapshot.
        check_container "simulator-edge-${edge_number}" running-or-exited-0 none
      else
        check_container "simulator-edge-${edge_number}" exited none
      fi
    done
    ;;
esac
REMOTE
  done
}

workload_role_completed() {
  local role="$1"

  ssh_run "${PUBLIC_IPS[${role}]}" bash -s -- "${role}" <<'REMOTE'
set -euo pipefail
role="$1"
names=()

case "${role}" in
  simulator)
    for i in $(seq 0 12); do names+=("simulator-edge-${i}"); done
    ;;
  edge)
    for i in $(seq 0 12); do names+=("edge-${i}"); done
    ;;
  cloud-core)
    names=(global-aggregator)
    ;;
  *)
    exit 1
    ;;
esac

complete=true
for name in "${names[@]}"; do
  snapshot="$(docker inspect --format '{{.State.Status}}|{{.RestartCount}}|{{.State.OOMKilled}}|{{.State.ExitCode}}' "${name}")"
  case "${snapshot}" in
    'exited|0|false|0') ;;
    'running|0|false|0') complete=false ;;
    *)
      echo "${name} lifecycle non valido: ${snapshot}" >&2
      exit 1
      ;;
  esac
done

if [[ "${role}" == cloud-core && "${complete}" == true ]]; then
  docker logs global-aggregator 2>&1 | grep -F 'GLOBAL_REPLAY_COMPLETED' >/dev/null
fi

printf '%s\n' "${complete}"
REMOTE
}

wait_for_run_completion() {
  local deadline=$(( $(date +%s) + RUN_COMPLETION_TIMEOUT_SECONDS ))
  local simulators_done edges_done global_done

  log "attesa completamento Simulator, Edge e Global Aggregator"

  while (( $(date +%s) < deadline )); do
    simulators_done="$(workload_role_completed simulator)" || return 1
    edges_done="$(workload_role_completed edge)" || return 1
    global_done="$(workload_role_completed cloud-core)" || return 1

    if [[ "${simulators_done}" == true &&
          "${edges_done}" == true &&
          "${global_done}" == true ]]; then
      return 0
    fi

    sleep "${POLL_INTERVAL_SECONDS}"
  done

  echo "run non completata entro ${RUN_COMPLETION_TIMEOUT_SECONDS}s" >&2
  return 1
}

main_run() {
  require_command "${PYTHON_BIN}"
  "${PYTHON_BIN}" -c 'import sys; assert sys.version_info >= (3, 9), "Python >= 3.9 required"'
  require_command go
  require_command jq
  require_command ssh
  require_command tee

  init_aws_context
  require_command "${TERRAFORM_BIN}"

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
  verify_deployed_config
  collect_instance_identities
  check_host_budgets

  log "run=${RUN_ID_VALUE} experiment=${EXPERIMENT_NAME}"

  reset_previous_run
  initialize_rds_schema
  start_metric_collectors
  start_cloud_core
  wait_for_kafka
  verify_kafka_tcp_from_role edge
  verify_kafka_tcp_from_role workers
  start_workers
  wait_for_worker_group "${KAFKA_READY_TIMEOUT_SECONDS}"
  start_kafka_metrics
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

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main_run "$@"
fi

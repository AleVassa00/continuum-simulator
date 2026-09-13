#!/usr/bin/env bash
# Executed over SSH on cloud-core. One read-only Go observer for the whole run.
set -Eeuo pipefail

interval="$1"
run_id="$2"
mode="${3:-loop}"
partition_count="${4:-}"

[[ "${partition_count}" =~ ^[1-9][0-9]*$ ]] || exit 2
[[ "${interval}" =~ ^[1-9][0-9]*$ && "${run_id}" =~ ^[A-Za-z0-9._-]+$ ]] || exit 2
[[ "${mode}" == loop || "${mode}" == once ]] || exit 2

pidfile="/tmp/continuum-kafka-lag-${run_id}.pid"
container_id=""
attach_pid=""

cleanup() {
  if [[ -n "${container_id}" ]]; then
    docker rm -f "${container_id}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${attach_pid}" ]]; then
    wait "${attach_pid}" 2>/dev/null || true
  fi
  if [[ "${mode}" == loop ]]; then
    rm -f "${pidfile}"
  fi
}
trap cleanup EXIT
trap 'exit 0' HUP INT TERM

if [[ "${mode}" == loop ]]; then
  printf '%s\n' "$$" >"${pidfile}"
fi

# Riusa la stessa immagine del Global Aggregator già distribuita sull'host,
# anche quando il container applicativo è terminato.
image_id="$(docker inspect --format '{{.Image}}' global-aggregator)"

container_id="$(docker create \
  --network container:kafka \
  --cpus 0.05 \
  --memory 64m \
  --memory-swap 64m \
  --env "SOURCE_PARTITION_COUNT=${partition_count}" \
  --entrypoint /app/kafka-lag-collector \
  "${image_id}" \
  -broker localhost:29092 \
  -interval "${interval}s" \
  -mode "${mode}")"

docker start --attach "${container_id}" &
attach_pid=$!
wait "${attach_pid}"

exit "$(docker inspect --format '{{.State.ExitCode}}' "${container_id}")"

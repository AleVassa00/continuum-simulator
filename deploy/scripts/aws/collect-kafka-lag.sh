#!/usr/bin/env bash
# Executed over SSH on cloud-core. One read-only Go observer for the whole run.
set -Eeuo pipefail
interval="$1"
run_id="$2"
mode="${3:-loop}"
[[ "${interval}" =~ ^[1-9][0-9]*$ && "${run_id}" =~ ^[A-Za-z0-9._-]+$ ]] || exit 2
[[ "${mode}" == loop || "${mode}" == once ]] || exit 2
pidfile="/tmp/continuum-kafka-lag-${run_id}.pid"
container_id=""
attach_pid=""
cleanup() {
  # Only remove the helper created by this invocation, never the application.
  if [[ -n "${container_id}" ]]; then docker rm -f "${container_id}" >/dev/null 2>&1 || true; fi
  if [[ -n "${attach_pid}" ]]; then wait "${attach_pid}" 2>/dev/null || true; fi
  if [[ "${mode}" == loop ]]; then rm -f "${pidfile}"; fi
}
trap cleanup EXIT
trap 'exit 0' HUP INT TERM
if [[ "${mode}" == loop ]]; then
  printf '%s\n' "$$" >"${pidfile}"
fi
# Reuse the prepared release image, including when Global has already exited.
# Share Kafka's network namespace, NOT its CPU/memory cgroup. No Java in broker.
image_id="$(docker inspect --format '{{.Image}}' global-aggregator)"
container_id="$(docker create --network container:kafka --cpus 0.05 \
  --memory 64m --memory-swap 64m --entrypoint /app/kafka-lag-collector \
  "${image_id}" -broker localhost:29092 -interval "${interval}s" -mode "${mode}")"
docker start --attach "${container_id}" &
attach_pid=$!
wait "${attach_pid}"
# docker start --attach need not propagate the container process exit status.
exit "$(docker inspect --format '{{.State.ExitCode}}' "${container_id}")"

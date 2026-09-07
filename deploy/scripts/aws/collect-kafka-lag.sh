#!/usr/bin/env bash
# Executed over SSH on cloud-core. Each query has its own timestamp and status.
set -Eeuo pipefail
interval="$1"
run_id="$2"
mode="${3:-loop}"
[[ "${interval}" =~ ^[1-9][0-9]*$ && "${run_id}" =~ ^[A-Za-z0-9._-]+$ ]] || exit 2
[[ "${mode}" == loop || "${mode}" == once ]] || exit 2
pidfile="/tmp/continuum-kafka-lag-${run_id}.pid"
if [[ "${mode}" == loop ]]; then
  printf '%s\n' "$$" >"${pidfile}"
  trap 'rm -f "${pidfile}"' EXIT
  trap 'exit 0' HUP INT TERM
fi
while true; do
  for group in cloud-workers global-aggregator; do
    printf '===== group %s sample %s =====\n' "${group}" "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    if timeout 30s docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka:29092 --describe --group "${group}"; then
      printf '===== query_exit 0 at %s =====\n' "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    else
      result="$?"
      printf '===== query_exit %s at %s =====\n' "${result}" "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    fi
  done
  [[ "${mode}" == loop ]] || break
  sleep "${interval}" &
  wait $!
done

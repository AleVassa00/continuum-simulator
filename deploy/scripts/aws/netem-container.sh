#!/usr/bin/env bash
set -Eeuo pipefail

readonly NETEM_QUEUE_LIMIT_PACKETS=10000

usage() {
  echo "usage: netem-container.sh <apply|show|snapshot|clear|monitor> <container-name[,container-name...]> [delay|interval-seconds] [rate] [target-env]" >&2
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "comando richiesto non disponibile: $1" >&2
    exit 1
  }
}

network_namespace() {
  local container="$1"
  local state pid route interface

  state="$(docker inspect --format '{{.State.Status}}' "${container}" 2>/dev/null || true)"
  [[ "${state}" == "running" ]] || return 1
  pid="$(docker inspect --format '{{.State.Pid}}' "${container}")"
  [[ "${pid}" =~ ^[1-9][0-9]*$ ]] || {
    echo "PID di rete non valido per ${container}: ${pid}" >&2
    return 1
  }
  route="$(sudo -n nsenter -t "${pid}" -n -- ip -o route show default)"
  interface="$(awk '{for (i = 1; i <= NF; i++) if ($i == "dev") {print $(i + 1); exit}}' <<<"${route}")"
  [[ -n "${interface}" ]] || {
    echo "interfaccia della default route non trovata per ${container}" >&2
    return 1
  }
  printf '%s\t%s\n' "${pid}" "${interface}"
}

target_endpoint() {
  local container="$1"
  local environment_name="$2"
  local endpoint host port

  endpoint="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "${container}" |
    awk -F= -v name="${environment_name}" '$1 == name {sub(/^[^=]*=/, ""); print; exit}')"
  [[ -n "${endpoint}" ]] || {
    echo "${environment_name} non presente in ${container}" >&2
    return 1
  }
  endpoint="${endpoint#tcp://}"
  host="${endpoint%:*}"
  port="${endpoint##*:}"
  [[ "${host}" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ && "${port}" =~ ^[1-9][0-9]*$ && ${port} -le 65535 ]] || {
    echo "endpoint IPv4 non valido in ${container}/${environment_name}: ${endpoint}" >&2
    return 1
  }
  printf '%s\t%s\n' "${host}" "${port}"
}

show_qdisc() {
  local container="$1"
  local strict="$2"
  local namespace pid interface state

  state="$(docker inspect --format '{{.State.Status}}' "${container}" 2>/dev/null || true)"
  if [[ "${state}" != "running" ]]; then
    printf 'container=%s state=%s qdisc=unavailable\n' "${container}" "${state:-missing}"
    [[ "${strict}" == "false" ]] && return 0
    return 1
  fi

  namespace="$(network_namespace "${container}")"
  IFS=$'\t' read -r pid interface <<<"${namespace}"
  printf 'container=%s pid=%s interface=%s\n' "${container}" "${pid}" "${interface}"
  sudo -n nsenter -t "${pid}" -n -- tc -s qdisc show dev "${interface}"
  sudo -n nsenter -t "${pid}" -n -- tc -s filter show dev "${interface}" parent 1:
}

apply_qdisc() {
  local container="$1"
  local namespace pid interface endpoint target_host target_port qdisc
  local root_string netem_string filter_string
  local -a root_command netem_command filter_command

  namespace="$(network_namespace "${container}")"
  IFS=$'\t' read -r pid interface <<<"${namespace}"
  endpoint="$(target_endpoint "${container}" "${target_environment}")"
  IFS=$'\t' read -r target_host target_port <<<"${endpoint}"

  root_command=(sudo -n nsenter -t "${pid}" -n -- tc qdisc replace dev "${interface}" root handle 1: prio bands 2 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0)
  netem_command=(sudo -n nsenter -t "${pid}" -n -- tc qdisc replace dev "${interface}" parent 1:2 handle 20: netem limit "${NETEM_QUEUE_LIMIT_PACKETS}")
  [[ "${delay}" == "0s" ]] || netem_command+=(delay "${delay}")
  [[ "${rate}" == "unlimited" ]] || netem_command+=(rate "${rate}")
  filter_command=(sudo -n nsenter -t "${pid}" -n -- tc filter replace dev "${interface}" protocol ip parent 1: prio 1 u32 match ip protocol 6 0xff match ip dst "${target_host}/32" match ip dport "${target_port}" 0xffff flowid 1:2)

  "${root_command[@]}"
  "${netem_command[@]}"
  "${filter_command[@]}"

  qdisc="$(sudo -n nsenter -t "${pid}" -n -- tc qdisc show dev "${interface}")"
  grep -Eq '^qdisc netem 20: parent 1:2 ' <<<"${qdisc}" || {
    echo "netem non risulta applicato a ${container}/${interface}" >&2
    return 1
  }

  printf -v root_string '%q ' "${root_command[@]}"
  printf -v netem_string '%q ' "${netem_command[@]}"
  printf -v filter_string '%q ' "${filter_command[@]}"
  printf '%s\t%s\t%s\t%s:%s\t%s\t%s\t%s ; %s ; %s\n' \
    "${container}" "${pid}" "${interface}" "${target_host}" "${target_port}" \
    "${delay}" "${rate}" "${root_string% }" "${netem_string% }" "${filter_string% }"
}

clear_qdisc() {
  local container="$1"
  local namespace pid interface qdisc

  if ! namespace="$(network_namespace "${container}")"; then
    printf 'container=%s cleanup=skipped-not-running\n' "${container}"
    return 0
  fi
  IFS=$'\t' read -r pid interface <<<"${namespace}"
  qdisc="$(sudo -n nsenter -t "${pid}" -n -- tc qdisc show dev "${interface}")"
  if ! grep -Eq '^qdisc netem ' <<<"${qdisc}"; then
    printf 'container=%s pid=%s interface=%s cleanup=already-absent\n' "${container}" "${pid}" "${interface}"
    return 0
  fi
  sudo -n nsenter -t "${pid}" -n -- tc qdisc del dev "${interface}" root
  printf 'container=%s pid=%s interface=%s cleanup=removed\n' "${container}" "${pid}" "${interface}"
}

(( $# >= 2 )) || usage
action="$1"
container_csv="$2"
delay="${3:-0s}"
rate="${4:-unlimited}"
target_environment="${5:-}"

[[ "${action}" =~ ^(apply|show|snapshot|clear|monitor)$ ]] || usage
[[ -n "${container_csv}" ]] || usage

require_command docker
require_command sudo
require_command nsenter
require_command ip
require_command tc
sudo -n true

declare -a containers=()
IFS=',' read -r -a containers <<<"${container_csv}"
for container in "${containers[@]}"; do
  [[ "${container}" =~ ^[A-Za-z0-9_.-]+$ ]] || {
    echo "nome container non valido: ${container}" >&2
    exit 2
  }
done

case "${action}" in
  apply)
    [[ "${target_environment}" =~ ^[A-Z][A-Z0-9_]*$ ]] || {
      echo "variabile endpoint non valida: ${target_environment}" >&2
      exit 2
    }
    printf 'container\tpid\tinterface\ttarget\tdelay\trate\tcommand\n'
    for container in "${containers[@]}"; do
      apply_qdisc "${container}"
    done
    ;;
  show)
    for container in "${containers[@]}"; do
      show_qdisc "${container}" true
    done
    ;;
  snapshot)
    for container in "${containers[@]}"; do
      show_qdisc "${container}" false
    done
    ;;
  clear)
    for container in "${containers[@]}"; do
      clear_qdisc "${container}"
    done
    ;;
  monitor)
    interval_seconds="${delay}"
    [[ "${interval_seconds}" =~ ^[1-9][0-9]*$ ]] || {
      echo "intervallo monitor netem non valido: ${interval_seconds}" >&2
      exit 2
    }
    trap 'exit 0' HUP INT TERM
    while true; do
      printf '===== sample %s =====\n' "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
      for container in "${containers[@]}"; do
        show_qdisc "${container}" false
      done
      sleep "${interval_seconds}" &
      wait $!
    done
    ;;
esac

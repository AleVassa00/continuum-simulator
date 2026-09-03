#!/usr/bin/env bash
set -Eeuo pipefail

exec > >(tee -a /var/log/continuum-bootstrap.log) 2>&1

readonly COMPOSE_VERSION="v2.40.3"
readonly CONTINUUM_DIR="/opt/continuum"

install_docker_apt() {
  export DEBIAN_FRONTEND=noninteractive

  apt-get update -y
  apt-get install -y ca-certificates curl
  install -m 0755 -d /etc/apt/keyrings

  . /etc/os-release
  curl -fsSL "https://download.docker.com/linux/${ID}/gpg" \
    -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc

  local architecture
  architecture="$(dpkg --print-architecture)"
  printf '%s\n' \
    "deb [arch=${architecture} signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list

  apt-get update -y
  apt-get install -y \
    containerd.io \
    docker-buildx-plugin \
    docker-ce \
    docker-ce-cli \
    docker-compose-plugin
}

install_docker_rpm() {
  local package_manager="$1"

  "${package_manager}" install -y ca-certificates curl docker
}

install_compose_plugin() {
  local architecture

  case "$(uname -m)" in
    x86_64)
      architecture="x86_64"
      ;;
    aarch64 | arm64)
      architecture="aarch64"
      ;;
    *)
      echo "Unsupported architecture for Docker Compose: $(uname -m)" >&2
      return 1
      ;;
  esac

  install -m 0755 -d /usr/local/lib/docker/cli-plugins
  curl -fsSL \
    "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-${architecture}" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  chmod 0755 /usr/local/lib/docker/cli-plugins/docker-compose
}

if ! command -v docker >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    install_docker_apt
  elif command -v dnf >/dev/null 2>&1; then
    install_docker_rpm dnf
  elif command -v yum >/dev/null 2>&1; then
    install_docker_rpm yum
  else
    echo "Unsupported Linux distribution: no apt-get, dnf or yum found" >&2
    exit 1
  fi
fi

systemctl enable --now docker

if ! docker compose version >/dev/null 2>&1; then
  install_compose_plugin
fi

primary_user="$(getent passwd 1000 | cut -d: -f1 || true)"
if [[ -n "${primary_user}" ]]; then
  usermod -aG docker "${primary_user}"
fi

install -d -m 0755 "${CONTINUUM_DIR}"
if [[ -n "${primary_user}" ]]; then
  chown "${primary_user}:${primary_user}" "${CONTINUUM_DIR}"
fi

docker --version
docker compose version

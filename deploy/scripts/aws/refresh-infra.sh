#!/usr/bin/env bash
set -Eeuo pipefail
set +x

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/aws-common.sh"

load_pilot_environment

terraform -chdir="${REPO_ROOT}/deploy/terraform" init -input=false
terraform -chdir="${REPO_ROOT}/deploy/terraform" apply \
  -refresh-only \
  -auto-approve \
  -input=false

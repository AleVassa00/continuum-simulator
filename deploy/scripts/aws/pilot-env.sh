#!/usr/bin/env bash
# Shared local configuration only. Never copy these files to an EC2/build context.
load_pilot_environment() {
  [[ "${CONTINUUM_ENV_LOADED:-}" != 1 ]] || return 0
  set +x
  local root="$1" key path
  local aws_file="${AWS_SESSION_FILE:-$1/deploy/aws-session.env}"
  local pilot_file="${CONTINUUM_PILOT_FILE:-${HOME}/.config/continuum/pilot.env}"
  local secrets_file="${CONTINUUM_SECRETS_FILE:-${HOME}/.config/continuum/secrets.env}"
  local -A overrides=()
  for key in AWS_SESSION_FILE CONTINUUM_PILOT_FILE CONTINUUM_SECRETS_FILE; do
    if [[ -n "${!key:-}" && ! -f "${!key}" ]]; then
      printf 'Configured environment file does not exist: %s\n' "$key" >&2
      return 1
    fi
  done
  # Explicit shell settings win over local files, including empty values.
  for key in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE \
    AWS_REGION AWS_DEFAULT_REGION TF_VAR_rds_password SSH_USER SSH_KEY_PATH \
    RESOURCE_PROFILE ALLOW_DIRTY_WORKTREE TERRAFORM_BIN TERRAFORM_DIR PYTHON_BIN \
    EXPERIMENT_CONFIG ARTIFACTS_ROOT DEPLOYMENT_ID RUN_ID SSH_WAIT_ATTEMPTS \
    SSH_WAIT_INTERVAL_SECONDS TIME_SYNC_ATTEMPTS KAFKA_READY_TIMEOUT_SECONDS \
    EDGE_READY_TIMEOUT_SECONDS RUN_COMPLETION_TIMEOUT_SECONDS POLL_INTERVAL_SECONDS \
    METRICS_INTERVAL_SECONDS; do
    [[ ! -v "$key" ]] || overrides["$key"]="${!key}"
  done
  # Legacy pilot settings in the AWS file still work, but cannot override the
  # dedicated pilot/secrets files. Keep only AWS authentication there going forward.
  for path in "$aws_file" "$pilot_file" "$secrets_file"; do
    if [[ -f "$path" ]]; then
      set -a
      source "$path"
      set +a
      set +x
    fi
  done
  for key in "${!overrides[@]}"; do
    printf -v "$key" '%s' "${overrides[$key]}"
    export "$key"
  done
  for key in SSH_KEY_PATH RESOURCE_PROFILE TERRAFORM_DIR ARTIFACTS_ROOT; do
    if [[ -n "${!key:-}" && "${!key}" != /* ]]; then
      printf -v "$key" '%s/%s' "$root" "${!key}"
      export "$key"
    fi
  done
  export CONTINUUM_ENV_LOADED=1
}

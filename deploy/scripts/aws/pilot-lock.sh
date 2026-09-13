#!/usr/bin/env bash
# WSL/Linux lock shared by wrapper, preparation and direct experiment runs.
# Descriptor 9 stays open through children and artifact export; never delete the
# lock file (unlinking it would allow another process to lock a different inode).
acquire_pilot_lock() {
  local lock_file="$1/.build/aws-pilot.lock"
  command -v flock >/dev/null || { echo 'flock required (run inside WSL/Linux)' >&2; return 1; }
  mkdir -p "$1/.build"
  if [[ "${CONTINUUM_PILOT_LOCK:-}" == "$lock_file" &&
        "$(readlink "/proc/$$/fd/9" 2>/dev/null)" == "$lock_file" ]]; then
    flock -n 9
    return
  fi
  exec 9>>"$lock_file"
  flock -n 9 || { echo 'Another pilot operation is running from this checkout.' >&2; return 1; }
  export CONTINUUM_PILOT_LOCK="$lock_file"
}

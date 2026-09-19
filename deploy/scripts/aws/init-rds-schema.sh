#!/usr/bin/env bash
# Eseguire su cloud-core prima della prima run.
set -Eeuo pipefail
set +x

readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"
cd "${ROOT}"

compose=(docker compose --env-file .env -f deploy/compose/distributed/cloud-core.generated.yml)

if [[ -n "$("${compose[@]}" ps --status running -q global-aggregator)" ]]; then
  echo 'Stop the Global Aggregator before installing the schema.' >&2
  exit 1
fi

# Le credenziali restano nell'ambiente del container.
"${compose[@]}" run --rm --no-deps -T --entrypoint /bin/sh global-aggregator -eu -c '
  [ "$GLOBAL_SINK_TYPE" = postgres ] || { echo "PostgreSQL sink is not enabled" >&2; exit 1; }
  export PGHOST="$GLOBAL_POSTGRES_HOST" PGPORT="$GLOBAL_POSTGRES_PORT"
  export PGDATABASE="$GLOBAL_POSTGRES_DATABASE" PGUSER="$GLOBAL_POSTGRES_USER"
  export PGPASSWORD="$GLOBAL_POSTGRES_PASSWORD" PGSSLMODE="$GLOBAL_POSTGRES_SSLMODE"
  export PGCONNECT_TIMEOUT=10

  exists=$(psql -X --no-password -At -v ON_ERROR_STOP=1 -c "SELECT to_regclass('\''public.global_aggregates'\'') IS NOT NULL")
  if [ "$exists" = t ]; then
    psql -X --no-password -v ON_ERROR_STOP=1 \
      -c "SELECT expected_partitions, contributing_partitions FROM public.global_aggregates LIMIT 0" >/dev/null
    echo "global_aggregates already exists; data preserved."
  else
    psql -X --no-password -v ON_ERROR_STOP=1 -f /app/global_aggregates.sql
    echo "global_aggregates installed."
  fi
'

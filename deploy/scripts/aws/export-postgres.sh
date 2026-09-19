#!/usr/bin/env bash
# Esporta NDJSON su stdout dopo il completamento del Global.
set -Eeuo pipefail
set +x
cd /opt/continuum/current
docker compose --env-file .env -f deploy/compose/distributed/cloud-core.generated.yml \
  run --rm --no-deps -T --entrypoint /bin/sh global-aggregator -eu -c '
    [ "$GLOBAL_SINK_TYPE" = postgres ] || exit 1
    export PGHOST="$GLOBAL_POSTGRES_HOST" PGPORT="$GLOBAL_POSTGRES_PORT"
    export PGDATABASE="$GLOBAL_POSTGRES_DATABASE" PGUSER="$GLOBAL_POSTGRES_USER"
    export PGPASSWORD="$GLOBAL_POSTGRES_PASSWORD" PGSSLMODE="$GLOBAL_POSTGRES_SSLMODE"
    export PGCONNECT_TIMEOUT=10 PGOPTIONS="-c statement_timeout=60000 -c timezone=UTC"
    exec psql -X --no-password -qAt -v ON_ERROR_STOP=1 \
      -c "SELECT row_to_json(g) FROM public.global_aggregates AS g ORDER BY window_start, aggregate_id"
  '

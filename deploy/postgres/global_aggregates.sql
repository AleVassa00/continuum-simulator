-- Install or upgrade in the current schema. Stop the Global writer first.
-- WARNING: the old Edge-based table and its data are deleted, without backup.
-- An already partition-based table and its data are left intact.
BEGIN;
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    target_schema TEXT := current_schema();
    target_table REGCLASS;
    edge_columns INTEGER;
    partition_columns INTEGER;
BEGIN
    IF target_schema IS NULL THEN
        RAISE EXCEPTION 'No writable target schema in search_path';
    END IF;
    target_table := to_regclass(format('%I.global_aggregates', target_schema));
    IF target_table IS NULL THEN
        RETURN;
    END IF;

    EXECUTE format('LOCK TABLE %I.global_aggregates IN ACCESS EXCLUSIVE MODE', target_schema);
    SELECT count(*) FILTER (WHERE attname IN ('expected_edges', 'contributing_edges')),
           count(*) FILTER (WHERE attname IN ('expected_partitions', 'contributing_partitions'))
    INTO edge_columns, partition_columns
    FROM pg_attribute
    WHERE attrelid = target_table AND attnum > 0 AND NOT attisdropped;

    IF edge_columns = 2 AND partition_columns = 0 THEN
        -- No CASCADE: unrelated dependent objects must never be deleted.
        EXECUTE format('DROP TABLE %I.global_aggregates', target_schema);
        RAISE NOTICE 'Old Edge-based table deleted; creating an empty partition-based table';
    ELSIF edge_columns <> 0 OR partition_columns <> 2 THEN
        RAISE EXCEPTION 'Unrecognized global_aggregates schema; automatic upgrade refused';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS global_aggregates (
    aggregate_id TEXT PRIMARY KEY,

    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,

    expected_partitions     BIGINT NOT NULL,
    contributing_partitions BIGINT NOT NULL,
    events             BIGINT NOT NULL,

    temperature_valid   BIGINT NOT NULL,
    temperature_invalid BIGINT NOT NULL,
    temperature_sum     DOUBLE PRECISION NOT NULL,
    temperature_average DOUBLE PRECISION NULL,
    temperature_min     DOUBLE PRECISION NULL,
    temperature_max     DOUBLE PRECISION NULL,

    humidity_valid   BIGINT NOT NULL,
    humidity_invalid BIGINT NOT NULL,
    humidity_sum     DOUBLE PRECISION NOT NULL,
    humidity_average DOUBLE PRECISION NULL,
    humidity_min     DOUBLE PRECISION NULL,
    humidity_max     DOUBLE PRECISION NULL,

    pressure_valid   BIGINT NOT NULL,
    pressure_invalid BIGINT NOT NULL,
    pressure_sum     DOUBLE PRECISION NOT NULL,
    pressure_average DOUBLE PRECISION NULL,
    pressure_min     DOUBLE PRECISION NULL,
    pressure_max     DOUBLE PRECISION NULL,

    emitted_at TIMESTAMPTZ NOT NULL
);

-- Repeated application leaves the current table and index intact.
CREATE INDEX IF NOT EXISTS idx_global_aggregates_partitions_window_start
ON global_aggregates (window_start);

COMMIT;

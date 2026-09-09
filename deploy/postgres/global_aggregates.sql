CREATE TABLE global_aggregates (
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

CREATE INDEX idx_global_aggregates_window_start
ON global_aggregates (window_start);

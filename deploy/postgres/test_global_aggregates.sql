-- One targeted smoke test. Run ONLY in a disposable database, with psql
-- -v ON_ERROR_STOP=1 -f test_global_aggregates.sql, beside global_aggregates.sql.
CREATE SCHEMA partition_migration_test;
SET search_path = partition_migration_test;

-- Clean install; turn the table into the previous Edge contract as a fixture.
\ir global_aggregates.sql
ALTER TABLE global_aggregates RENAME COLUMN expected_partitions TO expected_edges;
ALTER TABLE global_aggregates RENAME COLUMN contributing_partitions TO contributing_edges;
-- Match the old index name as well.
ALTER INDEX idx_global_aggregates_partitions_window_start RENAME TO idx_global_aggregates_window_start;
INSERT INTO global_aggregates VALUES (
    'same-window-id', '2025-01-01 00:00Z', '2025-01-01 00:15Z', 13, 12, 3,
    3, 0, 21, 7, 7, 7, 3, 0, 21, 7, 7, 7, 3, 0, 21, 7, 7, 7, '2025-01-01 00:16Z'
);

-- Upgrade deletes old data; repeated application must not create an archive.
\ir global_aggregates.sql
\ir global_aggregates.sql
DO $$ BEGIN
    IF (SELECT count(*) FROM global_aggregates) <> 0 THEN
        RAISE EXCEPTION 'Old results leaked into the new table';
    END IF;
    IF to_regclass('partition_migration_test.global_aggregates_edge_legacy') IS NOT NULL THEN
        RAISE EXCEPTION 'Unexpected legacy archive';
    END IF;
END $$;

-- The same natural window ID must not suppress a new partition-based result.
INSERT INTO global_aggregates VALUES (
    'same-window-id', '2025-01-01 00:00Z', '2025-01-01 00:15Z', 6, 6, 3,
    3, 0, 21, 7, 7, 7, 3, 0, 21, 7, 7, 7, 3, 0, 21, 7, 7, 7, '2025-01-01 00:17Z'
);
\ir global_aggregates.sql
DO $$ BEGIN
    IF (SELECT count(*) FROM global_aggregates WHERE expected_partitions = 6) <> 1 THEN
        RAISE EXCEPTION 'Repeated installation changed results';
    END IF;
END $$;
SELECT 'PASS: old table replaced, no archive, new data preserved on rerun' AS result;

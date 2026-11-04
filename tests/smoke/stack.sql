-- End-to-end smoke test of the Lakehouse stack: Trino writes an Iceberg table
-- whose catalog is Nessie and whose files land in SeaweedFS.
-- Each assertion calls fail(), which aborts the script with a non-zero exit code.
-- The table is kept after the run so it can be inspected; it is recreated on each run.

DROP TABLE IF EXISTS iceberg.smoke_test.readings;
DROP SCHEMA IF EXISTS iceberg.smoke_test;

CREATE SCHEMA iceberg.smoke_test;

CREATE TABLE iceberg.smoke_test.readings (
    ts     timestamp(6),
    source varchar,
    mw     double
);

INSERT INTO iceberg.smoke_test.readings VALUES
    (TIMESTAMP '2026-01-01 12:00:00', 'wind',    4200.5),
    (TIMESTAMP '2026-01-01 12:00:00', 'solar',   1830.0),
    (TIMESTAMP '2026-01-01 12:00:00', 'nuclear', 38750.25);

-- Round trip through Trino: the rows read back are the rows written.
SELECT CASE
    WHEN count(*) = 3 AND sum(mw) = 44780.75 THEN 'OK: 3 rows read back'
    ELSE fail(format('unexpected content: %s rows, %s MW', count(*), sum(mw)))
END AS data_check
FROM iceberg.smoke_test.readings;

-- Iceberg metadata: the insert produced a snapshot.
SELECT CASE
    WHEN count(*) >= 1 THEN format('OK: %s snapshot(s)', count(*))
    ELSE fail('no Iceberg snapshot found')
END AS snapshot_check
FROM iceberg.smoke_test."readings$snapshots";

-- Object storage: every data file is stored in the SeaweedFS warehouse bucket.
SELECT CASE
    WHEN count(*) >= 1 AND count_if(file_path NOT LIKE 's3://warehouse/smoke_test/%') = 0
        THEN format('OK: %s data file(s) in s3://warehouse', count(*))
    ELSE fail('data files missing or outside s3://warehouse/smoke_test/')
END AS storage_check
FROM iceberg.smoke_test."readings$files";

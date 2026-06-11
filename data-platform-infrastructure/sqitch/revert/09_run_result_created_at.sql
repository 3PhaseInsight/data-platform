-- Revert 3phi-db:09_run_result_created_at from pg

BEGIN;

DROP INDEX IF EXISTS :meta_schema.run_result_dag_meter_created_idx;

ALTER TABLE :meta_schema.run_result
    DROP COLUMN IF EXISTS created_at;

COMMIT;

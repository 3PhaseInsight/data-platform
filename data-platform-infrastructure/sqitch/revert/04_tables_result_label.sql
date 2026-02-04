-- Revert 3phi-db:04_tables_result_label from pg

BEGIN;

DROP TABLE IF EXISTS :meta_schema.run_result;
DROP TYPE IF EXISTS result_phase;

COMMIT;

-- Revert 3phi-db:00_init from pg
BEGIN;

-- Drop child tables first to satisfy FKs
DROP TABLE IF EXISTS :meta_schema.file_index CASCADE;

DROP TABLE IF EXISTS :meta_schema.meter CASCADE;
DROP TABLE IF EXISTS :meta_schema.ingest_batch CASCADE;
DROP TABLE IF EXISTS :meta_schema.workflow_states CASCADE;

-- Drop helper function
DROP FUNCTION IF EXISTS :meta_schema.set_updated_at();

-- Drop airflow schema
DROP SCHEMA IF EXISTS airflow CASCADE;

COMMIT;

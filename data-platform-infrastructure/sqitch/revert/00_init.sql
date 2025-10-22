-- Revert 3phi-db:00_init from pg
BEGIN;

-- Drop child tables first to satisfy FKs
DROP TABLE IF EXISTS public.file_index CASCADE;

DROP TABLE IF EXISTS public.meter_data CASCADE;
DROP TABLE IF EXISTS public.hourly_measurements CASCADE;
DROP TABLE IF EXISTS public.meter CASCADE;
DROP TABLE IF EXISTS public.ingest_batch CASCADE;
DROP TABLE IF EXISTS public.workflow_states CASCADE;

-- Drop helper function
DROP FUNCTION IF EXISTS public.set_updated_at();

-- Drop airflow schema
DROP SCHEMA IF EXISTS airflow CASCADE;

COMMIT;

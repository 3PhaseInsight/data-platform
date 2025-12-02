-- Deploy 3phi-db:02_tables_intermediate_results to pg

BEGIN;

ALTER TABLE public.meter
ADD COLUMN data_quality jsonb,
ADD COLUMN data_statistics jsonb,
ADD COLUMN connectivity jsonb;


COMMIT;

-- Revert 3phi-db:02_tables_intermediate_results from pg

BEGIN;

ALTER TABLE public.meter
    DROP COLUMN data_quality,
    DROP COLUMN data_statistics,
    DROP COLUMN connectivity;

COMMIT;

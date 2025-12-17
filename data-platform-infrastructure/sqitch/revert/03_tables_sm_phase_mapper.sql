-- Revert 3phi-db:03_tables_sm_phase_mapper from pg

BEGIN;

-- Table
DROP TABLE IF EXISTS public.sm_phase_mapping;

-- enum
DROP TYPE IF EXISTS public.phase_enum;

COMMIT;
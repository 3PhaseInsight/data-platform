-- Revert 3phi-db:03_tables_sm_phase_mapper from pg

BEGIN;

-- Table
DROP TABLE IF EXISTS :meta_schema.sm_phase_mapping;

-- enum
DROP TYPE IF EXISTS :meta_schema.phase;

COMMIT;

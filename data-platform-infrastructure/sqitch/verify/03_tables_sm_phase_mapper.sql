-- Verify 3phi-db:03_tables_sm_phase_mapper on pg

BEGIN;

-- Table existence: :meta_schema.sm_phase_mapping
SELECT 1 / CASE WHEN to_regclass(format('%I.sm_phase_mapping', :'meta_schema')) IS NOT NULL
                THEN 1 ELSE 0 END;

-- Enum existence: :meta_schema.phase
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = :'meta_schema'
      AND t.typname = 'phase'
)
THEN 1 ELSE 0 END;

ROLLBACK;

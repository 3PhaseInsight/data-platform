-- Verify 3phi-db:04_tables_result_label on pg

BEGIN;

-- Table existence: :meta_schema.run_result
SELECT 1 / CASE
    WHEN to_regclass(format('%I.run_result', :'meta_schema')) IS NOT NULL
    THEN 1 ELSE 0
END;

-- Enum existence: :meta_schema.result_phase
SELECT 1 / CASE
    WHEN EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = :'meta_schema'
          AND t.typname = 'result_phase'
    )
    THEN 1 ELSE 0
END;

ROLLBACK;

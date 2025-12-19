-- Verify 3phi-db:03_tables_sm_phase_mapper on pg

BEGIN;

DO $$
BEGIN
    -- Table
    IF to_regclass('public.sm_phase_mapping') IS NULL THEN
        RAISE EXCEPTION 'Expected table %.% does not exist', 'public', 'sm_phase_mapping';
    END IF;

    -- Enum
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'public' AND t.typname = 'phase'
    ) THEN
        RAISE EXCEPTION 'Type public.phase is missing';
    END IF;
END $$;

ROLLBACK;

-- Verify 3phi-db:04_tables_result_label on pg

BEGIN;

DO $$
BEGIN
  -- Table
  IF to_regclass('public.run_result') IS NULL THEN
    RAISE EXCEPTION 'Expected table %.% does not exist', 'public', 'file_index';
  END IF;

  -- Enum
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'public' AND t.typname = 'result_phase'
    ) THEN
        RAISE EXCEPTION 'Type public.result_phase is missing';
    END IF;
END $$;

ROLLBACK;

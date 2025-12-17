-- Verify 3phi-db:04_table_result_label on pg

BEGIN;

DO $$
BEGIN
  IF to_regclass('public.run_result') IS NULL THEN
    RAISE EXCEPTION 'Expected table %.% does not exist', 'public', 'file_index';
  END IF;
END $$;

ROLLBACK;

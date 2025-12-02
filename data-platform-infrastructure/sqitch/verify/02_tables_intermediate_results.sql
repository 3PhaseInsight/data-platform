-- Verify 3phi-db:02_tables_intermediate_results on pg

BEGIN;

    DO $$
    BEGIN

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'meter'
          AND column_name  = 'data_quality'
      ) THEN
        RAISE EXCEPTION 'Column data_quality is missing from public.meter';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'meter'
          AND column_name  = 'data_statistics'
      ) THEN
        RAISE EXCEPTION 'Column data_statistics is missing from public.meter';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'meter'
          AND column_name  = 'connectivity'
      ) THEN
        RAISE EXCEPTION 'Column connectivity is missing from public.meter';
    END IF;

    END $$;

ROLLBACK;

-- Verify 3phi-db:09_run_result_created_at on pg

BEGIN;

-- Column exists, is NOT NULL, and is timestamptz
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name   = 'run_result'
      AND column_name  = 'created_at'
      AND is_nullable  = 'NO'
      AND data_type    = 'timestamp with time zone'
) THEN 1 ELSE 0 END;

-- Default is now()
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema   = :'meta_schema'
      AND table_name     = 'run_result'
      AND column_name    = 'created_at'
      AND column_default LIKE 'now()%'
) THEN 1 ELSE 0 END;

-- Supporting index exists
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_class i
    JOIN pg_namespace n ON n.oid = i.relnamespace
    WHERE n.nspname = :'meta_schema'
      AND i.relname = 'run_result_dag_meter_created_idx'
) THEN 1 ELSE 0 END;

ROLLBACK;

-- Verify 3phi-db:02_tables_intermediate_results on pg

BEGIN;

-- data_quality
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name   = 'meter'
      AND column_name  = 'data_quality'
) THEN 1 ELSE 0 END;

-- data_statistics
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name   = 'meter'
      AND column_name  = 'data_statistics'
) THEN 1 ELSE 0 END;

-- connectivity
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name   = 'meter'
      AND column_name  = 'connectivity'
) THEN 1 ELSE 0 END;

ROLLBACK;

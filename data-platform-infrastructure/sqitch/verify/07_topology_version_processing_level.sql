-- Verify 3phi-db:07_topology_version_processing_level on pg

BEGIN;

SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = :'lv_schema'
      AND table_name   = 'topology_version'
      AND column_name  = 'processing_level'
) THEN 1 ELSE 0 END;

ROLLBACK;

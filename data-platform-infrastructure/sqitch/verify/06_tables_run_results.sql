-- Verify 3phi-db:06_tables_run_results on pg

BEGIN;

-- Columns exist
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name   = 'run_result'
      AND column_name  = 'topology_version'
) THEN 1 ELSE 0 END;

SELECT 1 / CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name   = 'run_result'
      AND column_name  = 'node_id'
) THEN 1 ELSE 0 END;

SELECT 1 / CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name   = 'run_result'
      AND column_name  = 'edge_id'
) THEN 1 ELSE 0 END;

SELECT 1 / CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name   = 'run_result'
      AND column_name  = 'cable_id'
) THEN 1 ELSE 0 END;

-- meter_id is nullable
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name   = 'run_result'
      AND column_name  = 'meter_id'
      AND is_nullable  = 'YES'
) THEN 1 ELSE 0 END;

-- phase is nullable
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name   = 'run_result'
      AND column_name  = 'phase'
      AND is_nullable  = 'YES'
) THEN 1 ELSE 0 END;

-- Check constraints
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = :'meta_schema'
      AND t.relname = 'run_result'
      AND c.contype = 'c'
      AND c.conname = 'run_result_exactly_one_target_chk'
) THEN 1 ELSE 0 END;

SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = :'meta_schema'
      AND t.relname = 'run_result'
      AND c.contype = 'c'
      AND c.conname = 'run_result_version_required_for_graph_chk'
) THEN 1 ELSE 0 END;

-- FKs
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t  ON t.oid  = c.conrelid
    JOIN pg_namespace nt ON nt.oid = t.relnamespace
    JOIN pg_class rt ON rt.oid = c.confrelid
    JOIN pg_namespace nr ON nr.oid = rt.relnamespace
    WHERE nt.nspname = :'meta_schema'
      AND t.relname  = 'run_result'
      AND c.contype  = 'f'
      AND c.conname  = 'run_result_node_fk'
      AND nr.nspname = :'lv_schema'
      AND rt.relname = 'node'
) THEN 1 ELSE 0 END;

SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t  ON t.oid  = c.conrelid
    JOIN pg_namespace nt ON nt.oid = t.relnamespace
    JOIN pg_class rt ON rt.oid = c.confrelid
    JOIN pg_namespace nr ON nr.oid = rt.relnamespace
    WHERE nt.nspname = :'meta_schema'
      AND t.relname  = 'run_result'
      AND c.contype  = 'f'
      AND c.conname  = 'run_result_edge_fk'
      AND nr.nspname = :'lv_schema'
      AND rt.relname = 'edge'
) THEN 1 ELSE 0 END;

SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t  ON t.oid  = c.conrelid
    JOIN pg_namespace nt ON nt.oid = t.relnamespace
    JOIN pg_class rt ON rt.oid = c.confrelid
    JOIN pg_namespace nr ON nr.oid = rt.relnamespace
    WHERE nt.nspname = :'meta_schema'
      AND t.relname  = 'run_result'
      AND c.contype  = 'f'
      AND c.conname  = 'run_result_cable_fk'
      AND nr.nspname = :'lv_schema'
      AND rt.relname = 'cable'
) THEN 1 ELSE 0 END;

-- Indexes exist
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_class i
    JOIN pg_namespace n ON n.oid = i.relnamespace
    WHERE n.nspname = :'meta_schema'
      AND i.relname = 'run_result_node_idx'
) THEN 1 ELSE 0 END;

SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_class i
    JOIN pg_namespace n ON n.oid = i.relnamespace
    WHERE n.nspname = :'meta_schema'
      AND i.relname = 'run_result_edge_idx'
) THEN 1 ELSE 0 END;

SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_class i
    JOIN pg_namespace n ON n.oid = i.relnamespace
    WHERE n.nspname = :'meta_schema'
      AND i.relname = 'run_result_cable_idx'
) THEN 1 ELSE 0 END;

SELECT 1 / CASE WHEN EXISTS (
    SELECT 1
    FROM pg_class i
    JOIN pg_namespace n ON n.oid = i.relnamespace
    WHERE n.nspname = :'meta_schema'
      AND i.relname = 'run_result_meter_idx'
) THEN 1 ELSE 0 END;

COMMIT;

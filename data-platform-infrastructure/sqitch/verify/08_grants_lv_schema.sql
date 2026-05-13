-- Verify 3phi-db:08_grants_lv_schema on pg

BEGIN;

-- Schema-level USAGE
SELECT 1 / CASE
    WHEN has_schema_privilege('threephi_db_user', :'lv_schema', 'USAGE') THEN 1 ELSE 0
END;

-- Per-table privilege check on a representative versioned + unversioned table
SELECT 1 / CASE
    WHEN has_table_privilege('threephi_db_user', :'lv_schema' || '.topology_version', 'INSERT') THEN 1 ELSE 0
END;
SELECT 1 / CASE
    WHEN has_table_privilege('threephi_db_user', :'lv_schema' || '.node', 'INSERT') THEN 1 ELSE 0
END;
SELECT 1 / CASE
    WHEN has_table_privilege('threephi_db_user', :'lv_schema' || '.meter', 'SELECT') THEN 1 ELSE 0
END;

-- ENUM USAGE
SELECT 1 / CASE
    WHEN has_type_privilege('threephi_db_user', :'lv_schema' || '.node_type', 'USAGE') THEN 1 ELSE 0
END;

ROLLBACK;

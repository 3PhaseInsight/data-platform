-- Verify 3phi-db:05b_tables_topology_delivery_point_nodes on pg

BEGIN;

-- 1) delivery_point.cabinet_id must be nullable
SELECT 1 / CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :'lv_schema'
          AND table_name   = 'delivery_point'
          AND column_name  = 'cabinet_id'
          AND is_nullable  = 'NO'
    )
    THEN 1 ELSE 0
END;

-- 2) :lv_schema.node.delivery_point_id column must exist
SELECT 1 / CASE
    WHEN EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :'lv_schema'
          AND table_name   = 'node'
          AND column_name  = 'delivery_point_id'
    )
    THEN 1 ELSE 0
END;

-- 3) Partial unique index must exist: :lv_schema.lv_node_delivery_point_uniq
SELECT 1 / CASE
    WHEN EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :'lv_schema'
          AND c.relname = 'lv_node_delivery_point_uniq'
          AND c.relkind = 'i'
    )
    THEN 1 ELSE 0
END;

-- 4) Performance index must exist: :lv_schema.lv_node_delivery_point_fk_idx
SELECT 1 / CASE
    WHEN EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :'lv_schema'
          AND c.relname = 'lv_node_delivery_point_fk_idx'
          AND c.relkind = 'i'
    )
    THEN 1 ELSE 0
END;

-- 5) CHECK constraint must exist and reference delivery_point_id and DeliveryPoint
WITH chk AS (
    SELECT pg_get_constraintdef(con.oid) AS defn
    FROM pg_constraint con
    JOIN pg_class rel    ON rel.oid = con.conrelid
    JOIN pg_namespace ns ON ns.oid  = rel.relnamespace
    WHERE ns.nspname = :'lv_schema'
      AND rel.relname = 'node'
      AND con.conname = 'lv_node_exactly_one_id_chk'
      AND con.contype = 'c'
)
SELECT 1 / CASE
    WHEN EXISTS (
        SELECT 1
        FROM chk
        WHERE defn IS NOT NULL
          AND position('delivery_point_id' IN defn) > 0
          AND position('DeliveryPoint'   IN defn) > 0
    )
    THEN 1 ELSE 0
END;

-- 6) Updated view exists
SELECT 1 / CASE
    WHEN EXISTS (
      SELECT 1 
      FROM information_schema.views 
      WHERE table_schema=:'lv_schema' 
      AND table_name='node_current'
    )
    THEN 1 ELSE 0
END;

ROLLBACK;

-- Revert 3phi-db:05b_tables_topology_delivery_point_nodes from pg

BEGIN;

-- 1) Drop performance index
DROP INDEX IF EXISTS :lv_schema.lv_node_delivery_point_fk_idx;

-- 2) Drop uniqueness index
DROP INDEX IF EXISTS :lv_schema.lv_node_delivery_point_uniq;

-- 3) Restore the original CHECK constraint on :lv_schema.node
ALTER TABLE :lv_schema.node
  DROP CONSTRAINT IF EXISTS lv_node_exactly_one_id_chk;

ALTER TABLE :lv_schema.node
  ADD CONSTRAINT lv_node_exactly_one_id_chk CHECK (
    (node_type = 'LvFeeder' AND feeder_id IS NOT NULL AND cabinet_id IS NULL) OR
    (node_type = 'Cabinet'  AND cabinet_id IS NOT NULL AND feeder_id IS NULL)
  );

-- 4) Drop the delivery_point_id column (drops its FK automatically)
ALTER TABLE :lv_schema.node
  DROP COLUMN IF EXISTS delivery_point_id;

-- 5) Restore NOT NULL requirement on cabinet_id
-- This will fail if any delivery_point rows have cabinet_id IS NULL.
ALTER TABLE :lv_schema.delivery_point
  ALTER COLUMN cabinet_id SET NOT NULL;

-- 6) Restore view with delivery_point_id column
CREATE OR REPLACE VIEW :lv_schema.node_current AS
SELECT n.*
FROM :lv_schema.node n
WHERE n.version = (
    SELECT version
    FROM :lv_schema.topology_version
    WHERE is_current
);


COMMIT;

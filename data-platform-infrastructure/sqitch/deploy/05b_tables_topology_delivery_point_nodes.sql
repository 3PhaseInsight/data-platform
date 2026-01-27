-- Deploy 3phi-db:05b_tables_topology_delivery_point_nodes to pg

BEGIN;

-- 1) Allow delivery points without cabinets
ALTER TABLE lv.delivery_point
  ALTER COLUMN cabinet_id DROP NOT NULL;

-- 2) Add the new FK column
ALTER TABLE lv.node
  ADD COLUMN delivery_point_id BIGINT
  REFERENCES lv.delivery_point(id) ON DELETE CASCADE;

-- 3) Uniqueness per version for DP nodes (partial is best)
CREATE UNIQUE INDEX IF NOT EXISTS lv_node_delivery_point_uniq
  ON lv.node (version, delivery_point_id)
  WHERE delivery_point_id IS NOT NULL;

-- 4) Define the “exactly one” check
ALTER TABLE lv.node
  DROP CONSTRAINT lv_node_exactly_one_id_chk;

ALTER TABLE lv.node
  ADD CONSTRAINT lv_node_exactly_one_id_chk CHECK (
    (node_type = 'LvFeeder'      AND feeder_id IS NOT NULL AND cabinet_id IS NULL AND delivery_point_id IS NULL) OR
    (node_type = 'Cabinet'       AND cabinet_id IS NOT NULL AND feeder_id IS NULL AND delivery_point_id IS NULL) OR
    (node_type = 'DeliveryPoint' AND delivery_point_id IS NOT NULL AND feeder_id IS NULL AND cabinet_id IS NULL)
  );

-- 5) Performance index (common joins)
CREATE INDEX IF NOT EXISTS lv_node_delivery_point_fk_idx
  ON lv.node (version, delivery_point_id);

COMMIT;

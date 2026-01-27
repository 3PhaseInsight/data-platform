-- Verify 3phi-db:05b_tables_topology_delivery_point_nodes on pg

BEGIN;

-- 1) delivery_point.cabinet_id must be nullable
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'lv'
      AND table_name   = 'delivery_point'
      AND column_name  = 'cabinet_id'
      AND is_nullable  = 'NO'
  ) THEN
    RAISE EXCEPTION 'Expected lv.delivery_point.cabinet_id to be nullable';
  END IF;
END $$;

-- 2) lv.node.delivery_point_id column must exist
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'lv'
      AND table_name   = 'node'
      AND column_name  = 'delivery_point_id'
  ) THEN
    RAISE EXCEPTION 'Expected lv.node.delivery_point_id column to exist';
  END IF;
END $$;

-- 3) Partial unique index must exist: lv_node_delivery_point_uniq
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'lv'
      AND c.relname = 'lv_node_delivery_point_uniq'
      AND c.relkind = 'i'
  ) THEN
    RAISE EXCEPTION 'Expected index lv.lv_node_delivery_point_uniq to exist';
  END IF;
END $$;

-- 4) Performance index must exist: lv_node_delivery_point_fk_idx
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'lv'
      AND c.relname = 'lv_node_delivery_point_fk_idx'
      AND c.relkind = 'i'
  ) THEN
    RAISE EXCEPTION 'Expected index lv.lv_node_delivery_point_fk_idx to exist';
  END IF;
END $$;

-- 5) CHECK constraint must exist and reference delivery_point_id and DeliveryPoint
DO $$
DECLARE
  defn text;
BEGIN
  SELECT pg_get_constraintdef(con.oid)
    INTO defn
  FROM pg_constraint con
  JOIN pg_class rel      ON rel.oid = con.conrelid
  JOIN pg_namespace ns   ON ns.oid  = rel.relnamespace
  WHERE ns.nspname = 'lv'
    AND rel.relname = 'node'
    AND con.conname = 'lv_node_exactly_one_id_chk'
    AND con.contype = 'c';

  IF defn IS NULL THEN
    RAISE EXCEPTION 'Expected CHECK constraint lv.lv_node_exactly_one_id_chk to exist on lv.node';
  END IF;

  IF position('delivery_point_id' IN defn) = 0 OR position('DeliveryPoint' IN defn) = 0 THEN
    RAISE EXCEPTION 'CHECK constraint lv.lv_node_exactly_one_id_chk does not appear to include DeliveryPoint logic. Definition: %', defn;
  END IF;
END $$;


ROLLBACK;

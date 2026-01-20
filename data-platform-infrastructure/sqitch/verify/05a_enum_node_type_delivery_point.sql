-- Verify 3phi-db:05a_tables_topology_delivery_point_nodes on pg

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_enum e ON e.enumtypid = t.oid
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'lv'
      AND t.typname = 'node_type'
      AND e.enumlabel = 'DeliveryPoint'
  ) THEN
    RAISE EXCEPTION 'Expected enum label lv.node_type to include DeliveryPoint';
  END IF;
END $$;

COMMIT;

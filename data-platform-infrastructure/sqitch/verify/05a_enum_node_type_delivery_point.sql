-- Verify 3phi-db:05a_tables_topology_delivery_point_nodes on pg

BEGIN;

SELECT 1 / CASE
    WHEN EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_enum e ON e.enumtypid = t.oid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = :'lv_schema'
          AND t.typname = 'node_type'
          AND e.enumlabel = 'DeliveryPoint'
    )
    THEN 1 ELSE 0
END;

COMMIT;

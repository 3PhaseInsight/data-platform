BEGIN;

-- Schema exists
SELECT 1 FROM pg_namespace WHERE nspname = 'lv';

-- Enum exists
SELECT 1
FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = 'lv' AND t.typname = 'node_type';

-- Core hierarchy tables
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='secondary_substation';
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='transformer';
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='feeder';
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='cabinet';
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='delivery_point';
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='meter';

-- Versioning + topology tables
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='topology_version';
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='node';
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='edge';
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='cable';
SELECT 1 FROM information_schema.tables WHERE table_schema='lv' AND table_name='edge_cable';

-- Key columns present
SELECT 1 FROM information_schema.columns WHERE table_schema='lv' AND table_name='node' AND column_name='version';
SELECT 1 FROM information_schema.columns WHERE table_schema='lv' AND table_name='edge' AND column_name='id';
SELECT 1 FROM information_schema.columns WHERE table_schema='lv' AND table_name='cable'   AND column_name='cable_id';
SELECT 1 FROM information_schema.columns WHERE table_schema='lv' AND table_name='edge_cable' AND column_name='seq_no';

-- Views exist
SELECT 1 FROM information_schema.views WHERE table_schema='lv' AND table_name='node_current';
SELECT 1 FROM information_schema.views WHERE table_schema='lv' AND table_name='edge_current';
SELECT 1 FROM information_schema.views WHERE table_schema='lv' AND table_name='edge_current_with_totals';

COMMIT;

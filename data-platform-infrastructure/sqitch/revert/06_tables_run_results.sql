-- Revert 3phi-db:06_tables_run_results from pg

BEGIN;

-- 1) Drop indexes added by deploy
DROP INDEX IF EXISTS public.run_result_node_idx;
DROP INDEX IF EXISTS public.run_result_edge_idx;
DROP INDEX IF EXISTS public.run_result_cable_idx;
DROP INDEX IF EXISTS public.run_result_meter_idx;

-- 2) Drop check constraints added by deploy
ALTER TABLE public.run_result
  DROP CONSTRAINT IF EXISTS run_result_version_required_for_graph_chk;

ALTER TABLE public.run_result
  DROP CONSTRAINT IF EXISTS run_result_exactly_one_target_chk;

-- 3) Drop composite foreign keys added by deploy
ALTER TABLE public.run_result
  DROP CONSTRAINT IF EXISTS run_result_node_fk;

ALTER TABLE public.run_result
  DROP CONSTRAINT IF EXISTS run_result_edge_fk;

ALTER TABLE public.run_result
  DROP CONSTRAINT IF EXISTS run_result_cable_fk;

-- 4) Drop columns added by deploy
ALTER TABLE public.run_result
  DROP COLUMN IF EXISTS node_id,
  DROP COLUMN IF EXISTS edge_id,
  DROP COLUMN IF EXISTS cable_id,
  DROP COLUMN IF EXISTS topology_version;

-- 5) Restore NOT NULL constraints (only safe if there are no rows that would become invalid)
-- if there are conflicts when reverting, resolve manually
ALTER TABLE public.run_result
  ALTER COLUMN meter_id SET NOT NULL;

ALTER TABLE public.run_result
  ALTER COLUMN phase SET NOT NULL;

COMMIT;

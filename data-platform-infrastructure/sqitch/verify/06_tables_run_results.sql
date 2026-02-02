-- Verify 3phi-db:06_tables_run_results on pg

BEGIN;

DO $$
BEGIN
  -- Columns exist
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='run_result' AND column_name='topology_version'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing column public.run_result.topology_version';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='run_result' AND column_name='node_id'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing column public.run_result.node_id';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='run_result' AND column_name='edge_id'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing column public.run_result.edge_id';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='run_result' AND column_name='cable_id'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing column public.run_result.cable_id';
  END IF;

  -- meter_id is nullable
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name='run_result'
      AND column_name='meter_id'
      AND is_nullable='YES'
  ) THEN
    RAISE EXCEPTION 'verify failed: public.run_result.meter_id is still NOT NULL';
  END IF;

    -- phase is nullable
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name='run_result'
      AND column_name='phase'
      AND is_nullable='YES'
  ) THEN
    RAISE EXCEPTION 'verify failed: public.run_result.meter_id is still NOT NULL';
  END IF;

  -- Check constraints
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname='public'
      AND t.relname='run_result'
      AND c.contype='c'
      AND c.conname='run_result_exactly_one_target_chk'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing constraint run_result_exactly_one_target_chk';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname='public'
      AND t.relname='run_result'
      AND c.contype='c'
      AND c.conname='run_result_version_required_for_graph_chk'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing constraint run_result_version_required_for_graph_chk';
  END IF;

  -- FKs (ensure they exist; referenced table checks included)
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace nt ON nt.oid = t.relnamespace
    JOIN pg_class rt ON rt.oid = c.confrelid
    JOIN pg_namespace nr ON nr.oid = rt.relnamespace
    WHERE nt.nspname='public' AND t.relname='run_result'
      AND c.contype='f' AND c.conname='run_result_node_fk'
      AND nr.nspname='lv' AND rt.relname='node'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing FK run_result_node_fk to lv.node';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace nt ON nt.oid = t.relnamespace
    JOIN pg_class rt ON rt.oid = c.confrelid
    JOIN pg_namespace nr ON nr.oid = rt.relnamespace
    WHERE nt.nspname='public' AND t.relname='run_result'
      AND c.contype='f' AND c.conname='run_result_edge_fk'
      AND nr.nspname='lv' AND rt.relname='edge'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing FK run_result_edge_fk to lv.edge';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace nt ON nt.oid = t.relnamespace
    JOIN pg_class rt ON rt.oid = c.confrelid
    JOIN pg_namespace nr ON nr.oid = rt.relnamespace
    WHERE nt.nspname='public' AND t.relname='run_result'
      AND c.contype='f' AND c.conname='run_result_cable_fk'
      AND nr.nspname='lv' AND rt.relname='cable'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing FK run_result_cable_fk to lv.cable';
  END IF;

  -- Indexes exist
  IF NOT EXISTS (
    SELECT 1 FROM pg_class i
    JOIN pg_namespace n ON n.oid=i.relnamespace
    WHERE n.nspname='public' AND i.relname='run_result_node_idx'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing index public.run_result_node_idx';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class i
    JOIN pg_namespace n ON n.oid=i.relnamespace
    WHERE n.nspname='public' AND i.relname='run_result_edge_idx'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing index public.run_result_edge_idx';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class i
    JOIN pg_namespace n ON n.oid=i.relnamespace
    WHERE n.nspname='public' AND i.relname='run_result_cable_idx'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing index public.run_result_cable_idx';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class i
    JOIN pg_namespace n ON n.oid=i.relnamespace
    WHERE n.nspname='public' AND i.relname='run_result_meter_idx'
  ) THEN
    RAISE EXCEPTION 'verify failed: missing index public.run_result_meter_idx';
  END IF;

END$$;

COMMIT;

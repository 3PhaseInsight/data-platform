-- Verify 3phi-db:03_tables_sm_phase_mapper on pg

BEGIN;

DO $$
BEGIN
    -- Table
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'sm_phase_mapping'
    ) THEN
        RAISE EXCEPTION 'Table public.sm_phase_mapping is missing';
    END IF;

    -- Columns
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sm_phase_mapping' AND column_name = 'meter_id'
    ) THEN RAISE EXCEPTION 'Column meter_id is missing from public.sm_phase_mapping'; END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sm_phase_mapping' AND column_name = 'sm_phase'
    ) THEN RAISE EXCEPTION 'Column sm_phase is missing from public.sm_phase_mapping'; END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sm_phase_mapping' AND column_name = 'feeder_phase'
    ) THEN RAISE EXCEPTION 'Column feeder_phase is missing from public.sm_phase_mapping'; END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sm_phase_mapping' AND column_name = 'trafo_phase'
    ) THEN RAISE EXCEPTION 'Column trafo_phase is missing from public.sm_phase_mapping'; END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sm_phase_mapping' AND column_name = 'true_feeder_id'
    ) THEN RAISE EXCEPTION 'Column true_feeder_id is missing from public.sm_phase_mapping'; END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sm_phase_mapping' AND column_name = 'true_trafo_id'
    ) THEN RAISE EXCEPTION 'Column true_trafo_id is missing from public.sm_phase_mapping'; END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sm_phase_mapping' AND column_name = 'likely_cabinet_id'
    ) THEN RAISE EXCEPTION 'Column likely_cabinet_id is missing from public.sm_phase_mapping'; END IF;

    -- Enum
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'public' AND t.typname = 'phase_enum'
    ) THEN
        RAISE EXCEPTION 'Type public.phase_enum is missing';
    END IF;
END $$;

ROLLBACK;

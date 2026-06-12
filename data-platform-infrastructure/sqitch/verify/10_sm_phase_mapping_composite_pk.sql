-- Verify 3phi-db:10_sm_phase_mapping_composite_pk on pg

BEGIN;

-- Primary key is exactly (meter_id, sm_phase)
SELECT 1 / CASE WHEN (
    SELECT array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position)
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON kcu.constraint_name = tc.constraint_name
     AND kcu.table_schema = tc.table_schema
    WHERE tc.table_schema = :'meta_schema'
      AND tc.table_name = 'sm_phase_mapping'
      AND tc.constraint_type = 'PRIMARY KEY'
) = ARRAY['meter_id', 'sm_phase'] THEN 1 ELSE 0 END;

-- sm_phase is NOT NULL
SELECT 1 / CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = :'meta_schema'
      AND table_name = 'sm_phase_mapping'
      AND column_name = 'sm_phase'
      AND is_nullable = 'NO'
) THEN 1 ELSE 0 END;

ROLLBACK;

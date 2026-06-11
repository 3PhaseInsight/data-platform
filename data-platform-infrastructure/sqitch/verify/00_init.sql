-- Verify 3phi-db:00_init on pg
BEGIN;

-- Schema exists
SELECT 1 FROM pg_namespace WHERE nspname = :'meta_schema';
SELECT 1 FROM pg_namespace WHERE nspname = 'airflow';

-- Function exists
SELECT 1
FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = :'meta_schema' AND p.proname = 'set_updated_at';

-- Tables exist
SELECT 1 FROM information_schema.tables WHERE table_schema=:'meta_schema' AND table_name='file_index';
SELECT 1 FROM information_schema.tables WHERE table_schema=:'meta_schema' AND table_name='ingest_batch';
SELECT 1 FROM information_schema.tables WHERE table_schema=:'meta_schema' AND table_name='meter';
SELECT 1 FROM information_schema.tables WHERE table_schema=:'meta_schema' AND table_name='workflow_states';

-- Columns of interest exist
SELECT 1 FROM information_schema.columns WHERE table_schema=:'meta_schema' AND table_name='file_index' AND column_name='batch_id';

-- Trigger on workflow_states (from set_updated_at)
SELECT 1
FROM pg_trigger tg
         JOIN pg_class t  ON t.oid = tg.tgrelid
         JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname=:'meta_schema'
  AND t.relname='workflow_states'
  AND tg.tgname='update_workflow_states_timestamp';

COMMIT;

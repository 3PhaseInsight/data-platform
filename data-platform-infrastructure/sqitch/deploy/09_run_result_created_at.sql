-- Deploy 3phi-db:09_run_result_created_at to pg

BEGIN;

ALTER TABLE :meta_schema.run_result
    ADD COLUMN created_at timestamptz NOT NULL DEFAULT now();

-- Supports the (dag_id, meter_id, latest) lookup used by the API layer.
CREATE INDEX IF NOT EXISTS run_result_dag_meter_created_idx
    ON :meta_schema.run_result (dag_id, meter_id, created_at DESC)
    WHERE meter_id IS NOT NULL;

COMMIT;

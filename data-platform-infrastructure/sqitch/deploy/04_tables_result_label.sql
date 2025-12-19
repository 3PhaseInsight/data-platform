-- Deploy 3phi-db:04_tables_result_label to pg

BEGIN;

CREATE TYPE result_phase AS ENUM (
    'L1', 'L2', 'L3',
    'L1,L2', 'L1,L3', 'L2,L3',
    'all'
    );

CREATE TABLE IF NOT EXISTS public.run_result (
    id              uuid PRIMARY KEY,
    dag_id          text NOT NULL, -- part 1 of unique dag run identifier
    run_id          text NOT NULL, -- part 2 of unique dag run identifier
    meter_id        bigint NOT NULL REFERENCES public.meter(id),
    phase           result_phase NOT NULL,
    label_type      text NOT NULL,
    label_value     text NOT NULL,
    confidence      float NOT NULL,
    source          text NOT NULL, -- could be algorithm, data app, ...
    result          jsonb -- optional JSON blob to store more data
);

COMMIT;

-- Deploy 3phi-db:04_tables_result_label to pg

BEGIN;

CREATE TYPE :meta_schema.result_phase AS ENUM (
    'L1', 'L2', 'L3',
    'L1,L2', 'L1,L3', 'L2,L3',
    'all'
    );

CREATE TABLE IF NOT EXISTS :meta_schema.run_result (
    id              uuid PRIMARY KEY,
    dag_id          text, -- part 1 of unique dag run identifier, nullable for when not using airflow
    run_id          text, -- part 2 of unique dag run identifier, nullable for when not using airflow
    meter_id        bigint NOT NULL REFERENCES :meta_schema.meter(id),
    phase           result_phase NOT NULL,
    label_type      text NOT NULL,
    label_value     text NOT NULL,
    confidence      float NOT NULL,
    source          text NOT NULL, -- could be algorithm, data app, ...
    result          jsonb -- optional JSON blob to store more data
);

COMMIT;

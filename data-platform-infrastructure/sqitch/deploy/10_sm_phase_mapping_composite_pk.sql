-- Deploy 3phi-db:10_sm_phase_mapping_composite_pk to pg

-- The SM phase mapper stores one row per (meter_id, sm_phase): up to three rows
-- per meter, upserted with ON CONFLICT (meter_id, sm_phase). Migration 03 created
-- the table with meter_id alone as PRIMARY KEY, which both rejects a meter's
-- second phase row and breaks the ON CONFLICT target. Replace it with a
-- composite primary key.

BEGIN;

-- sm_phase must be NOT NULL to participate in the primary key; the phase mapper
-- never writes NULL phases, so any existing NULL rows are unusable artifacts.
DELETE FROM :meta_schema.sm_phase_mapping WHERE sm_phase IS NULL;

ALTER TABLE :meta_schema.sm_phase_mapping
    DROP CONSTRAINT sm_phase_mapping_pkey;

ALTER TABLE :meta_schema.sm_phase_mapping
    ALTER COLUMN sm_phase SET NOT NULL;

ALTER TABLE :meta_schema.sm_phase_mapping
    ADD PRIMARY KEY (meter_id, sm_phase);

COMMIT;

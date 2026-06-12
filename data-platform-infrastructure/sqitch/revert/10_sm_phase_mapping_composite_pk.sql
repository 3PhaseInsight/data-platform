-- Revert 3phi-db:10_sm_phase_mapping_composite_pk from pg

BEGIN;

ALTER TABLE :meta_schema.sm_phase_mapping
    DROP CONSTRAINT sm_phase_mapping_pkey;

-- Restoring the single-column PK requires one row per meter: keep the row with
-- the lowest sm_phase per meter.
DELETE FROM :meta_schema.sm_phase_mapping a
    USING :meta_schema.sm_phase_mapping b
    WHERE a.meter_id = b.meter_id
      AND a.sm_phase > b.sm_phase;

ALTER TABLE :meta_schema.sm_phase_mapping
    ALTER COLUMN sm_phase DROP NOT NULL;

ALTER TABLE :meta_schema.sm_phase_mapping
    ADD PRIMARY KEY (meter_id);

COMMIT;

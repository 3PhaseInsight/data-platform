
BEGIN;

-- Enum (phases)
CREATE TYPE :meta_schema.phase AS ENUM ('L1', 'L2', 'L3');

-- =========================
-- SM phase mapping results
-- =========================
CREATE TABLE IF NOT EXISTS :meta_schema.sm_phase_mapping (
    meter_id          bigint PRIMARY KEY REFERENCES :meta_schema.meter(id) ON DELETE CASCADE,
    sm_phase          :meta_schema.phase,
    feeder_phase      :meta_schema.phase,
    trafo_phase       :meta_schema.phase,
    true_feeder_id    bigint REFERENCES lv.feeder(id),
    true_trafo_id     bigint REFERENCES lv.transformer(id),
    likely_cabinet_id bigint REFERENCES lv.cabinet(id)
);

-- Indexes to speed lookups by topology
CREATE INDEX IF NOT EXISTS sm_phase_mapping_true_feeder_idx ON :meta_schema.sm_phase_mapping (true_feeder_id);
CREATE INDEX IF NOT EXISTS sm_phase_mapping_true_trafo_idx  ON :meta_schema.sm_phase_mapping (true_trafo_id);
CREATE INDEX IF NOT EXISTS sm_phase_mapping_likely_cab_idx  ON :meta_schema.sm_phase_mapping (likely_cabinet_id);

-- Grants
GRANT SELECT, INSERT, UPDATE, DELETE ON :meta_schema.sm_phase_mapping TO threephi_db_user;

COMMIT;

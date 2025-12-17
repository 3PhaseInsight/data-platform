
BEGIN;

-- Enum (phases)
CREATE TYPE IF NOT EXISTS public.phase_enum AS ENUM ('L1', 'L2', 'L3');

-- =========================
-- SM phase mapping results
-- =========================
CREATE TABLE IF NOT EXISTS public.sm_phase_mapping (
    meter_id          text PRIMARY KEY REFERENCES public.meter(id) ON DELETE CASCADE,
    sm_phase          public.phase_enum,
    feeder_phase      public.phase_enum,
    trafo_phase       public.phase_enum,
    true_feeder_id    bigint REFERENCES lv.feeder(id),
    true_trafo_id     bigint REFERENCES lv.transformer(id),
    likely_cabinet_id bigint REFERENCES lv.cabinet(id)
);

-- Indexes to speed lookups by topology
CREATE INDEX IF NOT EXISTS sm_phase_mapping_true_feeder_idx ON public.sm_phase_mapping (true_feeder_id);
CREATE INDEX IF NOT EXISTS sm_phase_mapping_true_trafo_idx  ON public.sm_phase_mapping (true_trafo_id);
CREATE INDEX IF NOT EXISTS sm_phase_mapping_likely_cab_idx  ON public.sm_phase_mapping (likely_cabinet_id);

-- -- Grants
-- GRANT SELECT, INSERT, UPDATE, DELETE ON public.sm_phase_mapping TO threephi_db_user;

COMMIT;

-- Deploy 3phi-db:03_tables_sm_phase_mapper to pg

BEGIN;

CREATE TYPE IF NOT EXISTS public.phase_enum AS ENUM ('L1','L2','L3');

CREATE TABLE public.sm_phase_mapping (
  meter_id text PRIMARY KEY REFERENCES public.meter(id) ON DELETE CASCADE,
  sm_phase public.phase_enum, feeder_phase public.phase_enum, trafo_phase public.phase_enum,
  true_feeder_id bigint REFERENCES lv.feeder(id), true_trafo_id bigint REFERENCES lv.transformer(id),
  likely_cabinet_id bigint REFERENCES lv.cabinet(id),
  created_at timestamptz DEFAULT now(), updated_at timestamptz DEFAULT now()
);

CREATE INDEX ON public.sm_phase_mapping(true_feeder_id); 
CREATE INDEX ON public.sm_phase_mapping(true_trafo_id); 
CREATE INDEX ON public.sm_phase_mapping(likely_cabinet_id);

GRANT SELECT,INSERT,UPDATE,DELETE ON public.sm_phase_mapping TO threephi_db_user;

COMMIT;


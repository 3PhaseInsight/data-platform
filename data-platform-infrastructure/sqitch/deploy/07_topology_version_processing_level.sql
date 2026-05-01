-- Deploy 3phi-db:07_topology_version_processing_level to pg

BEGIN;

ALTER TABLE :lv_schema.topology_version
    ADD COLUMN processing_level VARCHAR(32) NOT NULL DEFAULT 'raw';

COMMIT;

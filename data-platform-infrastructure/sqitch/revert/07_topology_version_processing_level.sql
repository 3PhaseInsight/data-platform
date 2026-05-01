-- Revert 3phi-db:07_topology_version_processing_level from pg

BEGIN;

ALTER TABLE :lv_schema.topology_version
    DROP COLUMN processing_level;

COMMIT;

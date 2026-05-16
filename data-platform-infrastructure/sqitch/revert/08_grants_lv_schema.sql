-- Revert 3phi-db:08_grants_lv_schema from pg

BEGIN;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA :lv_schema
    REVOKE USAGE, SELECT ON SEQUENCES FROM threephi_db_user;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA :lv_schema
    REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM threephi_db_user;

REVOKE USAGE ON TYPE :lv_schema.node_type FROM threephi_db_user;
REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA :lv_schema FROM threephi_db_user;
REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA :lv_schema FROM threephi_db_user;

REVOKE ALL ON SCHEMA :lv_schema FROM threephi_db_user;

COMMIT;

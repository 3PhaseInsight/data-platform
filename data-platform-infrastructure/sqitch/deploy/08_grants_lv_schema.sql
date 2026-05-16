-- Deploy 3phi-db:08_grants_lv_schema to pg

BEGIN;

-- Mirror the GRANTs that 00_init applies to the meta schema. The lv schema
-- was created by 01_schema_lv_topology without any explicit grants to the
-- application role, leaving threephi_db_user without access to topology
-- tables unless the runtime was using a superuser role.
GRANT ALL ON SCHEMA :lv_schema TO threephi_db_user;

-- Existing tables (including views) and sequences.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA :lv_schema TO threephi_db_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA :lv_schema TO threephi_db_user;

-- USAGE on user-defined enum types is required to INSERT/SELECT values of that type.
GRANT USAGE ON TYPE :lv_schema.node_type TO threephi_db_user;

-- Default privileges for future objects created in this schema by postgres
-- (sqitch migrations run as postgres). New tables / sequences automatically
-- pick up the same grants without needing per-migration GRANT statements.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA :lv_schema
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO threephi_db_user;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA :lv_schema
    GRANT USAGE, SELECT ON SEQUENCES TO threephi_db_user;

COMMIT;

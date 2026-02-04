BEGIN;

-- Views first
DROP VIEW IF EXISTS :lv_schema.edge_current_with_totals;
DROP VIEW IF EXISTS :lv_schema.edge_with_totals;
DROP VIEW IF EXISTS :lv_schema.edge_current;
DROP VIEW IF EXISTS :lv_schema.node_current;

-- Index on version "current" flag
DROP INDEX IF EXISTS lv_topology_version_current_uniq;

-- Mapping and physical cables
DROP TABLE IF EXISTS :lv_schema.edge_cable CASCADE;
DROP TABLE IF EXISTS :lv_schema.cable CASCADE;

-- Topology
DROP INDEX IF EXISTS lv_edge_undirected_uniq;
DROP INDEX IF EXISTS lv_edge_node1_ver_idx;
DROP INDEX IF EXISTS lv_edge_node2_ver_idx;
DROP TABLE IF EXISTS :lv_schema.edge CASCADE;

DROP INDEX IF EXISTS lv_node_by_type_idx;
DROP INDEX IF EXISTS lv_node_feeder_fk_idx;
DROP INDEX IF EXISTS lv_node_cabinet_fk_idx;
DROP TABLE IF EXISTS :lv_schema.node CASCADE;

-- Version catalog
DROP TABLE IF EXISTS :lv_schema.topology_version CASCADE;

-- Core hierarchy
DROP INDEX IF EXISTS meter_delivery_point_idx;
DROP TABLE IF EXISTS :lv_schema.meter CASCADE;

DROP INDEX IF EXISTS delivery_point_cabinet_idx;
DROP TABLE IF EXISTS :lv_schema.delivery_point CASCADE;

DROP TABLE IF EXISTS :lv_schema.cabinet CASCADE;

DROP INDEX IF EXISTS lv_feeder_transformer_idx;
DROP TABLE IF EXISTS :lv_schema.feeder CASCADE;

DROP INDEX IF EXISTS transformer_substation_idx;
DROP TABLE IF EXISTS :lv_schema.transformer CASCADE;

DROP TABLE IF EXISTS :lv_schema.secondary_substation CASCADE;

-- Enum
DROP TYPE IF EXISTS :lv_schema.node_type;

DROP SCHEMA IF EXISTS lv CASCADE;

COMMIT;

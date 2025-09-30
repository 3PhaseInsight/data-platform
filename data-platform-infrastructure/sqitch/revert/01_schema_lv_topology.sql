BEGIN;

-- Views first
DROP VIEW IF EXISTS lv.lv_edge_current_with_totals;
DROP VIEW IF EXISTS lv.lv_edge_with_totals;
DROP VIEW IF EXISTS lv.lv_edge_current;
DROP VIEW IF EXISTS lv.lv_node_current;

-- Index on version "current" flag
DROP INDEX IF EXISTS lv_topology_version_current_uniq;

-- Mapping and physical cables
DROP TABLE IF EXISTS lv.lv_edge_cable CASCADE;
DROP TABLE IF EXISTS lv.cable CASCADE;

-- Topology
DROP INDEX IF EXISTS lv_edge_undirected_uniq;
DROP INDEX IF EXISTS lv_edge_node1_ver_idx;
DROP INDEX IF EXISTS lv_edge_node2_ver_idx;
DROP TABLE IF EXISTS lv.lv_edge CASCADE;

DROP INDEX IF EXISTS lv_node_by_type_idx;
DROP INDEX IF EXISTS lv_node_feeder_fk_idx;
DROP INDEX IF EXISTS lv_node_cabinet_fk_idx;
DROP TABLE IF EXISTS lv.lv_node CASCADE;

-- Version catalog
DROP TABLE IF EXISTS lv.topology_version CASCADE;

-- Core hierarchy
DROP INDEX IF EXISTS meter_delivery_point_idx;
DROP TABLE IF EXISTS lv.meter CASCADE;

DROP INDEX IF EXISTS delivery_point_cabinet_idx;
DROP TABLE IF EXISTS lv.delivery_point CASCADE;

DROP TABLE IF EXISTS lv.cabinet CASCADE;

DROP INDEX IF EXISTS lv_feeder_transformer_idx;
DROP TABLE IF EXISTS lv.lv_feeder CASCADE;

DROP INDEX IF EXISTS transformer_substation_idx;
DROP TABLE IF EXISTS lv.transformer CASCADE;

DROP TABLE IF EXISTS lv.secondary_substation CASCADE;

-- Enum
DROP TYPE IF EXISTS lv.node_type;

DROP SCHEMA IF EXISTS lv CASCADE;

COMMIT;
